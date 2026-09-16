"""Persist transformation lifecycle and model/source batch lineage."""

from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extras import Json

from pipeline.configs.model_dependencies import (
    MODEL_SOURCES,
    SOURCE_PIPELINE_NAME,
    TRANSFORMATION_PIPELINE_NAME,
)


REQUIRED_QUALITY_STEPS = (
    "dbt_staging_intermediate",
    "gx_intermediate",
    "dbt_marts",
    "gx_marts",
)


def generate_batch_id(
    pipeline_name: str = TRANSFORMATION_PIPELINE_NAME,
    unix_seconds: int | None = None,
) -> str:
    """Return ``<pipeline_name>_<POSIX seconds>`` for a transformation run."""

    timestamp = int(time.time()) if unix_seconds is None else unix_seconds
    return f"{pipeline_name}_{timestamp}"


def get_connection():
    """Connect as the least-privilege dbt transformation role."""

    return psycopg2.connect(
        host=os.getenv("DBT_HOST", "127.0.0.1"),
        port=os.getenv("DBT_PORT", "5433"),
        dbname=os.getenv("POSTGRES_DB", "analytics_db"),
        user=os.getenv("DBT_USER", "dbt_transformer"),
        password=os.environ["DBT_PASSWORD"],
    )


def create_batch(
    batch_id: str,
    *,
    pipeline_name: str = TRANSFORMATION_PIPELINE_NAME,
) -> None:
    """Create the one active transformation batch."""

    started_ts = datetime.now(timezone.utc)
    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.transformation_batches (
                        batch_id,
                        pipeline_name,
                        status,
                        started_ts
                    )
                    VALUES (%s, %s, 'running', %s)
                    """,
                    (batch_id, pipeline_name, started_ts),
                )
    finally:
        connection.close()


def prepare_model_batches(
    batch_id: str,
    *,
    source_pipeline_name: str = SOURCE_PIPELINE_NAME,
    model_sources: Mapping[str, Sequence[str]] = MODEL_SOURCES,
) -> dict[str, int]:
    """Snapshot successful source batches not yet promoted by each model.

    The snapshot is isolated from dbt invocation details. Models discover it
    by joining lineage to the single running transformation batch.
    """

    selected_by_model = {model_name: 0 for model_name in model_sources}
    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 1
                    FROM staging.transformation_batches
                    WHERE batch_id = %s
                      AND status = 'running'
                    FOR UPDATE
                    """,
                    (batch_id,),
                )
                if cursor.fetchone() is None:
                    raise RuntimeError(
                        f"Transformation batch is not running: {batch_id}"
                    )

                for model_name, source_datasets in model_sources.items():
                    for source_dataset_name in source_datasets:
                        cursor.execute(
                            """
                            INSERT INTO staging.model_batch_control (
                                model_name,
                                source_dataset_name,
                                source_batch_id,
                                target_batch_id
                            )
                            SELECT
                                %s,
                                %s,
                                source_batch.batch_id,
                                %s
                            FROM bronze.ingestion_batches AS source_batch
                            WHERE source_batch.pipeline_name = %s
                              AND source_batch.dataset_name = %s
                              AND source_batch.status = 'success'
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM staging.model_batch_control AS prior_lineage
                                  INNER JOIN staging.transformation_batches AS prior_target
                                      ON prior_lineage.target_batch_id = prior_target.batch_id
                                  WHERE prior_lineage.model_name = %s
                                    AND prior_lineage.source_dataset_name = %s
                                    AND prior_lineage.source_batch_id = source_batch.batch_id
                                    AND prior_target.status = 'promoted'
                              )
                            ON CONFLICT DO NOTHING
                            """,
                            (
                                model_name,
                                source_dataset_name,
                                batch_id,
                                source_pipeline_name,
                                source_dataset_name,
                                model_name,
                                source_dataset_name,
                            ),
                        )
                        selected_by_model[model_name] += cursor.rowcount
    finally:
        connection.close()

    return selected_by_model


def start_step(batch_id: str, step_name: str, step_type: str) -> None:
    """Create a running diagnostic record for one transformation gate."""

    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO staging.transformation_step_runs (
                        batch_id,
                        step_name,
                        step_type,
                        status,
                        started_ts
                    )
                    VALUES (%s, %s, %s, 'running', %s)
                    """,
                    (
                        batch_id,
                        step_name,
                        step_type,
                        datetime.now(timezone.utc),
                    ),
                )
    finally:
        connection.close()


def finish_step(
    batch_id: str,
    step_name: str,
    status: str,
    *,
    error_message: str | None = None,
    failure_details: dict[str, Any] | None = None,
) -> None:
    """Complete a step with success or structured failure information."""

    if status not in {"success", "failed"}:
        raise ValueError(f"Unsupported terminal step status: {status}")

    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE staging.transformation_step_runs
                    SET status = %s,
                        completed_ts = %s,
                        error_message = %s,
                        failure_details = %s
                    WHERE batch_id = %s
                      AND step_name = %s
                      AND status = 'running'
                    """,
                    (
                        status,
                        datetime.now(timezone.utc),
                        error_message,
                        Json(failure_details) if failure_details is not None else None,
                        batch_id,
                        step_name,
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Transformation step is missing or already complete: "
                        f"{batch_id}/{step_name}"
                    )
    finally:
        connection.close()


def update_batch_status(
    batch_id: str,
    status: str,
    *,
    failed_step_name: str | None = None,
    error_message: str | None = None,
) -> None:
    """Update operational fields without changing append-only lineage rows."""

    allowed_statuses = {"candidate_ready", "quality_passed", "validated", "failed"}
    if status not in allowed_statuses:
        raise ValueError(f"Unsupported application-managed status: {status}")

    expected_current_statuses = {
        "candidate_ready": ["running"],
        "quality_passed": ["candidate_ready"],
        "validated": ["quality_passed"],
        "failed": ["running", "candidate_ready", "quality_passed"],
    }

    completed_ts = (
        datetime.now(timezone.utc) if status in {"validated", "failed"} else None
    )
    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                if status == "quality_passed":
                    cursor.execute(
                        """
                        SELECT step_name, status
                        FROM staging.transformation_step_runs
                        WHERE batch_id = %s
                          AND step_name = ANY(%s)
                        """,
                        (batch_id, list(REQUIRED_QUALITY_STEPS)),
                    )
                    step_statuses = dict(cursor.fetchall())
                    unsuccessful = [
                        step_name
                        for step_name in REQUIRED_QUALITY_STEPS
                        if step_statuses.get(step_name) != "success"
                    ]
                    if unsuccessful:
                        raise RuntimeError(
                            "Target batch cannot pass quality; incomplete or failed "
                            "steps: " + ", ".join(unsuccessful)
                        )

                cursor.execute(
                    """
                    UPDATE staging.transformation_batches
                    SET status = %s,
                        completed_ts = %s,
                        failed_step_name = %s,
                        error_message = %s
                    WHERE batch_id = %s
                      AND status = ANY(%s)
                    """,
                    (
                        status,
                        completed_ts,
                        failed_step_name,
                        error_message,
                        batch_id,
                        expected_current_statuses[status],
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        "Target batch is missing or not in an allowed prior state: "
                        f"{batch_id} -> {status}"
                    )
    finally:
        connection.close()
