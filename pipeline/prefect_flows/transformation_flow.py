"""Prefect flow: staged dbt builds, quality gates, and gold promotion.

staging+intermediate -> gate -> marts -> gate -> gold. Each stage only runs
if the previous one succeeded, so a failing dbt test or Great Expectations
check stops the flow before marts get built on bad upstream data, and
before gold is ever touched. See pipeline/promotion/README.md for why the
gate is split this way.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from pipeline.catalog_sync.sync import sync_catalog
from pipeline.promotion.gold_promotion import promote_marts_to_gold
from pipeline.quality.great_expectations_checks import (
    QualityGateError,
    validate_candidate_intermediate,
    validate_candidate_marts,
)
from pipeline.transformation_control.batches import (
    create_batch,
    finish_step,
    generate_batch_id,
    prepare_model_batches,
    start_step,
    update_batch_status,
)


DBT_UPSTREAM_STEP = "dbt_staging_intermediate"
GX_UPSTREAM_STEP = "gx_intermediate"
DBT_MARTS_STEP = "dbt_marts"
GX_MARTS_STEP = "gx_marts"
PROMOTION_STEP = "gold_promotion"


def dbt_build_command(
    select: str | None = None,
    full_refresh: bool = False,
) -> list[str]:
    """Build the dbt command without invoking a shell.

    `select` is a plain space-separated dbt selector string (e.g.
    "staging intermediate"), split into separate --select arguments so
    subprocess doesn't need a shell to word-split it.
    """

    project_dir = Path(os.getenv("DBT_PROJECT_DIR", "/app/dbt"))
    profiles_dir = Path(os.getenv("DBT_PROFILES_DIR", str(project_dir)))
    command = [
        "dbt",
        "build",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    if select:
        command += ["--select", *select.split()]
    if full_refresh:
        command.append("--full-refresh")
    return command


def dbt_failure_details(project_dir: Path, return_code: int) -> dict[str, Any]:
    """Extract concise failed-resource details from dbt's run-results artifact."""

    run_results_path = project_dir / "target" / "run_results.json"
    details: dict[str, Any] = {
        "return_code": return_code,
        "run_results_path": str(run_results_path),
        "failed_resources": [],
    }
    try:
        run_results = json.loads(run_results_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        details["artifact_error"] = str(exc)
        return details

    metadata = run_results.get("metadata", {})
    details["dbt_invocation_id"] = metadata.get("invocation_id")
    successful_statuses = {"pass", "success", "warn"}
    details["failed_resources"] = [
        {
            "unique_id": result.get("unique_id"),
            "status": result.get("status"),
            "message": result.get("message"),
            "failures": result.get("failures"),
            "execution_time": result.get("execution_time"),
        }
        for result in run_results.get("results", [])
        if result.get("status") not in successful_statuses
    ]
    return details


@task(
    name="dbt-build",
    task_run_name="dbt-build-{select}",
    retries=0,
)
def dbt_build_task(
    select: str | None = None,
    full_refresh: bool = False,
    batch_id: str | None = None,
    step_name: str = "dbt_build",
) -> None:
    """Build the selected models and fail the task if any dbt test fails."""

    logger = get_run_logger()
    command = dbt_build_command(select, full_refresh)
    if batch_id:
        start_step(batch_id, step_name, "dbt")
    logger.info(
        "Running dbt build select=%s full_refresh=%s batch_id=%s",
        select,
        full_refresh,
        batch_id,
    )
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception as exc:
        if batch_id:
            finish_step(
                batch_id,
                step_name,
                "failed",
                error_message=str(exc),
                failure_details={"exception_type": type(exc).__name__},
            )
        raise
    if result.stdout:
        logger.info("dbt output:\n%s", result.stdout.rstrip())
    if result.stderr:
        logger.warning("dbt stderr:\n%s", result.stderr.rstrip())
    if result.returncode:
        details = dbt_failure_details(
            Path(os.getenv("DBT_PROJECT_DIR", "/app/dbt")),
            result.returncode,
        )
        if batch_id:
            finish_step(
                batch_id,
                step_name,
                "failed",
                error_message=f"dbt build failed with exit code {result.returncode}",
                failure_details=details,
            )
        raise RuntimeError(f"dbt build failed with exit code {result.returncode}")
    if batch_id:
        finish_step(batch_id, step_name, "success")


@task(name="great-expectations-validate-intermediate", retries=0)
def great_expectations_intermediate_task(batch_id: str) -> dict[str, str]:
    """Run Great Expectations checks against candidate intermediate models."""

    logger = get_run_logger()
    start_step(batch_id, GX_UPSTREAM_STEP, "great_expectations")
    try:
        summaries = validate_candidate_intermediate()
    except QualityGateError as exc:
        finish_step(
            batch_id,
            GX_UPSTREAM_STEP,
            "failed",
            error_message=str(exc),
            failure_details={"failed_expectations": exc.failures},
        )
        raise
    except Exception as exc:
        finish_step(
            batch_id,
            GX_UPSTREAM_STEP,
            "failed",
            error_message=str(exc),
            failure_details={"exception_type": type(exc).__name__},
        )
        raise
    finish_step(batch_id, GX_UPSTREAM_STEP, "success")
    results = {
        summary.table_name: f"{summary.checks_passed}/{summary.checks_run}"
        for summary in summaries
    }
    for table_name, result in results.items():
        logger.info(
            "Great Expectations (intermediate) table=%s passed=%s", table_name, result
        )
    return results


@task(name="great-expectations-validate-marts", retries=0)
def great_expectations_marts_task(batch_id: str) -> dict[str, str]:
    """Run Great Expectations checks against candidate marts."""

    logger = get_run_logger()
    start_step(batch_id, GX_MARTS_STEP, "great_expectations")
    try:
        summaries = validate_candidate_marts()
    except QualityGateError as exc:
        finish_step(
            batch_id,
            GX_MARTS_STEP,
            "failed",
            error_message=str(exc),
            failure_details={"failed_expectations": exc.failures},
        )
        raise
    except Exception as exc:
        finish_step(
            batch_id,
            GX_MARTS_STEP,
            "failed",
            error_message=str(exc),
            failure_details={"exception_type": type(exc).__name__},
        )
        raise
    finish_step(batch_id, GX_MARTS_STEP, "success")
    results = {
        summary.table_name: f"{summary.checks_passed}/{summary.checks_run}"
        for summary in summaries
    }
    for table_name, result in results.items():
        logger.info("Great Expectations (marts) table=%s passed=%s", table_name, result)
    return results


@task(name="promote-marts-to-gold", retries=0)
def promote_marts_to_gold_task(batch_id: str) -> list[dict[str, str]]:
    """Blue-green promote validated candidate marts into gold."""

    logger = get_run_logger()
    start_step(batch_id, PROMOTION_STEP, "promotion")
    try:
        results = promote_marts_to_gold(batch_id)
    except Exception as exc:
        finish_step(
            batch_id,
            PROMOTION_STEP,
            "failed",
            error_message=str(exc),
            failure_details={"exception_type": type(exc).__name__},
        )
        raise
    for result in results:
        logger.info("Promoted %s: %s", result["promoted_table"], result["action"])
    return results


@task(name="catalog-sync", retries=0)
def catalog_sync_task(batch_id: str) -> dict[str, object] | None:
    """Best-effort: sync gold descriptions into the pgvector catalog after promotion.

    Deliberately non-blocking -- a catalog-sync problem is a stale search
    index for agents, not a data-correctness problem, and shouldn't fail
    the promotion that just succeeded. Not tracked in
    staging.transformation_step_runs (that table's step_type CHECK
    constraint is scoped to the other agent's own dbt/GX/promotion steps,
    and widening someone else's constraint for an unrelated concern isn't
    the right fix here) -- sync_catalog already has its own dedicated audit
    trail, catalog.catalog_sync_runs, so this would just be duplicate
    tracking of the same fact in two places. There is no alerting on
    either audit trail in this toy setup, which is a known, accepted gap
    for now, not an oversight.
    """

    logger = get_run_logger()
    try:
        result = sync_catalog(logger=logger)
    except Exception as exc:
        logger.warning(
            "Catalog sync failed (non-blocking, promotion still stands) "
            "batch_id=%s: %s",
            batch_id,
            exc,
        )
        return None

    logger.info("Catalog sync (batch_id=%s): %s", batch_id, result)
    return result


@flow(
    name="olist-transform-quality",
    flow_run_name="olist-transform-quality",
    log_prints=True,
    retries=0,
)
def transformation_flow(
    full_refresh: bool = False,
    promote: bool = True,
    batch_prefix: str = "olist_transform_quality",
) -> dict[str, object]:
    """Run the full staged gate, promoting to gold only if everything passes."""

    logger = get_run_logger()
    batch_id = generate_batch_id(batch_prefix)
    batch_created = False
    active_step = "create_batch"

    try:
        create_batch(batch_id)
        batch_created = True
        logger.info(
            "Started transformation batch_id=%s",
            batch_id,
        )

        active_step = DBT_UPSTREAM_STEP
        dbt_build_task(
            select="staging intermediate",
            full_refresh=full_refresh,
            batch_id=batch_id,
            step_name=DBT_UPSTREAM_STEP,
        )
        active_step = GX_UPSTREAM_STEP
        intermediate_results = great_expectations_intermediate_task(batch_id)

        selected_batches = prepare_model_batches(batch_id)
        logger.info("Selected source batches by model: %s", selected_batches)

        active_step = DBT_MARTS_STEP
        dbt_build_task(
            select="marts",
            full_refresh=full_refresh,
            batch_id=batch_id,
            step_name=DBT_MARTS_STEP,
        )
        update_batch_status(batch_id, "candidate_ready")
        active_step = GX_MARTS_STEP
        marts_results = great_expectations_marts_task(batch_id)
        update_batch_status(batch_id, "quality_passed")

        active_step = PROMOTION_STEP
        catalog_sync_results = None
        if promote:
            promotion_results = promote_marts_to_gold_task(batch_id)
            # Not gated by active_step / the outer except below -- see
            # catalog_sync_task's own docstring for why a sync failure here
            # must not turn into a failed transformation batch.
            catalog_sync_results = catalog_sync_task(batch_id)
        else:
            update_batch_status(batch_id, "validated")
            promotion_results = None

        return {
            "batch_id": batch_id,
            "intermediate": intermediate_results,
            "marts": marts_results,
            "promotion": promotion_results,
            "catalog_sync": catalog_sync_results,
        }
    except Exception as exc:
        if batch_created:
            try:
                update_batch_status(
                    batch_id,
                    "failed",
                    failed_step_name=active_step,
                    error_message=str(exc),
                )
            except Exception:
                logger.exception(
                    "Could not mark failed transformation batch_id=%s",
                    batch_id,
                )
        raise
