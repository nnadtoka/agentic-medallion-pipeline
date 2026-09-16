"""Shared tasks and validation for Olist Prefect flows."""

import os
from datetime import date
from pathlib import Path
from typing import Sequence

from prefect import get_run_logger, task

from pipeline.configs.olist_datasets import ALLOWED_DATASETS
from pipeline.ingestion.ingest_olist import ingest_dataset


def validate_flow_tables(
    tables: Sequence[str],
    configured_tables: Sequence[str],
) -> tuple[str, ...]:
    """Return a validated, explicit table tuple for a flow run."""

    selected_tables = tuple(tables)
    if not selected_tables:
        raise ValueError("At least one table must be selected")
    if len(selected_tables) != len(set(selected_tables)):
        raise ValueError("Duplicate table names are not allowed")

    unsupported = set(selected_tables) - set(configured_tables)
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise ValueError(f"Tables are not configured for this flow: {names}")

    return selected_tables


def source_path_for(table_name: str) -> Path:
    """Resolve a configured source file below the Olist data directory."""

    data_directory = Path(os.getenv("OLIST_DATA_DIR", "data/olist"))
    return data_directory / ALLOWED_DATASETS[table_name]["source_file"]


@task(
    name="ingest-olist-table",
    task_run_name="ingest-{table_name}-{batch_date}",
    retries=0,
    log_prints=True,
)
def ingest_table_task(
    table_name: str,
    batch_date: date,
    force: bool,
) -> str:
    """Run one configured ingestion and expose it as a Prefect task."""

    source_path = source_path_for(table_name)
    get_run_logger().info("Ingesting table=%s source=%s", table_name, source_path)
    ingest_dataset(
        input_file=str(source_path),
        table_name=table_name,
        batch_date=batch_date,
        force=force,
    )
    return table_name
