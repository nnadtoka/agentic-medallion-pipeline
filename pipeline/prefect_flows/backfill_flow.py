"""Manual Prefect flow for non-partitioned Olist sources."""

from datetime import date
from typing import Sequence

from prefect import flow, get_run_logger

from pipeline.configs.olist_flows import BACKFILL_TABLES
from pipeline.prefect_flows.common import ingest_table_task, validate_flow_tables
from pipeline.prefect_flows.transformation_flow import transformation_flow


@flow(
    name="olist-backfill",
    flow_run_name="olist-backfill-{snapshot_date}",
    log_prints=True,
    retries=0,
)
def backfill_flow(
    snapshot_date: date,
    tables: Sequence[str] = BACKFILL_TABLES,
    force: bool = False,
    run_transformations: bool = True,
) -> list[str]:
    """Load configured full-refresh Olist snapshots without a schedule."""

    selected_tables = validate_flow_tables(tables, BACKFILL_TABLES)
    get_run_logger().info(
        "Backfill snapshot=%s tables=%s force=%s",
        snapshot_date,
        ", ".join(selected_tables),
        force,
    )
    ingested_tables = [
        ingest_table_task(table_name, snapshot_date, force)
        for table_name in selected_tables
    ]
    if run_transformations:
        transformation_flow(
            full_refresh=True,
            batch_prefix="olist_backfill",
        )
    return ingested_tables
