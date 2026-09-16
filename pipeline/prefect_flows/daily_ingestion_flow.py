"""Prefect flow for date-partitioned Olist sources."""

from datetime import date
from typing import Sequence

from prefect import flow, get_run_logger

from pipeline.configs.olist_flows import DAILY_INGESTION_TABLES
from pipeline.prefect_flows.common import ingest_table_task, validate_flow_tables
from pipeline.prefect_flows.transformation_flow import transformation_flow


@flow(
    name="olist-daily-ingestion",
    flow_run_name="olist-daily-{batch_date}",
    log_prints=True,
    retries=0,
)
def daily_ingestion_flow(
    batch_date: date,
    tables: Sequence[str] = DAILY_INGESTION_TABLES,
    force: bool = False,
    run_transformations: bool = True,
) -> list[str]:
    """Ingest an explicit date for configured incremental Olist tables."""

    selected_tables = validate_flow_tables(tables, DAILY_INGESTION_TABLES)
    get_run_logger().info(
        "Daily ingestion date=%s tables=%s force=%s",
        batch_date,
        ", ".join(selected_tables),
        force,
    )
    ingested_tables = [
        ingest_table_task(table_name, batch_date, force)
        for table_name in selected_tables
    ]
    if run_transformations:
        transformation_flow(
            full_refresh=False,
            batch_prefix="olist_daily_ingestion",
        )
    return ingested_tables
