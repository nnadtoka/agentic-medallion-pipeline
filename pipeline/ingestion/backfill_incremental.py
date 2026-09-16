"""Backfill the complete Olist history through logical incremental batches."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone

import pandas as pd

from pipeline.configs.olist_datasets import ALLOWED_DATASETS
from pipeline.configs.olist_flows import BACKFILL_TABLES, DAILY_INGESTION_TABLES
from pipeline.ingestion.ingest_olist import ingest_dataset
from pipeline.prefect_flows.common import source_path_for


def populated_dates(source_df: pd.DataFrame, table_name: str) -> list[date]:
    """Return sorted, valid business dates represented by an incremental source."""

    config = ALLOWED_DATASETS[table_name]
    if config["load_strategy"] != "incremental":
        raise ValueError(f"{table_name} is not configured as incremental")
    date_column = config["source_date_column"]
    if date_column not in source_df.columns:
        raise ValueError(f"Date column '{date_column}' was not found in source file")

    parsed = pd.to_datetime(source_df[date_column], errors="coerce")
    invalid_count = int(parsed.isna().sum())
    if invalid_count:
        raise ValueError(
            f"{table_name} contains {invalid_count} null or invalid {date_column} values"
        )
    return sorted(parsed.dt.date.unique().tolist())


def run_full_history_backfill(
    *,
    force: bool = False,
    run_transformations: bool = True,
) -> dict[str, object]:
    """Load snapshots once and every populated incremental source date."""

    snapshot_date = datetime.now(timezone.utc).date()
    loaded_batches: dict[str, int] = {}

    for table_name in BACKFILL_TABLES:
        source_path = source_path_for(table_name)
        source_df = pd.read_csv(source_path)
        ingest_dataset(
            input_file=str(source_path),
            table_name=table_name,
            batch_date=snapshot_date,
            force=force,
            source_df=source_df,
        )
        loaded_batches[table_name] = 1

    for table_name in DAILY_INGESTION_TABLES:
        source_path = source_path_for(table_name)
        source_df = pd.read_csv(source_path)
        dates = populated_dates(source_df, table_name)
        for batch_date in dates:
            ingest_dataset(
                input_file=str(source_path),
                table_name=table_name,
                batch_date=batch_date,
                force=force,
                source_df=source_df,
            )
        loaded_batches[table_name] = len(dates)

    transformation_result = None
    if run_transformations:
        # Import lazily so ingestion-only use does not require Prefect/dbt.
        from pipeline.prefect_flows.transformation_flow import transformation_flow

        transformation_result = transformation_flow(
            full_refresh=True,
            batch_prefix="olist_full_history_backfill",
        )

    return {
        "logical_batches": loaded_batches,
        "transformation": transformation_result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incrementally ingest every populated Olist history date."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Atomically replace dates/snapshots that were loaded previously.",
    )
    parser.add_argument(
        "--no-transformations",
        action="store_true",
        help="Stop after Bronze ingestion.",
    )
    args = parser.parse_args()
    result = run_full_history_backfill(
        force=args.force,
        run_transformations=not args.no_transformations,
    )
    print(result)


if __name__ == "__main__":
    main()
