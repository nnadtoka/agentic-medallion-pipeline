import argparse
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import execute_values

if __package__:
    from pipeline.configs.olist_datasets import ALLOWED_DATASETS
else:
    # Preserve direct-script execution from the repository root or an IDE.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pipeline.configs.olist_datasets import ALLOWED_DATASETS


# ============================================================
# Configuration
# ============================================================

load_dotenv()

PIPELINE_NAME = "olist_ingestion"
SOURCE = "olist"

# ============================================================
# Database
# ============================================================

def get_connection():
    """Create a PostgreSQL connection using values from .env."""

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


# ============================================================
# Ingestion control
# ============================================================

def get_successful_batch(
    cursor,
    dataset_name,
    batch_date,
):
    """
    Find a successful logical batch for this dataset/date.

    A logical batch is identified by:
        pipeline_name + dataset_name + batch_date
    """

    cursor.execute(
        """
        SELECT
            batch_id,
            status,
            run_type,
            rows_read,
            rows_loaded,
            created_ts,
            completed_ts
        FROM bronze.ingestion_batches
        WHERE pipeline_name = %s
          AND dataset_name = %s
          AND batch_date = %s
          AND status = 'success'
        ORDER BY created_ts DESC
        LIMIT 1
        """,
        (
            PIPELINE_NAME,
            dataset_name,
            batch_date,
        ),
    )

    return cursor.fetchone()


def create_batch(
    cursor,
    dataset_name,
    batch_id,
    batch_date,
    run_type,
    started_ts,
):
    """Create a new ingestion control record."""

    cursor.execute(
        """
        INSERT INTO bronze.ingestion_batches (
            pipeline_name,
            dataset_name,
            batch_id,
            batch_date,
            source,
            run_type,
            started_ts,
            status
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, 'running'
        )
        """,
        (
            PIPELINE_NAME,
            dataset_name,
            batch_id,
            batch_date,
            SOURCE,
            run_type,
            started_ts,
        ),
    )


def update_batch_success(
    cursor,
    batch_id,
    completed_ts,
    rows_read,
    rows_loaded,
):
    """Mark the current batch as successful."""

    cursor.execute(
        """
        UPDATE bronze.ingestion_batches
        SET
            completed_ts = %s,
            rows_read = %s,
            rows_loaded = %s,
            status = 'success',
            error_message = NULL
        WHERE pipeline_name = %s
          AND batch_id = %s
        """,
        (
            completed_ts,
            rows_read,
            rows_loaded,
            PIPELINE_NAME,
            batch_id,
        ),
    )


def update_batch_failed(
    cursor,
    batch_id,
    completed_ts,
    error_message,
):
    """Mark the current batch as failed."""

    cursor.execute(
        """
        UPDATE bronze.ingestion_batches
        SET
            completed_ts = %s,
            status = 'failed',
            error_message = %s
        WHERE pipeline_name = %s
          AND batch_id = %s
        """,
        (
            completed_ts,
            error_message[:4000],
            PIPELINE_NAME,
            batch_id,
        ),
    )


# ============================================================
# Bronze data loading
# ============================================================

def delete_existing_batch_rows(
    cursor,
    table_name,
    date_column,
    batch_start,
    batch_end,
):
    """
    Delete existing Bronze rows for the logical business date.

    This is intentionally executed in the same transaction as the
    subsequent INSERT so a failed backfill cannot leave the date
    with missing data.
    """

    delete_query = sql.SQL(
        """
        DELETE FROM bronze.{table}
        WHERE {date_column} >= %s
          AND {date_column} < %s
        """
    ).format(
        table=sql.Identifier(table_name),
        date_column=sql.Identifier(date_column),
    )

    cursor.execute(
        delete_query,
        (
            batch_start,
            batch_end,
        ),
    )

    return cursor.rowcount


def delete_all_rows(cursor, table_name):
    """Delete all rows from a whitelisted Bronze snapshot table."""

    delete_query = sql.SQL(
        "DELETE FROM bronze.{table}"
    ).format(
        table=sql.Identifier(table_name),
    )

    cursor.execute(delete_query)

    return cursor.rowcount


def insert_bronze_rows(
    cursor,
    connection,
    table_name,
    batch_df,
):
    """Insert a DataFrame into the whitelisted Bronze table."""

    source_columns = [
        column
        for column in batch_df.columns
        if column not in {
            "created_ts",
            "batch_id",
            "source",
        }
    ]

    columns = source_columns + [
        "created_ts",
        "batch_id",
        "source",
    ]

    def to_database_value(value):
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if hasattr(value, "item"):
            return value.item()
        return value

    records = [
        tuple(to_database_value(value) for value in row)
        for row in batch_df[columns].itertuples(index=False, name=None)
    ]

    if not records:
        return 0

    insert_query = sql.SQL(
        """
        INSERT INTO bronze.{table} ({columns})
        VALUES %s
        """
    ).format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(", ").join(
            sql.Identifier(column)
            for column in columns
        ),
    )

    execute_values(
        cursor,
        insert_query.as_string(connection),
        records,
        page_size=1000,
    )

    return len(records)


def resolve_batch_date(load_strategy, batch_date, table_name):
    """Validate or default the logical batch date."""

    if batch_date is None:
        if load_strategy == "incremental":
            raise ValueError(
                f"--batch-date is required for incremental table {table_name}"
            )
        return datetime.now(timezone.utc).date()

    if isinstance(batch_date, str):
        try:
            return datetime.strptime(batch_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                "--batch-date must use YYYY-MM-DD format"
            ) from exc

    if isinstance(batch_date, datetime):
        return batch_date.date()

    if isinstance(batch_date, date):
        return batch_date

    raise ValueError("batch_date must be a date or YYYY-MM-DD string")


def select_rows_for_load(df, dataset_config, batch_date):
    """Select incremental rows or return the complete source snapshot."""

    if dataset_config["load_strategy"] == "full_refresh":
        return df.copy()

    source_date_column = dataset_config["source_date_column"]
    partition_column = dataset_config["partition_column"]

    if source_date_column not in df.columns:
        raise ValueError(
            f"Date column '{source_date_column}' was not found in source file"
        )

    parsed_dates = pd.to_datetime(
        df[source_date_column],
        errors="coerce",
    )
    df = df.copy()
    df[source_date_column] = parsed_dates

    if partition_column != source_date_column:
        df[partition_column] = parsed_dates.dt.date

    batch_start = pd.Timestamp(batch_date)
    batch_end = batch_start + pd.Timedelta(days=1)

    return df[
        (parsed_dates >= batch_start)
        & (parsed_dates < batch_end)
    ].copy()


# ============================================================
# Main ingestion
# ============================================================

def ingest_dataset(
    input_file,
    table_name,
    batch_date=None,
    force=False,
    source_df=None,
):
    """Ingest an incremental batch or full Olist snapshot into Bronze."""

    # --------------------------------------------------------
    # Validate dataset/table arguments
    # --------------------------------------------------------

    if table_name not in ALLOWED_DATASETS:
        allowed = ", ".join(ALLOWED_DATASETS.keys())

        raise ValueError(
            f"Unsupported table '{table_name}'. "
            f"Allowed tables: {allowed}"
        )

    dataset_config = ALLOWED_DATASETS[table_name]
    dataset_name = dataset_config["dataset_name"]
    load_strategy = dataset_config["load_strategy"]
    partition_column = dataset_config.get("partition_column")

    # --------------------------------------------------------
    # Parse batch date
    # --------------------------------------------------------

    batch_date = resolve_batch_date(
        load_strategy,
        batch_date,
        table_name,
    )

    batch_start = pd.Timestamp(batch_date)
    batch_end = batch_start + pd.Timedelta(days=1)

    # --------------------------------------------------------
    # Read source
    # --------------------------------------------------------

    if not os.path.exists(input_file):
        raise FileNotFoundError(
            f"Input file not found: {input_file}"
        )

    print(f"Pipeline:    {PIPELINE_NAME}")
    print(f"Dataset:     {dataset_name}")
    print(f"Input file:  {input_file}")
    print(f"Target:      bronze.{table_name}")
    print(f"Strategy:    {load_strategy}")
    print(f"Batch date:  {batch_date}")
    print(f"Force:       {force}")

    # Range backfills may invoke this function hundreds of times. Allow the
    # orchestrator to reuse one parsed source frame while preserving exactly
    # the same per-date transaction and ingestion-control behavior.
    df = pd.read_csv(input_file) if source_df is None else source_df

    rows_read = len(df)

    print(f"Source rows: {rows_read}")

    # --------------------------------------------------------
    # Select rows according to the configured load strategy
    # --------------------------------------------------------

    batch_df = select_rows_for_load(
        df,
        dataset_config,
        batch_date,
    )

    rows_to_load = len(batch_df)

    print(f"Rows for batch: {rows_to_load}")

    # --------------------------------------------------------
    # Database connection
    # --------------------------------------------------------

    started_ts = datetime.now(timezone.utc)
    run_type = "backfill" if force else load_strategy

    # Physical execution ID.
    batch_id = (
        f"{PIPELINE_NAME}_"
        f"{dataset_name}_"
        f"{started_ts.strftime('%Y%m%d%H%M%S%f')}"
    )

    print(f"Batch ID:    {batch_id}")
    print(f"Started TS:  {started_ts.isoformat()}")
    print(f"Run type:    {run_type}")

    connection = get_connection()

    try:
        # ----------------------------------------------------
        # Check existing logical batch.
        #
        # This check is done before starting the load
        # transaction because a normal successful batch
        # should simply be skipped.
        # ----------------------------------------------------

        with connection.cursor() as cursor:

            successful_batch = get_successful_batch(
                cursor,
                dataset_name,
                batch_date,
            )

        if successful_batch and not force:

            print(
                f"Batch for {dataset_name} / {batch_date} "
                f"already succeeded."
            )
            print("Use --force to explicitly backfill this date.")
            print("Status:      SKIPPED")

            connection.close()
            return

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Everything below happens in ONE transaction:
        #
        #   1. Remove previous logical batch rows if needed
        #   2. Register the new batch
        #   3. Insert replacement data
        #   4. Mark batch successful
        #
        # If anything fails, PostgreSQL rolls everything back.
        # ----------------------------------------------------

        with connection:

            with connection.cursor() as cursor:

                # ------------------------------------------------
                # Backfill:
                # remove the previous business-date records.
                #
                # This happens BEFORE the new INSERT but inside
                # the same transaction.
                # ------------------------------------------------

                if load_strategy == "incremental":
                    deleted_rows = 0
                    if force:
                        deleted_rows = delete_existing_batch_rows(
                            cursor,
                            table_name,
                            partition_column,
                            batch_start,
                            batch_end,
                        )
                else:
                    deleted_rows = delete_all_rows(
                        cursor,
                        table_name,
                    )

                if force or load_strategy == "full_refresh":
                    delete_scope = (
                        "existing Bronze rows"
                        if load_strategy == "incremental"
                        else "snapshot rows"
                    )
                    print(
                        f"Deleted:     {deleted_rows} {delete_scope}"
                    )

                # ------------------------------------------------
                # Create control record for this physical run.
                # ------------------------------------------------

                create_batch(
                    cursor,
                    dataset_name,
                    batch_id,
                    batch_date,
                    run_type,
                    started_ts,
                )

                # ------------------------------------------------
                # Add Bronze metadata.
                # ------------------------------------------------

                batch_df["created_ts"] = started_ts
                batch_df["batch_id"] = batch_id
                batch_df["source"] = SOURCE

                # ------------------------------------------------
                # Insert data.
                # ------------------------------------------------

                rows_loaded = insert_bronze_rows(
                    cursor,
                    connection,
                    table_name,
                    batch_df,
                )

                # ------------------------------------------------
                # Mark successful.
                # ------------------------------------------------

                completed_ts = datetime.now(timezone.utc)

                update_batch_success(
                    cursor,
                    batch_id,
                    completed_ts,
                    rows_read,
                    rows_loaded,
                )

        # ----------------------------------------------------
        # Transaction committed successfully.
        # ----------------------------------------------------

        print(f"Rows loaded: {rows_loaded}")
        print("Status:      SUCCESS")

    except Exception as exc:

        # ----------------------------------------------------
        # If the transaction failed, rollback guarantees that
        # a backfill cannot leave the Bronze table partially
        # deleted/loaded.
        # ----------------------------------------------------

        connection.rollback()

        print("Status: FAILED")
        print(f"Error:  {exc}")

        # ----------------------------------------------------
        # Record failure in a separate transaction.
        #
        # The original transaction may have rolled back the
        # ingestion_batches INSERT, so create the failure record
        # independently.
        # ----------------------------------------------------

        try:

            with connection:
                with connection.cursor() as cursor:

                    create_batch(
                        cursor,
                        dataset_name,
                        batch_id,
                        batch_date,
                        run_type,
                        started_ts,
                    )

                    update_batch_failed(
                        cursor,
                        batch_id,
                        datetime.now(timezone.utc),
                        str(exc),
                    )

        except Exception as tracking_exc:

            print(
                "WARNING: Could not record failed batch: "
                f"{tracking_exc}"
            )

        raise

    finally:
        connection.close()


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Ingest an Olist batch or full snapshot into Bronze."
        )
    )

    parser.add_argument(
        "--input-file",
        required=True,
        help="Path to the source CSV file.",
    )

    parser.add_argument(
        "--table",
        required=True,
        help="Target Bronze table, e.g. raw_orders.",
    )

    parser.add_argument(
        "--batch-date",
        help=(
            "Date to ingest in YYYY-MM-DD format. Required for incremental "
            "tables; defaults to today in UTC for full-refresh tables."
        ),
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Backfill an already processed date. "
            "Existing Bronze rows for the date are deleted "
            "and replaced atomically."
        ),
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    try:
        ingest_dataset(
            input_file=args.input_file,
            table_name=args.table,
            batch_date=args.batch_date,
            force=args.force,
        )
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
