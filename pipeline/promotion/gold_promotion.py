"""Call the gold-promotion function as the least-privilege `gold_promoter` role.

The actual privilege boundary lives in Postgres (postgres/init/040, 041):
`gold_promoter` cannot touch bronze/staging/gold tables directly, only
execute `gold.promote_marts_to_gold()`, which runs SECURITY DEFINER as its
owner. This module is a thin caller, not where the safety lives.
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Create a PostgreSQL connection as `gold_promoter` using .env values."""

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.getenv("GOLD_PROMOTER_USER", "gold_promoter"),
        password=os.environ["GOLD_PROMOTER_PASSWORD"],
    )


def promote_marts_to_gold(batch_id: str) -> list[dict[str, str]]:
    """Promote all candidate marts into gold; raises if any is missing."""

    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM gold.promote_marts_to_gold(%s);",
                    (batch_id,),
                )
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
    finally:
        connection.close()

    return [dict(zip(columns, row)) for row in rows]


def main() -> None:
    import argparse

    from pipeline.transformation_control.batches import finish_step, start_step

    parser = argparse.ArgumentParser(description="Promote validated marts to Gold")
    parser.add_argument("batch_id")
    args = parser.parse_args()

    start_step(args.batch_id, "gold_promotion", "promotion")
    try:
        results = promote_marts_to_gold(args.batch_id)
    except Exception as exc:
        finish_step(
            args.batch_id,
            "gold_promotion",
            "failed",
            error_message=str(exc),
            failure_details={"exception_type": type(exc).__name__},
        )
        raise

    for result in results:
        print(f"{result['promoted_table']}: {result['action']}")


if __name__ == "__main__":
    main()
