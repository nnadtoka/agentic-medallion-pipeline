"""Explicit operator entry point for atomic rollback of an active Gold release."""

from __future__ import annotations

import argparse

from pipeline.promotion.gold_promotion import get_connection


def rollback_release(release_id: str) -> list[dict[str, str]]:
    """Restore all ordinary tables, snapshots, and the Gold serving view."""

    connection = get_connection()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM gold.rollback_release(%s);", (release_id,))
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
    finally:
        connection.close()

    return [dict(zip(columns, row)) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Atomically roll back the currently active Gold release"
    )
    parser.add_argument("release_id")
    args = parser.parse_args()

    for result in rollback_release(args.release_id):
        print(
            f"{result['object_type']} {result['object_name']}: "
            f"{result['action']}"
        )


if __name__ == "__main__":
    main()
