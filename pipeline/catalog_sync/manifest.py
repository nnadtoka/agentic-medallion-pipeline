"""Read dbt's compiled manifest for gold-layer table/column descriptions.

Marts models are identified by `node["schema"] == "marts_candidate"`
(`dbt_project.yml`: `marts: +schema: marts_candidate`) -- their own dedicated
candidate schema, materialized there by dbt, then promoted into `gold`
afterward, out of dbt's view, via `gold.promote_marts_to_gold()` (see
pipeline/promotion/README.md). `gold` is filled in here as the catalog's
`schema_name` because that's where the promoted table actually ends up
living, not because dbt's manifest says so -- dbt never materializes
directly into `gold`.

Earlier version of this module identified marts models via `fqn` instead,
because marts used to share the `staging` schema with unrelated
staging/intermediate views and dbt's own bookkeeping tables, so
`node["schema"]` alone couldn't distinguish "promotable candidate" from
"unrelated staging content". Giving marts their own schema
(`marts_candidate`) removed the need for that workaround.
"""

import json
from pathlib import Path

MARTS_CANDIDATE_SCHEMA = "marts_candidate"
GOLD_SCHEMA = "gold"


def load_gold_descriptions(manifest_path):
    """Return one dict per non-empty table/column description for a marts model.

    Each record has the shape stored in `catalog.dataset_embeddings`:
    `object_type`, `schema_name` (always "gold"), `table_name`,
    `column_name` (None for table-level records), and `description`.
    Models or columns without a `description:` in dbt YAML are skipped --
    there is nothing to embed.
    """

    manifest_file = Path(manifest_path)
    if not manifest_file.exists():
        raise FileNotFoundError(
            f"dbt manifest not found at {manifest_file}. "
            "Run `dbt compile` or `dbt run` first."
        )

    manifest = json.loads(manifest_file.read_text())

    records = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        if node.get("schema") != MARTS_CANDIDATE_SCHEMA:
            continue

        table_name = node["name"]
        table_description = (node.get("description") or "").strip()
        if table_description:
            records.append({
                "object_type": "table",
                "schema_name": GOLD_SCHEMA,
                "table_name": table_name,
                "column_name": None,
                "description": table_description,
            })

        for column_name, column in node.get("columns", {}).items():
            column_description = (column.get("description") or "").strip()
            if not column_description:
                continue
            records.append({
                "object_type": "column",
                "schema_name": GOLD_SCHEMA,
                "table_name": table_name,
                "column_name": column_name,
                "description": column_description,
            })

    return records
