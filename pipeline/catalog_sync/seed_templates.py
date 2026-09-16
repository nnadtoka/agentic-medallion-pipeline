"""Seed hand-authored query template YAML files into `catalog.query_templates`
and embed each one's `description` into `catalog.dataset_embeddings`.

Unlike `pipeline/catalog_sync/sync.py`, templates are not generated from
dbt's manifest -- they're hand-authored data files under `mcp_agent/templates/`
(see that folder's README for the file schema). This script is the "source
of truth -> Postgres" sync for that data, run manually whenever a template
file is added or changed, not on every gold promotion.

Runs as `catalog_writer`, same role and connection helper as `sync.py` --
`catalog_writer` already has read/write on all of `catalog.*` and nothing
else, which covers `query_templates` too.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pgvector import Vector
from pgvector.psycopg2 import register_vector
from psycopg2.extras import Json

from pipeline.catalog_sync.embedding import embed_texts
from pipeline.catalog_sync.sync import get_connection

DEFAULT_TEMPLATES_DIR = "mcp_agent/templates"

UPSERT_TEMPLATE_SQL = """
    INSERT INTO catalog.query_templates
        (template_id, name, description, sql_template, parameters, target_schema, max_limit, updated_at)
    VALUES (%(template_id)s, %(name)s, %(description)s, %(sql_template)s, %(parameters)s,
            %(target_schema)s, %(max_limit)s, now())
    ON CONFLICT (template_id) DO UPDATE SET
        name = EXCLUDED.name,
        description = EXCLUDED.description,
        sql_template = EXCLUDED.sql_template,
        parameters = EXCLUDED.parameters,
        target_schema = EXCLUDED.target_schema,
        max_limit = EXCLUDED.max_limit,
        updated_at = now()
"""

# `WHERE object_type = 'query_template'` matches 044_dataset_embeddings_
# template_key.sql's partial unique index -- Postgres requires the ON
# CONFLICT predicate to line up exactly with the index it's targeting.
UPSERT_EMBEDDING_SQL = """
    INSERT INTO catalog.dataset_embeddings
        (object_type, template_id, description, embedding, source, updated_at)
    VALUES ('query_template', %s, %s, %s, 'manual', %s)
    ON CONFLICT (template_id) WHERE object_type = 'query_template'
    DO UPDATE SET
        description = EXCLUDED.description,
        embedding = EXCLUDED.embedding,
        updated_at = EXCLUDED.updated_at
"""


def _load_templates(templates_dir) -> list[dict]:
    templates_path = Path(templates_dir)
    if not templates_path.is_dir():
        raise FileNotFoundError(f"Templates directory not found: {templates_path}")

    templates = []
    for file_path in sorted(templates_path.glob("*.yaml")):
        data = yaml.safe_load(file_path.read_text())
        missing = {"template_id", "name", "description", "sql_template", "parameters"} - data.keys()
        if missing:
            raise ValueError(f"{file_path} is missing required keys: {sorted(missing)}")
        templates.append(data)
    return templates


def seed_templates(templates_dir=None, embed_fn=None, logger=None) -> dict:
    """Upsert every template YAML file's row and re-embed its description.

    Always re-embeds every file found, regardless of whether it changed --
    unlike `sync.py`'s hash-based skip, this runs rarely (by hand, when a
    template is authored or edited), not after every gold promotion, so the
    extra embedding cost doesn't need optimizing away.
    """

    templates_dir = templates_dir or os.getenv("QUERY_TEMPLATES_DIR", DEFAULT_TEMPLATES_DIR)
    embed_fn = embed_fn or embed_texts

    templates = _load_templates(templates_dir)
    if not templates:
        message = f"No template YAML files found under {templates_dir!r}."
        if logger:
            logger.warning(message)
        else:
            print(message)
        return {"templates": 0}

    descriptions = [template["description"] for template in templates]
    embeddings = embed_fn(descriptions)
    if len(embeddings) != len(templates):
        raise ValueError(
            f"embed_fn returned {len(embeddings)} vectors for {len(templates)} templates"
        )

    connection = get_connection()
    register_vector(connection)
    try:
        with connection:
            with connection.cursor() as cursor:
                synced_at = datetime.now(timezone.utc)
                for template, embedding in zip(templates, embeddings):
                    cursor.execute(
                        UPSERT_TEMPLATE_SQL,
                        {
                            "template_id": template["template_id"],
                            "name": template["name"],
                            "description": template["description"],
                            "sql_template": template["sql_template"],
                            "parameters": Json(template["parameters"]),
                            "target_schema": template.get("target_schema", "gold"),
                            "max_limit": template.get("max_limit", 100),
                        },
                    )
                    cursor.execute(
                        UPSERT_EMBEDDING_SQL,
                        (
                            template["template_id"],
                            template["description"],
                            Vector(embedding),
                            synced_at,
                        ),
                    )
    finally:
        connection.close()

    return {"templates": len(templates), "template_ids": [t["template_id"] for t in templates]}


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Seed query template YAML files into the pgvector catalog."
    )
    parser.add_argument(
        "--templates-dir",
        help=f"Defaults to $QUERY_TEMPLATES_DIR, or {DEFAULT_TEMPLATES_DIR!r}.",
    )
    args = parser.parse_args()

    result = seed_templates(templates_dir=args.templates_dir)
    print(f"Templates seeded: {result['templates']}")
    for template_id in result.get("template_ids", []):
        print(f"  - {template_id}")


if __name__ == "__main__":
    main()
