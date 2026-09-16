"""Prefect flow for syncing dbt gold-layer descriptions into the pgvector catalog."""

from prefect import flow, get_run_logger, task

from pipeline.catalog_sync.sync import sync_catalog


@task(
    name="sync-gold-catalog",
    retries=0,
    log_prints=True,
)
def sync_catalog_task(manifest_path: str | None = None) -> dict:
    """Run the catalog sync and expose it as a Prefect task."""

    logger = get_run_logger()
    result = sync_catalog(manifest_path=manifest_path, logger=logger)
    logger.info(
        "Catalog sync: tables=%s columns=%s unchanged=%s",
        result["tables"],
        result["columns"],
        result["unchanged"],
    )
    return result


@flow(
    name="catalog-sync",
    flow_run_name="catalog-sync",
    log_prints=True,
    retries=0,
)
def catalog_sync_flow(manifest_path: str | None = None) -> dict:
    """Sync dbt-authored gold descriptions into the semantic-layer catalog.

    `transformation_flow.py` already triggers a sync after every gold
    promotion (see its own `catalog_sync_task`), so this deployment exists
    for standalone/manual re-syncs -- e.g. re-embedding after a
    `dataset_embeddings` schema change -- not as the primary trigger path.
    """

    return sync_catalog_task(manifest_path)
