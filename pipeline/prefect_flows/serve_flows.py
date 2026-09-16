"""Serve Olist flow deployments from the pipeline-engine container."""

from prefect import serve

from pipeline.prefect_flows.backfill_flow import backfill_flow
from pipeline.prefect_flows.catalog_sync_flow import catalog_sync_flow
from pipeline.prefect_flows.daily_ingestion_flow import daily_ingestion_flow
from pipeline.prefect_flows.transformation_flow import transformation_flow


def main() -> None:
    """Register all deployments and listen for flow runs."""

    daily_deployment = daily_ingestion_flow.to_deployment(
        name="daily-ingestion",
        tags=["olist", "ingestion", "incremental"],
        concurrency_limit=1,
    )
    backfill_deployment = backfill_flow.to_deployment(
        name="backfill",
        tags=["olist", "ingestion", "full-refresh"],
        concurrency_limit=1,
    )
    catalog_sync_deployment = catalog_sync_flow.to_deployment(
        name="catalog-sync",
        tags=["catalog", "semantic-layer", "mcp"],
        concurrency_limit=1,
    )
    transformation_deployment = transformation_flow.to_deployment(
        name="transform-quality",
        tags=["olist", "dbt", "quality"],
        concurrency_limit=1,
    )
    serve(
        daily_deployment,
        backfill_deployment,
        transformation_deployment,
        catalog_sync_deployment,
    )


if __name__ == "__main__":
    main()
