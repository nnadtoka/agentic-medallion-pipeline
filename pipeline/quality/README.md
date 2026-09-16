# Data-quality gate

The quality gate has two complementary parts:

1. `dbt build` creates candidate models in `staging` and runs schema and SQL
   business-rule tests.
2. Great Expectations independently validates the resulting dimension and fact
   tables and the flattened `customer_360_current` view through PostgreSQL.

Both commands return a non-zero exit status on failure. Gold promotion must only
run after both succeed.

Prefect records dbt and Great Expectations as separate rows in
`staging.transformation_step_runs`. dbt failures include failed resource IDs
and messages parsed from `target/run_results.json`; Great Expectations
failures include the table, expectation configuration, and observed result in
`failure_details`. The parent transformation record exposes
`failed_step_name` for quick routing.

Run the Great Expectations checks inside the pipeline engine:

```bash
docker compose exec pipeline-engine \
  python -m pipeline.quality.great_expectations_checks
```
