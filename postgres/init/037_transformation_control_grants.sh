#!/bin/bash
set -euo pipefail

psql \
  -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-'SQL'
  GRANT SELECT, INSERT, UPDATE
      ON staging.transformation_batches
      TO dbt_transformer;

  GRANT SELECT, INSERT
      ON staging.model_batch_control
      TO dbt_transformer;

  GRANT SELECT, INSERT, UPDATE
      ON staging.transformation_step_runs
      TO dbt_transformer;

  REVOKE DELETE, TRUNCATE
      ON staging.transformation_batches,
         staging.model_batch_control,
         staging.transformation_step_runs
      FROM dbt_transformer;
SQL
