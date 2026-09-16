-- postgres/init/10_roles.sql
-- Runs once on first init, after 00_schemas.sql.
-- Read-only analyst role, scoped to gold only. Password set by 15_analyst_password.sh.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_junior') THEN
    CREATE ROLE analyst_junior LOGIN PASSWORD 'placeholder_overridden_by_sh';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE analytics_db TO analyst_junior;
GRANT USAGE  ON SCHEMA gold TO analyst_junior;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO analyst_junior;

-- Future gold tables auto-grant SELECT (does NOT cover SET SCHEMA swaps — swap task handles that)
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT SELECT ON TABLES TO analyst_junior;

-- Defensive: ensure no silver/bronze access
REVOKE ALL ON SCHEMA staging FROM analyst_junior;
REVOKE ALL ON SCHEMA bronze  FROM analyst_junior;
REVOKE ALL ON ALL TABLES IN SCHEMA staging FROM analyst_junior;
REVOKE ALL ON ALL TABLES IN SCHEMA bronze  FROM analyst_junior;
