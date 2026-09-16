-- 00_schemas.sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;     -- was: public-as-gold
-- Dedicated candidate location for marts models only -- staging/intermediate
-- views and the transformation_control bookkeeping tables stay in `staging`.
-- Previously marts shared `staging` with those, which meant dbt's own
-- manifest.json schema field couldn't tell "this is a promotable candidate"
-- apart from "this is an unrelated staging view" (catalog_sync had to work
-- around it via fqn matching instead -- see pipeline/catalog_sync/manifest.py).
CREATE SCHEMA IF NOT EXISTS marts_candidate;
CREATE SCHEMA IF NOT EXISTS gold_control;
CREATE SCHEMA IF NOT EXISTS gold_rollback;
ALTER SCHEMA bronze          OWNER TO admin_user;
ALTER SCHEMA staging         OWNER TO admin_user;
ALTER SCHEMA gold            OWNER TO admin_user;
ALTER SCHEMA marts_candidate OWNER TO admin_user;
ALTER SCHEMA gold_control     OWNER TO admin_user;
ALTER SCHEMA gold_rollback    OWNER TO admin_user;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;   -- neutralize the leftover default schema
