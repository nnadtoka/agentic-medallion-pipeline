-- ============================================================
-- Gold promotion: blue-green swap of validated candidate marts
-- (materialized in `marts_candidate`, per dbt_project.yml) into `gold`.
--
-- gold_promoter itself has zero direct privileges on bronze/staging/gold
-- (postgres/init/024_gold_promoter_grants.sh revokes everything there,
-- deliberately). It can only ever call this one allowlisted function.
-- SECURITY DEFINER makes the function run with its owner's (admin_user's)
-- privileges regardless of who invokes it -- that's the actual privilege
-- boundary here, not a table-level grant. See mcp_agent/README.md and
-- README's "blue-green schema promotion" line.
--
-- Blue-green with retention: the outgoing gold table is renamed to
-- <table>_previous (one generation of rollback), not dropped. Only the
-- immediately-previous generation is kept.
--
-- Incremental facts (fct_orders, fct_order_items, fct_order_payments) are
-- cloned back into marts_candidate right after the move (see
-- incremental_facts below). Without this, dbt's is_incremental() macro
-- would find no marts_candidate.<table> on the next run (SET SCHEMA moved
-- it to gold) and silently fall back to rebuilding the entire fact from
-- scratch every single run -- correct, but paying a full recompute of every
-- upstream join/aggregation instead of the cheap row-copy this does. True
-- partition-level incremental promotion (rebuild/swap only affected date
-- partitions) remains a further improvement; this closes the correctness/
-- performance gap at whole-table granularity in the meantime.
-- ============================================================

DROP FUNCTION IF EXISTS gold.promote_marts_to_gold();

CREATE OR REPLACE FUNCTION gold.promote_marts_to_gold(p_target_batch_id text)
RETURNS TABLE(promoted_table text, action text)
SECURITY DEFINER
SET search_path = pg_catalog
LANGUAGE plpgsql
AS $$
DECLARE
    mart_table text;
    index_name text;
    target_status text;
    updated_steps integer;
    snapshot_date date;
    snapshot_date_count integer;
    new_partition_name text;
    old_partition_schema text;
    old_partition_name text;
    old_partition_release text;
    previous_active_release text;
    previous_table_name text;
    candidate_view_definition text;
    gold_view_definition text;
    marts CONSTANT text[] := ARRAY[
        'dim_customer', 'dim_product', 'dim_seller', 'dim_date', 'dim_location',
        'dim_unique_customer_profile', 'fct_orders', 'fct_order_items',
        'fct_order_payments', 'fct_customer_category_activity_daily',
        'agg_seller_performance',
        'agg_customer_lifetime_value', 'agg_customer_interests'
    ];
    snapshot_marts CONSTANT text[] := ARRAY[
        'agg_customer_lifetime_value', 'agg_customer_interests'
    ];
    -- The only models actually materialized='incremental' (dbt/models/marts/
    -- fct_orders.sql, fct_order_items.sql, fct_order_payments.sql). Every
    -- other mart is materialized='table' (full rebuild by design each run,
    -- e.g. the snapshot aggregates and SCD2 profile), so cloning them back
    -- would cost a copy for zero incremental benefit.
    incremental_facts CONSTANT text[] := ARRAY[
        'fct_orders', 'fct_order_items', 'fct_order_payments'
    ];
BEGIN
    -- Serialize promotion and rollback without holding a permanent lock row.
    IF NOT pg_try_advisory_xact_lock(hashtext('gold_release_publication')) THEN
        RAISE EXCEPTION 'Another Gold promotion or rollback is already running';
    END IF;

    SELECT release_id
    INTO previous_active_release
    FROM gold_control.promotion_releases
    WHERE status IN ('active', 'active_partial_rollback');

    INSERT INTO gold_control.promotion_releases (
        release_id, transformation_batch_id, previous_release_id, status
    ) VALUES (
        p_target_batch_id, p_target_batch_id, previous_active_release, 'promoting'
    );

    SELECT status
    INTO target_status
    FROM staging.transformation_batches
    WHERE batch_id = p_target_batch_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown transformation batch: %', p_target_batch_id;
    END IF;

    IF target_status <> 'quality_passed' THEN
        RAISE EXCEPTION
            'Target batch % is not ready for promotion; current status is %',
            p_target_batch_id,
            target_status;
    END IF;

    -- Fail fast, before moving anything, if a candidate is missing --
    -- a partial promotion would leave gold in a worse state than either
    -- the old or new generation.
    FOREACH mart_table IN ARRAY marts LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = 'marts_candidate' AND tablename = mart_table
        ) THEN
            RAISE EXCEPTION
                'Candidate table marts_candidate.% does not exist; aborting promotion',
                mart_table;
        END IF;
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_views
        WHERE schemaname = 'marts_candidate'
          AND viewname = 'customer_360_current'
    ) THEN
        RAISE EXCEPTION
            'Candidate view marts_candidate.customer_360_current does not exist; aborting promotion';
    END IF;

    -- Capture the validated dbt definition before moving any dependencies, then
    -- drop both view objects. Moving the candidate view itself would preserve
    -- its OID dependencies on the standalone snapshot tables; after those
    -- tables become partitions, the Gold view would still point directly at
    -- the physical children instead of the stable partitioned parents. That
    -- breaks snapshot rollback because the view follows a rejected child into
    -- gold_rollback. Recreating the view after publication, with only the
    -- candidate schema rewritten to Gold, binds it to the stable parent names
    -- while keeping the dbt model as the single source of truth for its SELECT.
    SELECT pg_get_viewdef(
        'marts_candidate.customer_360_current'::regclass,
        true
    ) INTO candidate_view_definition;

    IF candidate_view_definition IS NULL THEN
        RAISE EXCEPTION
            'Could not read candidate view definition for customer_360_current';
    END IF;

    gold_view_definition := replace(
        candidate_view_definition,
        'marts_candidate.',
        'gold.'
    );

    IF gold_view_definition LIKE '%marts_candidate.%'
       OR position('gold.agg_customer_lifetime_value' IN gold_view_definition) = 0
       OR position('gold.agg_customer_interests' IN gold_view_definition) = 0
       OR position('gold.dim_location' IN gold_view_definition) = 0 THEN
        RAISE EXCEPTION
            'Candidate customer_360_current definition could not be safely rebound to Gold';
    END IF;

    DROP VIEW marts_candidate.customer_360_current;
    DROP VIEW IF EXISTS gold.customer_360_current;

    FOREACH mart_table IN ARRAY marts LOOP
        -- Each snapshot candidate is one complete daily partition. Publish it
        -- by attaching the validated table to a stable partitioned Gold
        -- parent. A same-date predecessor is detached and retained in
        -- gold_rollback, making publication and rollback catalog operations
        -- rather than a destructive DELETE/INSERT rewrite.
        IF mart_table = ANY(snapshot_marts) THEN
            EXECUTE format(
                'SELECT count(DISTINCT as_of_date), min(as_of_date) '
                'FROM marts_candidate.%I',
                mart_table
            ) INTO snapshot_date_count, snapshot_date;

            IF snapshot_date_count <> 1 OR snapshot_date IS NULL THEN
                RAISE EXCEPTION
                    'Candidate %.% must contain exactly one non-null as_of_date',
                    'marts_candidate', mart_table;
            END IF;

            IF EXISTS (
                SELECT 1
                FROM pg_class relation
                JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'gold'
                  AND relation.relname = mart_table
                  AND relation.relkind <> 'p'
            ) THEN
                RAISE EXCEPTION
                    'gold.% exists but is not partitioned; migrate it before partition-aware promotion',
                    mart_table;
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM pg_class relation
                JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'gold'
                  AND relation.relname = mart_table
                  AND relation.relkind = 'p'
            ) THEN
                EXECUTE format(
                    'CREATE TABLE gold.%1$I '
                    '(LIKE marts_candidate.%1$I INCLUDING DEFAULTS '
                    'INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING IDENTITY) '
                    'PARTITION BY RANGE (as_of_date)',
                    mart_table
                );
            END IF;

            SELECT partition_schema, partition_name, release_id
            INTO old_partition_schema, old_partition_name, old_partition_release
            FROM gold_control.published_snapshot_partitions
            WHERE table_name = mart_table
              AND as_of_date = snapshot_date
            FOR UPDATE;

            IF FOUND THEN
                EXECUTE format(
                    'ALTER TABLE gold.%I DETACH PARTITION %I.%I',
                    mart_table, old_partition_schema, old_partition_name
                );

                -- Free the live index names before the new candidate enters
                -- the Gold schema; retained rollback partitions keep suffixed
                -- equivalents.
                FOR index_name IN
                    SELECT index_class.relname
                    FROM pg_index index_meta
                    JOIN pg_class table_class ON table_class.oid = index_meta.indrelid
                    JOIN pg_class index_class ON index_class.oid = index_meta.indexrelid
                    JOIN pg_namespace ns ON ns.oid = table_class.relnamespace
                    WHERE ns.nspname = old_partition_schema
                      AND table_class.relname = old_partition_name
                LOOP
                    EXECUTE format(
                        'ALTER INDEX %I.%I RENAME TO %I',
                        old_partition_schema,
                        index_name,
                        left(index_name, 45) || '_r_' || left(md5(p_target_batch_id), 8)
                    );
                END LOOP;

                EXECUTE format(
                    'ALTER TABLE %I.%I SET SCHEMA gold_rollback',
                    old_partition_schema, old_partition_name
                );
                old_partition_schema := 'gold_rollback';
            ELSE
                old_partition_schema := NULL;
                old_partition_name := NULL;
                old_partition_release := NULL;
            END IF;

            new_partition_name := left(mart_table, 40)
                || '_p' || to_char(snapshot_date, 'YYYYMMDD')
                || '_r' || left(md5(p_target_batch_id), 8);
            EXECUTE format(
                'ALTER TABLE marts_candidate.%I RENAME TO %I',
                mart_table, new_partition_name
            );

            -- Index names are schema-wide in PostgreSQL. Every daily snapshot
            -- starts with the same dbt post-hook names, so scope the incoming
            -- partition's indexes to this release before moving it into Gold;
            -- otherwise a different already-attached date can collide.
            FOR index_name IN
                SELECT index_class.relname
                FROM pg_index index_meta
                JOIN pg_class table_class ON table_class.oid = index_meta.indrelid
                JOIN pg_class index_class ON index_class.oid = index_meta.indexrelid
                JOIN pg_namespace ns ON ns.oid = table_class.relnamespace
                WHERE ns.nspname = 'marts_candidate'
                  AND table_class.relname = new_partition_name
            LOOP
                EXECUTE format(
                    'ALTER INDEX marts_candidate.%I RENAME TO %I',
                    index_name,
                    left(index_name, 45) || '_p_' || left(md5(p_target_batch_id), 8)
                );
            END LOOP;

            EXECUTE format(
                'ALTER TABLE marts_candidate.%I SET SCHEMA gold',
                new_partition_name
            );
            EXECUTE format(
                'ALTER TABLE gold.%I ATTACH PARTITION gold.%I '
                'FOR VALUES FROM (%L) TO (%L)',
                mart_table,
                new_partition_name,
                snapshot_date,
                snapshot_date + 1
            );

            EXECUTE format('ALTER TABLE gold.%I OWNER TO admin_user', mart_table);
            EXECUTE format('ALTER TABLE gold.%I OWNER TO admin_user', new_partition_name);
            EXECUTE format('GRANT SELECT ON gold.%I TO analyst_junior', mart_table);
            EXECUTE format('GRANT SELECT ON gold.%I TO mcp_reader', mart_table);

            INSERT INTO gold_control.snapshot_partition_history (
                release_id, table_name, as_of_date, published_partition_name,
                previous_release_id, previous_partition_schema,
                previous_partition_name
            ) VALUES (
                p_target_batch_id, mart_table, snapshot_date,
                new_partition_name, old_partition_release,
                old_partition_schema, old_partition_name
            );

            INSERT INTO gold_control.published_snapshot_partitions (
                table_name, as_of_date, release_id,
                partition_schema, partition_name, published_ts
            ) VALUES (
                mart_table, snapshot_date, p_target_batch_id,
                'gold', new_partition_name, CURRENT_TIMESTAMP
            )
            ON CONFLICT (table_name, as_of_date) DO UPDATE
            SET release_id = EXCLUDED.release_id,
                partition_schema = EXCLUDED.partition_schema,
                partition_name = EXCLUDED.partition_name,
                published_ts = EXCLUDED.published_ts;

            promoted_table := mart_table;
            action := CASE WHEN old_partition_name IS NULL
                THEN 'snapshot_partition_attached'
                ELSE 'snapshot_partition_replaced'
            END;
            RETURN NEXT;
            CONTINUE;
        END IF;

        -- Retain only the immediately-previous generation.
        EXECUTE format('DROP TABLE IF EXISTS gold.%I', mart_table || '_previous');
        previous_table_name := NULL;

        IF EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = 'gold' AND tablename = mart_table
        ) THEN
            EXECUTE format(
                'ALTER TABLE gold.%I RENAME TO %I',
                mart_table, mart_table || '_previous'
            );
            previous_table_name := mart_table || '_previous';
            -- Nobody should query a demoted "previous" copy thinking it's live.
            EXECUTE format(
                'REVOKE ALL ON gold.%I FROM analyst_junior, mcp_reader',
                mart_table || '_previous'
            );
            -- Explicit dbt post-hook indexes retain their names when their
            -- table is renamed. Rename them too, otherwise moving the next
            -- candidate (whose indexes use the live names) into gold would
            -- collide with indexes attached to the retained previous table.
            FOR index_name IN
                SELECT index_class.relname
                FROM pg_index index_meta
                JOIN pg_class table_class
                  ON table_class.oid = index_meta.indrelid
                JOIN pg_class index_class
                  ON index_class.oid = index_meta.indexrelid
                JOIN pg_namespace table_namespace
                  ON table_namespace.oid = table_class.relnamespace
                WHERE table_namespace.nspname = 'gold'
                  AND table_class.relname = mart_table || '_previous'
            LOOP
                EXECUTE format(
                    'ALTER INDEX gold.%I RENAME TO %I',
                    index_name,
                    left(index_name, 54) || '_previous'
                );
            END LOOP;
        END IF;

        -- ALTER TABLE ... SET SCHEMA does not change ownership; dbt_transformer
        -- owns the table but has no USAGE on gold (023_dbt_transformer_grants.sh),
        -- so it couldn't use it there even though it nominally owns it. Reassign
        -- to admin_user so gold objects are consistently owned like the rest of
        -- the schema, then (re-)grant the read-only roles explicitly -- moving a
        -- table via SET SCHEMA does not inherit ALTER DEFAULT PRIVILEGES, which
        -- only applies to objects CREATEd after the rule was set.
        EXECUTE format('ALTER TABLE marts_candidate.%I SET SCHEMA gold', mart_table);
        EXECUTE format('ALTER TABLE gold.%I OWNER TO admin_user', mart_table);
        EXECUTE format('GRANT SELECT ON gold.%I TO analyst_junior', mart_table);
        EXECUTE format('GRANT SELECT ON gold.%I TO mcp_reader', mart_table);

        -- Restore a marts_candidate copy for incremental facts so the next
        -- dbt run's is_incremental() finds a prior build to merge against
        -- (see incremental_facts' declaration above) instead of silently
        -- doing a full rebuild. dbt_transformer must own it -- it's the role
        -- that runs the next `dbt build` and needs to write into it.
        IF mart_table = ANY(incremental_facts) THEN
            EXECUTE format(
                'CREATE TABLE marts_candidate.%1$I '
                '(LIKE gold.%1$I INCLUDING ALL)',
                mart_table
            );
            EXECUTE format(
                'INSERT INTO marts_candidate.%1$I SELECT * FROM gold.%1$I',
                mart_table
            );
            EXECUTE format(
                'ALTER TABLE marts_candidate.%I OWNER TO dbt_transformer',
                mart_table
            );
        END IF;

        INSERT INTO gold_control.release_table_history (
            release_id, table_name, published_table_schema,
            published_table_name, previous_table_schema,
            previous_table_name
        ) VALUES (
            p_target_batch_id, mart_table, 'gold', mart_table,
            CASE WHEN previous_table_name IS NULL THEN NULL ELSE 'gold' END,
            previous_table_name
        );

        promoted_table := mart_table;
        action := 'promoted';
        RETURN NEXT;
    END LOOP;

    EXECUTE 'CREATE VIEW gold.customer_360_current AS ' || gold_view_definition;

    -- Treat the dependency shape as a release invariant. A future change to
    -- the view-rebinding logic must fail this transaction rather than quietly
    -- publishing a view pinned to one replaceable child partition again.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_rewrite rewrite
        JOIN pg_depend dependency
          ON dependency.classid = 'pg_rewrite'::regclass
         AND dependency.objid = rewrite.oid
         AND dependency.refclassid = 'pg_class'::regclass
        JOIN pg_class relation ON relation.oid = dependency.refobjid
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE rewrite.ev_class = 'gold.customer_360_current'::regclass
          AND dependency.deptype = 'n'
          AND namespace.nspname = 'gold'
          AND relation.relname = 'agg_customer_lifetime_value'
          AND relation.relkind = 'p'
    ) OR NOT EXISTS (
        SELECT 1
        FROM pg_rewrite rewrite
        JOIN pg_depend dependency
          ON dependency.classid = 'pg_rewrite'::regclass
         AND dependency.objid = rewrite.oid
         AND dependency.refclassid = 'pg_class'::regclass
        JOIN pg_class relation ON relation.oid = dependency.refobjid
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE rewrite.ev_class = 'gold.customer_360_current'::regclass
          AND dependency.deptype = 'n'
          AND namespace.nspname = 'gold'
          AND relation.relname = 'agg_customer_interests'
          AND relation.relkind = 'p'
    ) OR EXISTS (
        SELECT 1
        FROM pg_rewrite rewrite
        JOIN pg_depend dependency
          ON dependency.classid = 'pg_rewrite'::regclass
         AND dependency.objid = rewrite.oid
         AND dependency.refclassid = 'pg_class'::regclass
        JOIN pg_class relation ON relation.oid = dependency.refobjid
        WHERE rewrite.ev_class = 'gold.customer_360_current'::regclass
          AND dependency.deptype = 'n'
          AND relation.relispartition
    ) THEN
        RAISE EXCEPTION
            'Gold customer_360_current is not bound to stable snapshot parents';
    END IF;

    ALTER VIEW gold.customer_360_current OWNER TO admin_user;
    GRANT SELECT ON gold.customer_360_current TO analyst_junior;
    GRANT SELECT ON gold.customer_360_current TO mcp_reader;

    promoted_table := 'customer_360_current';
    action := 'view_promoted';
    RETURN NEXT;

    UPDATE staging.transformation_step_runs
    SET status = 'success',
        completed_ts = CURRENT_TIMESTAMP,
        error_message = NULL,
        failure_details = NULL
    WHERE batch_id = p_target_batch_id
      AND step_name = 'gold_promotion'
      AND status = 'running';

    GET DIAGNOSTICS updated_steps = ROW_COUNT;
    IF updated_steps <> 1 THEN
        RAISE EXCEPTION
            'Running gold_promotion step not found for target batch %',
            p_target_batch_id;
    END IF;

    UPDATE staging.transformation_batches
    SET status = 'promoted',
        promoted_ts = CURRENT_TIMESTAMP,
        completed_ts = CURRENT_TIMESTAMP,
        error_message = NULL
    WHERE batch_id = p_target_batch_id;

    UPDATE gold_control.promotion_releases
    SET status = 'superseded', completed_ts = CURRENT_TIMESTAMP
    WHERE release_id = previous_active_release
      AND status IN ('active', 'active_partial_rollback');

    UPDATE gold_control.promotion_releases
    SET status = 'active', completed_ts = CURRENT_TIMESTAMP
    WHERE release_id = p_target_batch_id;
END;
$$;

ALTER FUNCTION gold.promote_marts_to_gold(text) OWNER TO admin_user;
REVOKE ALL ON FUNCTION gold.promote_marts_to_gold(text) FROM PUBLIC;
