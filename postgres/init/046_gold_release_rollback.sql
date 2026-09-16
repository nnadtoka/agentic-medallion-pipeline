-- Atomic rollback of the currently active complete Gold release. Ordinary
-- blue/green tables and snapshot partitions are restored together, followed
-- by rebinding the derived flattened consumer view to the restored tables.

CREATE OR REPLACE FUNCTION gold.rollback_release(p_release_id text)
RETURNS TABLE(object_name text, object_type text, action text)
SECURITY DEFINER
SET search_path = pg_catalog
LANGUAGE plpgsql
AS $$
DECLARE
    table_record record;
    partition_result record;
    index_name text;
    failed_table_name text;
    view_definition text;
    previous_release text;
    release_status text;
    expected_tables CONSTANT text[] := ARRAY[
        'dim_customer', 'dim_product', 'dim_seller', 'dim_date', 'dim_location',
        'dim_unique_customer_profile', 'fct_orders', 'fct_order_items',
        'fct_order_payments', 'fct_customer_category_activity_daily',
        'agg_seller_performance'
    ];
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtext('gold_release_publication')) THEN
        RAISE EXCEPTION 'Another Gold promotion or rollback is already running';
    END IF;

    SELECT status, previous_release_id
    INTO release_status, previous_release
    FROM gold_control.promotion_releases
    WHERE release_id = p_release_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Unknown Gold release: %', p_release_id;
    END IF;
    IF release_status NOT IN ('active', 'active_partial_rollback') THEN
        RAISE EXCEPTION
            'Release % is not active; current status is %',
            p_release_id, release_status;
    END IF;
    IF previous_release IS NULL THEN
        RAISE EXCEPTION
            'Release % has no predecessor and cannot be fully rolled back',
            p_release_id;
    END IF;

    IF (
        SELECT count(*)
        FROM gold_control.release_table_history
        WHERE release_id = p_release_id
          AND table_name = ANY(expected_tables)
    ) <> cardinality(expected_tables) THEN
        RAISE EXCEPTION
            'Release % does not have a complete ordinary-table rollback manifest',
            p_release_id;
    END IF;

    IF to_regclass('gold.customer_360_current') IS NULL THEN
        RAISE EXCEPTION
            'Release % does not have the current serving view',
            p_release_id;
    END IF;

    -- Fail before changing anything if a current or predecessor table is gone.
    FOR table_record IN
        SELECT *
        FROM gold_control.release_table_history
        WHERE release_id = p_release_id
          AND table_name = ANY(expected_tables)
        ORDER BY table_name
    LOOP
        IF table_record.rolled_back_ts IS NOT NULL THEN
            RAISE EXCEPTION 'Table % was already rolled back', table_record.table_name;
        END IF;
        IF table_record.previous_table_name IS NULL THEN
            RAISE EXCEPTION
                'Release % has no predecessor for table %',
                p_release_id, table_record.table_name;
        END IF;
        IF to_regclass(format(
            '%I.%I', table_record.published_table_schema,
            table_record.published_table_name
        )) IS NULL THEN
            RAISE EXCEPTION 'Published table %.% is missing',
                table_record.published_table_schema,
                table_record.published_table_name;
        END IF;
        IF to_regclass(format(
            '%I.%I', table_record.previous_table_schema,
            table_record.previous_table_name
        )) IS NULL THEN
            RAISE EXCEPTION 'Predecessor table %.% is missing',
                table_record.previous_table_schema,
                table_record.previous_table_name;
        END IF;
    END LOOP;

    -- Capture the dbt-built definition while its references still use stable
    -- Gold names. Dropping it prevents OID dependencies from following failed
    -- tables into gold_rollback. Recreating it after restoration rebinds the
    -- same definition to the restored table OIDs without another SQL copy.
    SELECT pg_get_viewdef('gold.customer_360_current'::regclass, true)
    INTO view_definition;
    DROP VIEW gold.customer_360_current;

    FOR table_record IN
        SELECT *
        FROM gold_control.release_table_history
        WHERE release_id = p_release_id
          AND table_name = ANY(expected_tables)
        ORDER BY table_name
    LOOP
        -- Rename current indexes before retaining the failed table so restored
        -- predecessor index names remain collision-free in Gold.
        FOR index_name IN
            SELECT index_class.relname
            FROM pg_index index_meta
            JOIN pg_class table_class ON table_class.oid = index_meta.indrelid
            JOIN pg_class index_class ON index_class.oid = index_meta.indexrelid
            JOIN pg_namespace ns ON ns.oid = table_class.relnamespace
            WHERE ns.nspname = table_record.published_table_schema
              AND table_class.relname = table_record.published_table_name
        LOOP
            EXECUTE format(
                'ALTER INDEX %I.%I RENAME TO %I',
                table_record.published_table_schema,
                index_name,
                left(index_name, 43) || '_failed_' || left(md5(p_release_id), 8)
            );
        END LOOP;

        failed_table_name := left(table_record.table_name, 42)
            || '_failed_' || left(md5(p_release_id), 8);
        EXECUTE format(
            'ALTER TABLE %I.%I RENAME TO %I',
            table_record.published_table_schema,
            table_record.published_table_name,
            failed_table_name
        );
        EXECUTE format(
            'ALTER TABLE %I.%I SET SCHEMA gold_rollback',
            table_record.published_table_schema,
            failed_table_name
        );

        EXECUTE format(
            'ALTER TABLE %I.%I RENAME TO %I',
            table_record.previous_table_schema,
            table_record.previous_table_name,
            table_record.table_name
        );
        EXECUTE format('ALTER TABLE gold.%I OWNER TO admin_user', table_record.table_name);
        EXECUTE format('GRANT SELECT ON gold.%I TO analyst_junior', table_record.table_name);
        EXECUTE format('GRANT SELECT ON gold.%I TO mcp_reader', table_record.table_name);

        UPDATE gold_control.release_table_history
        SET rolled_back_ts = CURRENT_TIMESTAMP
        WHERE release_id = p_release_id
          AND release_table_history.table_name = table_record.table_name;

        object_name := table_record.table_name;
        object_type := 'table';
        action := 'previous_table_restored';
        RETURN NEXT;
    END LOOP;

    -- If snapshot rollback has not already been performed separately, restore
    -- it now as part of this same outer transaction.
    IF EXISTS (
        SELECT 1
        FROM gold_control.snapshot_partition_history
        WHERE release_id = p_release_id AND rolled_back_ts IS NULL
    ) THEN
        FOR partition_result IN
            SELECT * FROM gold.rollback_snapshot_partitions(p_release_id)
        LOOP
            object_name := partition_result.table_name;
            object_type := 'snapshot_partition';
            action := partition_result.action;
            RETURN NEXT;
        END LOOP;
    END IF;

    EXECUTE 'CREATE VIEW gold.customer_360_current AS ' || view_definition;

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
            'Rolled-back customer_360_current is not bound to stable snapshot parents';
    END IF;

    ALTER VIEW gold.customer_360_current OWNER TO admin_user;
    GRANT SELECT ON gold.customer_360_current TO analyst_junior;
    GRANT SELECT ON gold.customer_360_current TO mcp_reader;

    UPDATE gold_control.promotion_releases
    SET status = 'rolled_back', rolled_back_ts = CURRENT_TIMESTAMP
    WHERE release_id = p_release_id;

    UPDATE gold_control.promotion_releases
    SET status = 'active', completed_ts = CURRENT_TIMESTAMP
    WHERE release_id = previous_release;

    object_name := 'customer_360_current';
    object_type := 'view';
    action := 'rebound';
    RETURN NEXT;
END;
$$;

ALTER FUNCTION gold.rollback_release(text) OWNER TO admin_user;
REVOKE ALL ON FUNCTION gold.rollback_release(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION gold.rollback_release(text) TO gold_promoter;
