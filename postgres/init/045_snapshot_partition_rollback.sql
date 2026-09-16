-- Roll back only the daily snapshot partitions published by one release.
-- Stable partitioned parent tables remain in place, so dependent views keep
-- working throughout the transactional detach/attach swap.

CREATE OR REPLACE FUNCTION gold.rollback_snapshot_partitions(p_release_id text)
RETURNS TABLE(table_name text, as_of_date date, action text)
SECURITY DEFINER
SET search_path = pg_catalog
LANGUAGE plpgsql
AS $$
DECLARE
    partition_record record;
    current_partition_name text;
    rollback_partition_name text;
    index_name text;
    release_status text;
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtext('gold_release_publication')) THEN
        RAISE EXCEPTION 'Another Gold promotion or rollback is already running';
    END IF;

    SELECT status
    INTO release_status
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

    IF NOT EXISTS (
        SELECT 1 FROM gold_control.snapshot_partition_history
        WHERE release_id = p_release_id AND rolled_back_ts IS NULL
    ) THEN
        RAISE EXCEPTION 'Release % has no published snapshot partitions to roll back', p_release_id;
    END IF;

    -- Preflight every partition before changing any attachment.
    FOR partition_record IN
        SELECT *
        FROM gold_control.snapshot_partition_history
        WHERE release_id = p_release_id AND rolled_back_ts IS NULL
        ORDER BY table_name, as_of_date
    LOOP
        SELECT partition_name
        INTO current_partition_name
        FROM gold_control.published_snapshot_partitions
        WHERE published_snapshot_partitions.table_name = partition_record.table_name
          AND published_snapshot_partitions.as_of_date = partition_record.as_of_date
          AND release_id = p_release_id;

        IF NOT FOUND THEN
            RAISE EXCEPTION
                'Release % is not currently published for %.%',
                p_release_id, partition_record.table_name, partition_record.as_of_date;
        END IF;

        IF partition_record.previous_partition_name IS NOT NULL
           AND to_regclass(format(
               '%I.%I',
               partition_record.previous_partition_schema,
               partition_record.previous_partition_name
           )) IS NULL THEN
            RAISE EXCEPTION
                'Retained predecessor %.% is missing',
                partition_record.previous_partition_schema,
                partition_record.previous_partition_name;
        END IF;
    END LOOP;

    FOR partition_record IN
        SELECT *
        FROM gold_control.snapshot_partition_history
        WHERE release_id = p_release_id AND rolled_back_ts IS NULL
        ORDER BY table_name, as_of_date
    LOOP
        SELECT partition_name
        INTO current_partition_name
        FROM gold_control.published_snapshot_partitions
        WHERE published_snapshot_partitions.table_name = partition_record.table_name
          AND published_snapshot_partitions.as_of_date = partition_record.as_of_date
          AND release_id = p_release_id
        FOR UPDATE;

        EXECUTE format(
            'ALTER TABLE gold.%I DETACH PARTITION gold.%I',
            partition_record.table_name, current_partition_name
        );

        FOR index_name IN
            SELECT index_class.relname
            FROM pg_index index_meta
            JOIN pg_class table_class ON table_class.oid = index_meta.indrelid
            JOIN pg_class index_class ON index_class.oid = index_meta.indexrelid
            JOIN pg_namespace ns ON ns.oid = table_class.relnamespace
            WHERE ns.nspname = 'gold'
              AND table_class.relname = current_partition_name
        LOOP
            EXECUTE format(
                'ALTER INDEX gold.%I RENAME TO %I',
                index_name,
                left(index_name, 44) || '_rb_' || left(md5(p_release_id), 8)
            );
        END LOOP;

        rollback_partition_name := left(current_partition_name, 50) || '_rolledback';
        EXECUTE format(
            'ALTER TABLE gold.%I RENAME TO %I',
            current_partition_name, rollback_partition_name
        );
        EXECUTE format(
            'ALTER TABLE gold.%I SET SCHEMA gold_rollback',
            rollback_partition_name
        );

        IF partition_record.previous_partition_name IS NULL THEN
            DELETE FROM gold_control.published_snapshot_partitions
            WHERE published_snapshot_partitions.table_name = partition_record.table_name
              AND published_snapshot_partitions.as_of_date = partition_record.as_of_date;
            action := 'new_partition_removed';
        ELSE
            EXECUTE format(
                'ALTER TABLE %I.%I SET SCHEMA gold',
                partition_record.previous_partition_schema,
                partition_record.previous_partition_name
            );
            EXECUTE format(
                'ALTER TABLE gold.%I ATTACH PARTITION gold.%I '
                'FOR VALUES FROM (%L) TO (%L)',
                partition_record.table_name,
                partition_record.previous_partition_name,
                partition_record.as_of_date,
                partition_record.as_of_date + 1
            );

            UPDATE gold_control.published_snapshot_partitions
            SET release_id = partition_record.previous_release_id,
                partition_schema = 'gold',
                partition_name = partition_record.previous_partition_name,
                published_ts = CURRENT_TIMESTAMP
            WHERE published_snapshot_partitions.table_name = partition_record.table_name
              AND published_snapshot_partitions.as_of_date = partition_record.as_of_date;
            action := 'previous_partition_restored';
        END IF;

        UPDATE gold_control.snapshot_partition_history
        SET rolled_back_ts = CURRENT_TIMESTAMP
        WHERE release_id = p_release_id
          AND snapshot_partition_history.table_name = partition_record.table_name
          AND snapshot_partition_history.as_of_date = partition_record.as_of_date;

        table_name := partition_record.table_name;
        as_of_date := partition_record.as_of_date;
        RETURN NEXT;
    END LOOP;

    UPDATE gold_control.promotion_releases
    SET status = 'active_partial_rollback',
        rolled_back_ts = CURRENT_TIMESTAMP
    WHERE release_id = p_release_id
      AND status IN ('active', 'active_partial_rollback');
END;
$$;

ALTER FUNCTION gold.rollback_snapshot_partitions(text) OWNER TO admin_user;
REVOKE ALL ON FUNCTION gold.rollback_snapshot_partitions(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION gold.rollback_snapshot_partitions(text) TO gold_promoter;
