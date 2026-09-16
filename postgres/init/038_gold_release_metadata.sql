-- Release and physical-partition metadata used by transactional Gold
-- publication. These tables are deliberately admin-owned: callers reach the
-- state machine only through SECURITY DEFINER promotion/rollback functions.

CREATE TABLE IF NOT EXISTS gold_control.promotion_releases (
    release_id          TEXT PRIMARY KEY,
    transformation_batch_id TEXT NOT NULL UNIQUE,
    previous_release_id TEXT REFERENCES gold_control.promotion_releases(release_id),
    status              TEXT NOT NULL CHECK (status IN (
                            'promoting', 'active', 'active_partial_rollback',
                            'superseded', 'rolled_back'
                        )),
    started_ts          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_ts        TIMESTAMPTZ,
    rolled_back_ts      TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_gold_release_one_active
    ON gold_control.promotion_releases ((status))
    WHERE status IN ('active', 'active_partial_rollback');

CREATE TABLE IF NOT EXISTS gold_control.published_snapshot_partitions (
    table_name          TEXT NOT NULL,
    as_of_date          DATE NOT NULL,
    release_id          TEXT NOT NULL REFERENCES gold_control.promotion_releases(release_id),
    partition_schema    TEXT NOT NULL DEFAULT 'gold',
    partition_name      TEXT NOT NULL,
    published_ts        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, as_of_date)
);

CREATE TABLE IF NOT EXISTS gold_control.snapshot_partition_history (
    release_id              TEXT NOT NULL REFERENCES gold_control.promotion_releases(release_id),
    table_name              TEXT NOT NULL,
    as_of_date              DATE NOT NULL,
    published_partition_name TEXT NOT NULL,
    previous_release_id     TEXT,
    previous_partition_schema TEXT,
    previous_partition_name TEXT,
    rolled_back_ts          TIMESTAMPTZ,
    PRIMARY KEY (release_id, table_name, as_of_date)
);

CREATE TABLE IF NOT EXISTS gold_control.release_table_history (
    release_id              TEXT NOT NULL REFERENCES gold_control.promotion_releases(release_id),
    table_name              TEXT NOT NULL,
    published_table_schema  TEXT NOT NULL DEFAULT 'gold',
    published_table_name    TEXT NOT NULL,
    previous_table_schema   TEXT,
    previous_table_name     TEXT,
    rolled_back_ts          TIMESTAMPTZ,
    PRIMARY KEY (release_id, table_name)
);

ALTER TABLE gold_control.promotion_releases OWNER TO admin_user;
ALTER TABLE gold_control.published_snapshot_partitions OWNER TO admin_user;
ALTER TABLE gold_control.snapshot_partition_history OWNER TO admin_user;
ALTER TABLE gold_control.release_table_history OWNER TO admin_user;

REVOKE ALL ON SCHEMA gold_control, gold_rollback FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA gold_control, gold_rollback FROM PUBLIC;
