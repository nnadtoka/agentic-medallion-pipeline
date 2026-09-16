# Future Improvements

Known gaps, found via adversarial code review, not yet fixed.

- **Ingestion idempotency race.** The successful-batch check in `ingest_olist.py` runs before
  the write transaction, and `bronze.ingestion_batches` only has a non-unique lookup index
  (`021_ingestion_batches.sql`). Two concurrent runs for the same dataset/date can both see "no
  success" and both append the same rows. Fix: add locking, e.g., via Postgres lock and re-check success after acquiring it.

- **Historical snapshots leak future customers.** `agg_customer_lifetime_value.sql` and
  `agg_customer_interests.sql` start from every customer in the current source snapshot, only
  limiting *activity* by `as_of_date` — not the customer universe itself. A snapshot for
  2017-11-06 emits all 96,096 customers instead of the 32,033 actually observed by then,
  distorting historical population/retention analysis. This only impacts our ingestion simulating production like situation for several tables ingested by date of records. Fix: restrict the customer universe to
  identities observed by the end of `as_of_date`.

- **Semantic catalog sync never removes stale entries.** `sync.py` upserts changed records but
  never deletes ones absent from the current dbt manifest, so a removed model/column/description
  stays searchable through MCP indefinitely. Template seeding has the same gap for deleted YAML
  files. Fix: diff against current state and delete (or soft-delete) records no longer present.
