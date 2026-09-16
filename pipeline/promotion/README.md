# Gold promotion

Blue-green promotion of validated candidate marts (materialized in `marts_candidate`
per `dbt_project.yml`) into `gold`.

## How it's actually safe

The privilege boundary lives in Postgres, not in this Python code:

- `postgres/init/040_gold_promotion_function.sql` defines
  `gold.promote_marts_to_gold(batch_id)`, `SECURITY DEFINER`, owned by
  `admin_user`.
- `postgres/init/041_gold_promoter_grants.sh` grants `gold_promoter`
  `EXECUTE` on that one function and nothing else -- `gold_promoter` has no
  direct grants on `bronze`/`staging`/`gold` at all
  (`postgres/init/024_gold_promoter_grants.sh`).

So `gold_promoter` can only ever trigger this one specific, allowlisted
operation; it runs with the function owner's privileges internally
regardless of who calls it. `gold_promotion.py` is just the caller.

## What the function does

For each allowlisted dimension/fact table:
1. Fails the whole call up front if any candidate is missing from `marts_candidate`
   (no partial promotions).
2. Drops the existing `<table>_previous` in `gold`, if any (keeps only one
   generation of rollback).
3. Renames the current `gold.<table>` (if it exists) to `<table>_previous`,
   and revokes `analyst_junior`/`mcp_reader` access to it -- a demoted copy
   shouldn't look queryable as if it were live.
4. Moves `marts_candidate.<table>` into `gold` via `ALTER TABLE ... SET SCHEMA`,
   reassigns ownership to `admin_user` (moving a table doesn't move
   ownership, and `dbt_transformer` -- the original owner -- has no `USAGE`
   on `gold` anyway), and re-grants `SELECT` to `analyst_junior`/`mcp_reader`
   (moving a table doesn't pick up `ALTER DEFAULT PRIVILEGES`, which only
   applies to newly `CREATE`d objects).

All of this runs inside the one function call, in one transaction: if it
raises partway through, everything done so far in that call rolls back.
Daily customer snapshots use native PostgreSQL range partitions. Each candidate
must contain exactly one `as_of_date`; promotion attaches it beneath a stable
partitioned Gold parent. A matching predecessor is detached into
`gold_rollback`, recorded in `gold_control`, and can be restored with
`gold.rollback_snapshot_partitions(release_id)`. Older dates remain attached.
The validated `marts_candidate.customer_360_current` definition is captured and
the candidate view is dropped before its dependencies move. After publication,
the same dbt-built definition is recreated in Gold with its schema references
rewritten from `marts_candidate` to `gold`. This deliberately binds the serving
view to the stable partitioned snapshot parents rather than to one physical
child partition, while avoiding a second hand-maintained copy of the SELECT.
The outgoing derived view is dropped rather than retained as rollback state.

Snapshot-only rollback deliberately marks the release
`active_partial_rollback`: ordinary dimensions/facts remain on that release
while its snapshot partitions point back to their predecessors.

The same transaction marks the `gold_promotion` step successful and changes
the target transformation batch from `quality_passed` to `promoted`.

The three `materialized='incremental'` facts (`fct_orders`, `fct_order_items`,
`fct_order_payments`) are cloned back into `marts_candidate` right after the
move, owned by `dbt_transformer`. Without this, dbt's `is_incremental()`
would find no `marts_candidate.<table>` on the next run (the move relocated
it to `gold`) and silently do a full rebuild of the fact every single run --
correct, but discarding all incremental benefit. Every other mart is
`materialized='table'` (full rebuild by design each run) and isn't cloned
back, since there'd be nothing to gain. This is still a whole-table copy, not
true partition-level incremental promotion (rebuilding and swapping only
affected date partitions); that finer-grained design remains a further
improvement.

## Run it directly

```bash
docker compose exec pipeline-engine python -m pipeline.promotion.gold_promotion \
  '<quality-passed-target-batch-id>'
```

Or as part of the full gate, via the `transformation_flow` Prefect flow,
which only calls this after both the intermediate and marts Great
Expectations gates have passed.

## Rollback

`gold.rollback_release(release_id)` restores the currently active release in
one transaction: all ordinary tables return to their recorded `_previous`
generation, snapshot partitions return to their retained predecessors, and
`customer_360_current` is rebound to the restored tables from its captured
dbt-built definition. Failed physical objects are retained in
`gold_rollback` for diagnosis.

The function refuses non-active releases, first releases with no predecessor,
incomplete manifests, missing predecessor objects, and already-rolled-back
tables. It takes the same advisory publication lock as promotion.

Run it through the least-privilege operator wrapper:

```bash
docker compose exec pipeline-engine \
  python -m pipeline.promotion.gold_rollback '<active-release-id>'
```

Snapshot-only emergency rollback remains available through
`gold.rollback_snapshot_partitions(release_id)`, but produces the explicit
`active_partial_rollback` state. Prefer complete release rollback when the
whole release is suspect, and prefer a corrected roll-forward for normal
recovery. The function accepts only the currently `active` (or already
`active_partial_rollback`) release; passing a superseded or rolled-back release
fails before any partition is detached.
