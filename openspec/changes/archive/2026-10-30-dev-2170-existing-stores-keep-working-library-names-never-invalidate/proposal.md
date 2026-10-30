## Why

The SQL-grammar / portable-library / list-column stack breaks stores written by the last release: stored models whose custom aggregation or datasource granularity takes a newly reserved library name stop loading, memories saved on the new code become unreadable after a rollback, a database error is blamed on drift the failing statement never reads, valid SQLite date columns and working list-backed measures are proposed for deletion, and version-gated grammar upgrades corrupt hand-edited text. This is a release blocker: existing stores must keep working, and future library growth must never break them again (system principle 11.2).

## What Changes

- A model's custom aggregation may take a scalar/list function or parser-spelling name; inside its owning model every call spelling resolves to it, with a structured warning on save, load and every query response that resolves through it.
- A formula-backed definition of a built-in aggregation (`collect` included) is the user's aggregation at every site.
- A stored datasource custom granularity whose name collides with a library name is renamed by a stored-only migration, with every reference in that datasource's documents rewritten; a library-named granularity stays refused on save.
- A stored aggregation over a column named `distinct` (`agg(distinct)`) is migrated to the quoted identifier.
- Grammar-upgrade migrations leave text that is not old grammar unchanged, with a warning.
- Every persisted document omits unset (`None`) fields, so a store written by this release still reads on the previous one.
- Query-time drift attribution blames only drift touching an element the failing statement reads.
- On SQLite, a DATE/TIMESTAMP column over live TEXT is not drift.
- Any non-list column over a live list is a report-only re-type finding, and re-ingest re-types it in place.
- A test pins every reserved library vocabulary, so a future addition must either be model-shadowable or ship with a migration.
- Docs: schema drift, ingestion, and models §Schema versioning; arc42 principles 11.2 (enforced) and 14 (any non-list column).

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `models/aggregation-names`: library function names become shadowable model-local aggregation names, with a warning.
- `aggregations/collect`: a formula-backed `collect` definition is the user's aggregation.
- `models/stored-upgrade`: granularity-collision rename, `agg(distinct)` rewrite, the old-grammar gate, rollback-safe serialisation, and the library-growth guarantee.
- `models/schema-drift`: element-level attribution, SQLite temporal-over-TEXT compatibility, and the re-type finding for every non-list type.

## Impact

- Code: `slayer/core/models.py`, `slayer/core/keys.py`, `slayer/core/syntax.py`, `slayer/engine/binding.py`, `slayer/engine/elaborate_env.py`, `slayer/engine/compile/`, `slayer/engine/query_engine.py`, `slayer/engine/schema_drift.py`, `slayer/engine/ingestion.py`, `slayer/sql/generator.py`, `slayer/sql/dialects/`, `slayer/storage/` (migrations, both backends), `slayer/memories/`.
- Versions: SlayerModel 18, SlayerQuery 9, DatasourceConfig 3, Memory 7.
- Docs: `docs/concepts/schema-drift.md`, `docs/concepts/ingestion.md`, `docs/concepts/models.md`, `docs/development.md`.
- Architecture: `architecture/system.arc42.md` principles 11.2 and 14 (approved wording).
