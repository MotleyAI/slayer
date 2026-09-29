## Why

Every failed data or EXPLAIN query runs `validate_models` over its whole datasource — schema enumeration, listing, introspection of every table, a trial of every `sql` model — with nothing reused: one `inspect` of a failing model sent 629 requests to a customer ClickHouse in 86 s, 26 of them full drift checks (DEV-1978). Attribution also judges the whole join component, so an unrelated failure becomes a `SchemaDriftError` whenever any reachable model has drift.

## What Changes

- Query-time drift attribution is scoped to the **read set**: the persisted models whose relations the final rendered statement contains. The renderer's one relation door stamps each model relation; the read set is collected from the final (post-pruning) statement AST. The join-component walk in attribution is removed.
- Scoped validation shares the one validation code path: only read `sql_table` objects are introspected (same enumeration/listing/keying), only read `sql` models are trial-executed, only read models are SQLite-probed. Explicit `validate_models`, ingest validation and `--force-clean` stay whole-datasource and always read live.
- Attribution reuses live-schema facts through a per-datasource snapshot cache (60 s TTL, single-flight, bounded); verdicts are recomputed from current persisted models on every failure. A cached listing's absence is trusted only for names it was taken to look for.
- Attribution runs against the query's resolved datasource only (a model with no `data_source` no longer validates every datasource).
- **BREAKING (payload semantics)**: `SchemaDriftError.models` (REST 422 `models`) lists the models the blamed `to_delete` entries name, not every touched model.

## Capabilities

### New Capabilities
- `models/schema-drift`: query-time schema-drift attribution — scope, live-fact reuse, datasource, and the `SchemaDriftError` payload.

### Modified Capabilities

## Impact

- `slayer/sql/generator.py` — relation stamping at `_emit_relation`; read-set collection from the final statement.
- `slayer/engine/query_engine.py` — `_Prepared.touched` = read set; `_maybe_raise_schema_drift` rewritten over scoped validation + snapshot cache; `_touched_models_for_plan`, `_collect_query_backed_base_names`, `_expand_join_graph`, `_load_join_graph_models` deleted.
- `slayer/engine/schema_drift.py` — scoped `validate_datasource` / `_collect_live_tables`; new `LiveSnapshotCache`.
- `slayer/core/errors.py` — `SchemaDriftError.models` semantics.
- REST `POST /query` 422 `schema_drift` body (`models` field).
- `docs/concepts/schema-drift.md`.
