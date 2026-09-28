## Context

`_maybe_raise_schema_drift` (`slayer/engine/query_engine.py`) wraps both the data query and EXPLAIN. Its `touched` set comes from `bundle.referenced_models`, which `bundle_builder._collect_referenced_models` deliberately makes the datasource's whole join component (edges walked both ways), then `_expand_join_graph` re-walks the component. It calls `validate_models(model.data_source)` — `None` means every datasource — and filters entries to `touched`.

`validate_datasource` (`slayer/engine/schema_drift.py`) runs `_live_schema_for_datasource` (enumerate own-catalog schemas → list each → `_introspect_one_table` per object, ~3 requests each, keyed bare/qualified per `_add_live_object`), trial-executes every `sql` model, runs the SQLite probe, then cascades (`compute_datasource_drops`).

`SQLGenerator._emit_relation` (`slayer/sql/generator.py`) is the one door a model reaches the SQL through (FROM, join target, semi-join hop). Multi-stage generation renders every stage, then prunes unreachable CTEs (`reachable_cte_entries`), so reads observed during rendering over-approximate the statement.

## Goals / Non-Goals

**Goals:** attribution cost proportional to the tables the failed statement read, paid at most once per datasource per 60 s; attribution evidence limited to what the statement read; one validation code path.

**Non-Goals:** column-granular attribution (drift on an unused column of a read model still counts); error-class gating of attribution; profiling / agent retry loops (DEV-1979, DEV-1983); a public TTL setting.

## Decisions

**D1 — Read set from stamped relations in the final AST.** `_emit_relation` stamps the node it returns (`meta["slayer_model"] = model.name`) for `sql_table` tables and embedded `sql` subqueries; synthesized stage relations (those in `_gen_stage_relations`) are not stamped. After final assembly — post-pruning, pre-policy — the engine walks the statement and collects stamped names into `_Prepared.touched`; spliced query-backed models whose stages survive are added by name (the splice path records them on the rendered stage). sqlglot 30.17 preserves `meta` through `copy()` and nesting. *Alternatives:* recording reads during `_emit_relation` (over-approximates: pruned stages); parsing `exp.Table` names back to models (a second source of truth; `sql` models are anonymous subqueries). `_touched_models_for_plan`, `_collect_query_backed_base_names`, `_expand_join_graph`, `_load_join_graph_models` are deleted (verify no other callers via LSP).

**D2 — Scoped validation, one path.** `validate_datasource` takes the models to validate plus `available_in_ds` (all DS model names; join-target existence checks need the full set) and an optional snapshot. `_collect_live_tables` takes an optional `wanted` object-name set: enumeration, listing and keying are unchanged; unwanted objects skip `_introspect_one_table`. `wanted = None` is today's behaviour. Only in-scope `sql` models are trialled, only in-scope models probed; the cascade runs over the in-scope models. *Alternative:* targeted `has_table` lookups — rejected: a second resolution path re-implementing bare/single-schema/catalog keying, and dialect-variable view coverage.

**D3 — `LiveSnapshotCache` (schema_drift.py, one per engine, attribution-only).** Per datasource key (`_sql_client_cache_key`): a `DriftSnapshot` with `created_at`, the catalog (schema refs + per-schema listings + the set of names each listing was taken for), `LiveTable`s filled lazily, `sql` trial results keyed by SQL text (live columns or `None`, plus `_source_tables_resolve`), and an `unavailable` flag. Whole-snapshot expiry 60 s after `created_at` (re-listing does not extend it). Absence rule: a wanted name absent from the cached listing is trusted only if it was in that listing's wanted set; otherwise re-list. Single-flight via a per-key `asyncio.Lock`; blocking introspection stays in `asyncio.to_thread`. LRU cap 256 keys, expired entries swept on access. Private constants; injectable clock. The SQLite probe is not cached (local file). A connection failure (listing, introspection, or a failed `sql` trial whose `SELECT 1` also fails) marks the snapshot unreachable: no verdict and no further requests until expiry. No invalidation hooks: persisted models are never cached, so edits are always seen. *Alternative:* memoizing `validate_models` verdicts — rejected: whole-DS first cost and stale verdicts across model edits.

**D4 — Datasource.** Attribution validates `prepared.datasource` only.

**D5 — Payload.** `SchemaDriftError(models=...)` receives the sorted distinct `model_name`s of the filtered entries. The `invalid_sql` exclusion and swallow-and-re-raise of internal attribution errors are unchanged.

## Risks / Trade-offs

- [A table dropped after a healthy snapshot surfaces as the raw DB error for up to 60 s] → accepted; bounded, degrades diagnosis only, never a false verdict or deletion.
- [A relation reaching SQL without the stamp is silently unread] → the door is the single emit path; a ratchet test renders a corpus of query shapes and asserts every model relation carries the stamp.
- [A transform that rebuilds `Table` nodes drops `meta`] → collection happens on the final AST before policy rewriting; the ratchet test covers identifier fitting and CTE hoisting.
- [Cache memory in a multi-tenant engine] → LRU cap + expiry sweep.
