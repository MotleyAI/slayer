## Why

Stores written by SLayer 0.10.x (model v10, query v4, memory v2) break when opened by 1.x in several ways:

- they fail to load offline;
- they fail to load when a join key is not a declared column;
- they fail to query when they hold rank orders, legacy time literals, or quoted aggregation placeholders;
- one unloadable document blocks unrelated saves, re-ingest and listings.

Every break comes from a check that got tighter, or a migration that missed a shape, without being exercised against documents a released version actually wrote.

## What Changes

- **Upgrade corpus.** A committed fixture store written by the real `motley-slayer==0.10.2` (YAML + SQLite + its DuckDB data), with recorded 0.10.2 query results. A unit test opens it with the datasource present and absent; every document must load, re-save, re-ingest, and query to the recorded rows.
- **Live type refinement** on load applies only to models stored below v8, the first version written only by refining ingest code. It was previously below v11, which made every v10 model with a DOUBLE column need a live datasource.
- **Undeclared join keys.** On load, a join key that names no declared column on its side, and is not a physical rename, becomes a declared hidden base column of that side's own document. This covers both the source and the target side.
- **New stored-only migration steps**: SlayerModel v13→14, SlayerQuery v5→6, Memory v3→4. They re-apply the rank-direction rewrite, now including the `raw_formula` of persisted expression order items, and repair legacy `date_range` values:
  - drop when the length is not 2;
  - strip a zone offset, keeping the wall-clock time;
  - turn a slashed date into ISO.
- **Legacy time literals in stored filters.** Literals compared against a DATE/TIMESTAMP column in stored query `filters` are repaired on load the same way. Unresolvable columns and non-temporal columns stay verbatim.
- **Typed load boundary.** Any failure to load one stored model or memory raises a typed `StoredDocumentLoadError` naming the document, with the cause chained. **BREAKING** for callers catching the inner exception class directly; it subclasses `ValueError`.
- **One-document isolation.** A shared helper loads every model (and every memory) and returns the loaded ones plus the failures.
  - Enumeration operations (save-time peer checks, ingest, listings, catalogs, search, summaries) skip a failed document and emit a structured `unloadable_document` warning once per document per operation.
  - `validate_models` reports the failure.
  - Operations that pick an answer among a datasource's models fail closed with the document's error.
- **Quoted aggregation placeholders.** A `{param}` inside an ordinary string literal of an aggregation formula is a placeholder. It counts as read, and it renders the bound literal's value inside the literal, escaped per dialect. A non-literal binding, or a placeholder in any other literal kind, raises a typed error. `{{` / `}}` are literal braces. **BREAKING**: `'{value}'` in a formula now raises instead of being emitted verbatim.

## Capabilities

### New Capabilities
- `models/stored-upgrade`: stores written by any released SLayer version load, re-save, re-ingest and query with that release's results. Covers live-refinement gating, the stored-only repair steps (rank `raw_formula`, legacy `date_range`, legacy filter literals) and the upgrade corpus.
- `models/document-isolation`: the typed per-document load boundary, and what each operation does when one stored model or memory cannot be loaded.

### Modified Capabilities
- `models/join-keys`: the load-time canonicalisation also turns an unmatched, validly named join key into a declared hidden base column, on both sides.
- `aggregations/formula-templates`: placeholders inside ordinary string literals substitute the bound literal's value instead of being inert.

## Impact

- **Storage:** `slayer/storage/base.py` (load pipeline, refinement gate, peer loading, isolation helper), `yaml_storage.py`, `sqlite_storage.py`, `migrations.py`, `rank_direction_migration.py`, and a new stored-repair module for legacy time literals.
- **Core:** `slayer/core/warnings.py` (new payload and carrier), the core errors module (`StoredDocumentLoadError`), and the model, query and memory version constants.
- **SQL:** `slayer/sql/sql_template.py`.
- **Engine:** `slayer/engine/ingestion.py`, `query_engine.py`, `population.py`, `param_binding.py`.
- **Other enumeration call sites:** `slayer/memories/resolver.py`, search, REST, MCP, CLI, Flight, PG facade, profiling, demo.
- **Docs:** one sentence in `docs/concepts/` on quoted aggregation placeholders.
- **Tests:** `tests/test_dev1934_formula_agg.py` (the inert-literal test is reversed); tests asserting an inner load exception class now assert it on `__cause__`.
- **DEV-2000 (time zones):** must rebase onto the new `CURRENT_VERSIONS` if it bumps any of them. Repaired literals are naive wall-clock time.
