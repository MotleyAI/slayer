## Why

Lazy sample-value profiling (DEV-1979, DEV-1980) keeps querying after a model's queries fail: one failing query per uncached column (27 for a 71-column model), none of it cached, so every later `inspect` repeats them all, each failure also paying a full schema-drift check (DEV-1978). The failures are swallowed with no log line. The root cause is structural: profiling has no model-level owner — three read paths loop over columns through a per-column helper that swallows its own failure, and `inspect_model` carries two further duplicate numeric profilers.

## What Changes

- One model-level profiling owner in `slayer/engine/profiling.py` that every read path and the forced refresh route through; it owns the cache check, the queries, persistence, failure classification, the failure cache, and logging.
- Failure classification: the first failed query in a call triggers one `count(*)` probe on the model. Probe fails → model-level failure: stop. Probe succeeds → column-level failure: continue, with a breaker that turns 3 consecutive column failures (no success in between) into a model-level failure. A failing numeric batch on a healthy model is re-run per column to isolate the bad column.
- Failures are cached in memory per engine for 1 hour, keyed by a fingerprint of the model (and column) definition, so an edit retries at once. Every classified failure logs one WARNING.
- A successful query always leaves the column cached: an empty categorical stores `[]`, an all-NULL numeric stores `all NULL` (today the search / `inspect` paths leave all-NULL numeric columns uncached and re-query them on every read).
- A forced refresh (CLI `refresh-samples`, `edit_model`) never clears an existing sample on failure (today it writes `None`).
- Under a row-level-security policy, profiling never reads or writes the persisted sample fields; scoped samples are cached in memory per engine, and search hit text is re-rendered from the scoped column. (Index-level containment is DEV-2002.)
- Removed: `ensure_column_sample_fresh`, `profile_column`, `_refresh_one_column`, `_profile_categorical_column`, `_collect_dim_profile`, `_DimProfileEntry`, `_format_dim_profile_value`, and `inspect_model`'s inline numeric profiler, `_persist_sample`, and `_collect_measure_profile`.

## Capabilities

### New Capabilities

- `models/sample-profiling`: lazy and forced profiling of `Column.sampled` / `sampled_values` / `distinct_count` — caching, failure classification and bounding, failure cache, logging, and row-level-security scoping.

### Modified Capabilities

(none)

## Impact

- Code: `slayer/engine/profiling.py` (rewritten owner), `slayer/inspect/model_render.py`, `slayer/inspect/service.py`, `slayer/search/service.py` (callers), `slayer/cli.py` (unchanged API of `refresh_table_backed_model_sampled` / `refresh_all_table_backed_sampled`).
- Tests: `tests/test_engine_profiling.py`, `tests/test_entry_points_namespacing.py`, `tests/test_dev1967_identifier_columns.py`, `tests/integration/test_mcp_inspect.py` rewritten against the owner (per-file consent at spec-tests); new scenario tests.
- Docs: `docs/concepts/search.md` (sample-value cache), `docs/concepts/row-level-security.md`.
- No storage schema change, no API change. Out of scope: the schema-drift check per failed query (DEV-1978); RLS containment of the BM25 / embedding indexes (DEV-2002).
