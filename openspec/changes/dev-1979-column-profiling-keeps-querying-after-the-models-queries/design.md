## Context

See proposal.md — Why. Today four entry points profile columns independently:

- `inspect_model` (`slayer/inspect/model_render.py`): categorical loop via `ensure_column_sample_fresh`, an inline untyped numeric batch (`_profile_numeric_temporal_columns`) + its own `_persist_sample`, then a second, typed numeric batch `_collect_measure_profile` (the SQLite-harmful `CAST` the untyped path avoids) that writes `all NULL` for empty ranges.
- `inspect` column lookup (`slayer/inspect/service.py`) and search column hits (`slayer/search/service.py`): `ensure_column_sample_fresh` per column.
- Forced refresh (`refresh_table_backed_model_sampled`, used by CLI `refresh-samples` and `edit_model`'s `handle_edit_refresh`): `_refresh_one_column` per column, writing `None` on failure.

Every per-column helper catches its own exception and returns `None`, so no loop can tell "model unqueryable" from "column empty". Relevant principles: engine §3.3 (storage consulted once — profiling becomes one call per model), system §3.7 (async), §3.12 (Pydantic, no dataclasses), §3.15 (RLS fails closed).

## Goals / Non-Goals

**Goals:** one owner; bounded failing-model cost; failure cache; one WARNING per classified failure; RLS containment on the owner's read paths.

**Non-Goals:** the schema-drift check per failed query (DEV-1978); RLS containment of the BM25 corpus and embeddings, including their ranking side channel (DEV-2002); cross-call concurrency dedup (see Risks); `inspect_model`'s non-profiling queries (row count, sample rows, column types) — they are unchanged.

## Decisions

### D1 — One owner in `slayer/engine/profiling.py`

`async ensure_samples_fresh(*, model, columns, engine, storage, force=False) -> ProfileOutcome`, where `ProfileOutcome(BaseModel)` carries `columns: list[Column]` (the input columns, refreshed where profiled, in input order) and `errors: list[str]`. It filters out hidden / identifier / opaque columns, then (unless `force`) cached columns and failure-cached columns (or the whole model when failure-cached at model level). Categorical: one top-50 query per column (`_profile_categorical_with_total`'s query and overflow rules unchanged). Numeric/temporal: one untyped batched min/max query (the current `_profile_numeric_temporal_columns` shape — untyped to avoid the SQLite `CAST` coercion). Query helpers raise; the owner is the only `except`.

Callers: `inspect_model` passes all its uncached columns and renders from the returned columns (its inline numeric path, `_persist_sample` and `_collect_measure_profile` are deleted); `inspect` column lookup passes `[col]`; search groups each model's column hits into one call (it already groups hits per model); `refresh_table_backed_model_sampled` keeps its signature, its `_is_table_backed` gate and `only_columns`, and calls the owner with `force=True`, returning `outcome.errors`.

Alternative rejected: a failure counter in `model_render.py`'s loop + a module-level failed-set — leaves search / `inspect` unprotected and two copies of the rules.

### D2 — Classification: probe on first failure + consecutive-failure breaker

First failed query in a call → one probe: `count(*)` over the model via `engine.execute` (same policy and datasource). Probe fails → model-level. Probe succeeds → column-level; the model is marked healthy for the rest of the call (no more probes). Breaker: 3 consecutive column-level failures with no success in between → model-level ("profiling unavailable"), covering column-grant denials where `count(*)` passes. Numeric-batch failure on a healthy model → one min/max query per numeric column to isolate the bad ones.

No exception-type shortcut: `ForcedFilterError` (RLS fail-closed, model-wide) subclasses `SlayerError`, so "SlayerError ⇒ column-level" would misclassify it.

Alternatives rejected: stop on any failure (a single bad column starves its siblings); probe first always (an extra full-model query on every healthy cache miss).

### D3 — Failure cache: in-memory, engine-scoped, fingerprint-keyed

`_ENGINE_STATE: WeakKeyDictionary[SlayerQueryEngine, _EngineProfileState]` in `profiling.py`; `_EngineProfileState(BaseModel)` holds the failure entries (and, under a policy, the positive sample entries — D5). Keys: model-level `(data_source, model_name, model_fingerprint)`; column-level adds `(column_name, column_fingerprint)`. Fingerprint = stable hash of `model_dump_json` with every column's `sampled` / `sampled_values` / `distinct_count` excluded (so profiling a sibling never changes the key, while an edit does). Entry = expiry + error first line. TTL = 1 h against a module-level injectable monotonic clock (tests patch it). A weak key keeps the state's lifetime equal to the engine's; the engine needs no new attribute (and `profiling.py` → `query_engine` stays the only import direction).

The policy is immutable engine state, so engine scope = policy scope. Alternative rejected: a persisted failure marker on `Column` — shared across tenants, never expires, and (by repo convention) a model version bump.

### D4 — Successful-empty is cached; failures never clear samples

Categorical success with no non-NULL values → `sampled_values=[]`, `distinct_count=0`, `sampled=""`. Numeric success with min and max both NULL → `sampled="all NULL"` (adopting `inspect_model`'s existing convention for all paths). A failed profile never writes to storage, in lazy and forced mode alike (today forced refresh writes `None`).

### D5 — RLS containment inside the owner

`engine.policy is not None` → the owner treats every column as uncached, never calls `update_column_sampled`, stores profiled samples in the engine state under the D3 keys + TTL, and returns columns whose three sample fields hold the engine-scoped values or `None` (stored values are always overwritten in the returned copy, including on failure). `inspect_model`'s pre-pass must therefore not render stored samples under a policy — it renders only from the owner's returned columns. The search column-hit hook re-renders hit text from the returned column whenever the engine has a policy (not only when the column changed).

### D6 — Logging

One WARNING per recorded failure: `"sample profiling: model %s.%s unavailable (%d columns skipped): %s"` / `"sample profiling: column %s.%s.%s failed: %s"` (first error line). Failure-cache hits log at DEBUG. Persist failure: one WARNING per column, appended to `errors`, fresh value still returned, not failure-cached.

## Risks / Trade-offs

- [Concurrent reads of the same failing model each pay the bound] → accepted; each call stays bounded. A per-model `asyncio.Lock` was rejected: locks bind to the first event loop that uses them, and the sync bridges (`execute_sync` / `run_sync`) may drive one engine from several loops.
- [Per-process cache: each worker / restart retries once per hour] → accepted; bounded cost.
- [A transient DB error fails a healthy model for an hour] → forced refresh and any edit retry immediately; TTL bounds staleness.
- [Probe failure still triggers a schema-drift check] → DEV-1978.
- [Removing `_collect_measure_profile` changes SQLite date-range rendering in `inspect_model` (no `CAST`)] → affected expectations are raised for consent at spec-tests.
- [RLS: BM25 / embedding ranking still sees stored samples] → DEV-2002.
