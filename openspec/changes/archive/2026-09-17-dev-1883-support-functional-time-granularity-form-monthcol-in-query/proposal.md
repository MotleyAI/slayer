# Proposal: functional time-granularity form `month(col)` in query dimensions

## Why

Agents naturally write transformed dimensions as function calls (`month(created_at)`), but SLayer expresses granularity only via `time_dimensions`, so the form dies at binding with an unknown-function error and every client must learn the `TimeDimension` dict shape. Accepting the functional form natively keeps the query surface consistent with the Motley scaffold's advertised notation and lets its translation layer eventually be dropped.

## What Changes

- A string entry `gran(col)` in `SlayerQuery.dimensions` — `gran` case-insensitively one of the nine `TimeGranularity` values, `col` a bare or dotted column ref — rewrites at query construction into a `TimeDimension` appended to `time_dimensions` (mirrors the func-style aggregation rewrite; downstream behaviour identical to the explicit form).
- The same string form is accepted as a `time_dimensions` entry; any other string there gets a clear error instead of a bare type error.
- A granularity-shaped order entry `gran(col)` resolves to the projected time dimension's bucket (position parity, `architecture/semantics.arc42.md` §2.13/§3.4); granularity calls in Mode-B filters are explicitly deferred.
- Wrong-shape granularity calls and unknown `name(bare_col)` calls in dimensions fail at construction with typed errors naming the valid granularities.
- Multiple time dimensions on one column get granularity-suffixed result keys (`orders.created_at.month`); exact duplicates dedupe; same column+granularity with differing metadata is rejected.
- **BREAKING**: custom aggregation names colliding with `TimeGranularity` values are rejected at model validation (alongside the existing transform/scalar reserved names), making granularity-vs-aggregation shadowing unrepresentable.
- The functional form is advertised in the help topics, the MCP `query` tool arg docs (whose granularity list currently omits four values), and user docs.

## Capabilities

### New Capabilities

- `queries/time-dimensions`: the time-dimension input surface — functional granularity form in `dimensions` / `time_dimensions` / order keys, its error surface, equivalence with the explicit `TimeDimension` form, and result-key disambiguation for same-column time dimensions.
- `models/aggregation-names`: reserved custom-aggregation names — granularity-value collisions rejected at model validation (the pre-existing transform/scalar reservations stay implementation-specified).

### Modified Capabilities

None.

## Impact

- `slayer/core/query.py` (construction-time rewrite + validators), `slayer/core/errors.py` (new typed error), `slayer/core/models.py` (reserved names), `slayer/engine/stage_planner.py` (collision-suffixed time-dimension keys), `slayer/mcp/server.py` + `slayer/api/server.py` (accept string `time_dimensions` entries; doc text).
- No SQL-emission or dialect changes; no import-linter impact (the `engine.syntax` import already exists in `core/query.py`).
- Breaking only for stored models defining a custom aggregation named like a granularity (judged vanishingly rare; fails loudly at model validation).
- Docs: `docs/concepts/queries.md`, help topics `01_queries.md`/`05_time.md`, `.claude/skills/slayer-query.md`.
