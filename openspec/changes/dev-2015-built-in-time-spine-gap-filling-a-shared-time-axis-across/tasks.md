## 0. Preconditions

- [x] 0.1 Merge `origin/main` after DEV-1999 has landed (never rebase); verify `openspec/specs/queries/time-points/spec.md` exists and the full unit suite is green before any DEV-2015 work
- [x] 0.2 Add the `queries/time-points` delta (custom granularities as `<unit>` in relative tokens — `last fiscal_year`, `last 2 fiscal_years`, `this fiscal_year` — and in `whole_periods_only` snapping), present the exact delta to the user for approval (the plan is frozen after pr-plan), then verify `openspec validate dev-2015-built-in-time-spine-gap-filling-a-shared-time-axis-across --strict` passes

## 1. Tests first (pr-tests stage)

- [x] 1.1 Shared fixtures (`tests/_dev2015_fixtures.py`): the spec's customers/orders/returns seed on SQLite and DuckDB, an injectable engine clock, and a datasource defining the four custom granularities; verify the fixture module imports and seeds both dialects
- [x] 1.2 `queries/time-spine` executed tests, one per scenario (spine listing and inspection, reserved name, axis inference incl. stages and the sole-column default for transforms / `first`/`last`, nearest-axis routing, equal-route error, no routing through the spine, product population incl. explicit P / explicit spine / two spine TDs / measure invariance, bounds incl. mid-bucket, exclusive period upper, implied upper at month and day, frame bounds, `whole_periods_only`, filters on P / facts / non-bound spine, no countable rows, plain spine dimension, dense-series transforms and `coalesce`, re-bucketing through the spine, RLS, cache hit/rollover, calendar-model parity); verify each fails for the right reason before implementation
- [x] 1.3 `queries/custom-granularities` tests (definition round-trip across storage/REST/MCP/CLI/inspect, every validation error, fiscal/billing/sprint/quarter-hour bucketing incl. before-origin, functional form + order key + filter, spine at a custom grain, unknown-name and datasource-scoping errors, nesting cases); verify they fail before implementation
- [x] 1.4 Modified-capability tests: population (spine factors out, spine-only unit, product reporting), time-dimensions (datasource callee, unknown callee at binding for dimensions and `time_dimensions` strings), column-granularity (custom value round-trip and re-bucketing, undefined name on save), transforms (fiscal-year shift and change, custom unit on a month axis, sprint streaks), trailing-window (window at a cell without home rows, two homes + partition + `last` + sibling invariance); verify they fail before implementation
- [x] 1.5 Law-style tests: sibling-measure / population invariance and grain-union broadcast over spine populations (extend the existing law harness instances), NULL-axis rows excluded, DATE and TIMESTAMP axes, model filters on an axis column; verify they fail or pass as expected pre-implementation
- [ ] 1.6 Golden SQL baselines for the two-fact and per-group spine queries and a custom-grain query (postgres, sqlite, duckdb, tsql, bigquery), and integration tests for spine generation, two facts and custom buckets on PostgreSQL (`pytest-postgresql`), MySQL, ClickHouse, SQL Server (testcontainers), plus the 35,040-row quarter-hour year on SQLite/DuckDB/PostgreSQL
- [x] 1.7 Codex review of the test suite against this change; fold findings, then verify the suite is red only on DEV-2015 behaviour

## 2. Granularity type and datasource definitions

- [x] 2.1 `CustomGranularity` Pydantic model and `DatasourceConfig.granularities` with save-time validation; verify 1.3 validation tests pass
- [x] 2.2 Open granularity type (built-in or reference) and a resolved-definition type; replace every `TimeGranularity(...)` construction / dispatch site (query construction, keys, stage schemas, `Column.granularity`, binding, transforms, generator, dialects) with the resolved form; verify the full unit suite stays green
- [x] 2.3 Bundle resolution of granularity references per datasource; functional `name(col)` with a non-built-in callee resolved at binding (dimensions, `time_dimensions` strings, order keys, filters); unknown-name typed error; verify the time-dimensions and custom-granularity binding tests pass
- [x] 2.4 Custom bucketing via `date_diff` / `date_add` with floor correction; verify the bucketing tests on SQLite and DuckDB
- [x] 2.5 One nesting function over resolved definitions (built-ins as its special case) used by re-bucketing, `Column.granularity` and `whole_periods_only`; verify the nesting and column-granularity tests
- [x] 2.6 Custom steps for `time_shift` / `change` / `change_pct` / `consecutive_periods` and custom `time_shift` units; verify the transforms delta tests

## 3. Spine model, wiring and routing

- [x] 3.1 Effective default time dimension (declared, else sole temporal column; propagated or sole for query-backed models and stages) feeding transforms and `first`/`last`; verify the axis-inference tests
- [x] 3.2 Bundle synthesis of `time_spine` and axis edges per datasource; reserved-name rejection on save and fail-closed clash at query time; verify the listing and reserved-name tests
- [x] 3.3 Sink rule in the join graph traversal (spine edge only as first/last hop) for every router; verify existing cross-fact queries are unchanged with spine edges present
- [x] 3.4 Nearest-axis spine routing over executable to-one hop tokens with the equal-route error; verify the routing tests

## 4. Population, bounds and checks

- [x] 4.1 Spine factor in population inference (spine TDs and spine frame bounds are not determination items; unit P; reporting `time_spine × P`); verify the population delta tests
- [x] 4.2 Bound extraction from DEV-1999's frame-bound predicate, missing-lower-bound error, implied `this <finest granularity>` upper bound from the engine clock; verify the bounds tests
- [x] 4.3 Checker errors: spine aggregation, plain / raw-rows spine column, non-bound spine filter, re-bucketing through a wired axis column; verify the error tests
- [x] 4.4 IR: spine factor on the planned query (granularities, bounds) beside P; verify planning of every spine test produces it

## 5. SQL emission

- [ ] 5.1 Integer-sequence dialect hook for every Tier-1 dialect and the Tier-2 default; verify golden SQL and the integration generation tests
- [x] 5.2 Bucket-series emission with the overlap filter, crossed with distinct P; producers joined on complete grain with promoted comparands; spine bounds lowered onto fact axes and stripped from window / shift frames; verify the executed spine tests on SQLite and DuckDB
- [x] 5.3 Windowed producer evaluated at the population's cells (local and cross-model); verify the trailing-window delta tests and the existing trailing-window suite
- [x] 5.4 RLS and cache behaviour for spine queries; verify the RLS and cache tests

## 6. Surfaces, importers, docs, architecture

- [ ] 6.1 MCP / REST / CLI datasource create/edit accept `granularities`; `inspect` / `inspect_model` / search / model listings show the spine and its wiring and the datasource granularities; verify the surface tests
- [ ] 6.2 dbt importer: `fill_nulls_with` → `coalesce(<agg>, v)`, `join_to_timespine` accepted with an ingest-report note; verify importer tests
- [ ] 6.3 Docs: `docs/concepts/time.md` Time spine + Custom granularities sections, `queries.md` Population, `models.md` effective default time dimension, datasource configuration `granularities`, agent help content, MCP `query` tool docs; `zensical.toml` nav if a page is added; verify docs build and grep for stale statements ("no time-spine gap filling", granularity lists)
- [ ] 6.4 Apply exactly the arc42 edits approved in design.md D12 (Axiom 15, Axiom 12 cross-reference; the enforcing test file must exist — any other wording or tag needs a fresh user OK); run `la-arch-check` (pinned) and verify green
- [ ] 6.5 Full unit suite, integration suite (CI invocation), `ruff check`, `basedpyright` (no baseline growth), `la-arch-check`; verify all green
