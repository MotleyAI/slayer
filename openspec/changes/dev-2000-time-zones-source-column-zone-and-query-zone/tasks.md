## 1. Tests first (pr-tests stage)

- [ ] 1.1 Write failing executed-value tests (SQLite and DuckDB) for every scenario in `specs/queries/time-zones/spec.md`, on the spec's scenario data, for `America/New_York` and `Asia/Tokyo`. Each must fail for the missing feature, not because of a fixture error.
- [ ] 1.2 Write no-zone golden tests: byte-identical SQL and equal results for queries without a zone, with and without source zones, and for BigQuery, MySQL and ClickHouse instant fields typed `DateTime` vs `DateTimeTz` (filters, casts, truncation, joins, derived fields). These must pass before and after the change.
- [ ] 1.3 Write `tests/test_law_zoned_reads.py`, the law behind system principle 12. It holds the position matrix, run on SQLite and DuckDB:
  - field kinds: local, joined, derived, stage, query-backed, instant, civil, undeclared
  - positions: time dimension, `date_trunc` dimension/filter/order, custom granularity, `date_range`, relative token, `whole_periods_only`, `date_part`/`hour`/`day_of_week`, `date_diff`, `date_add`, trailing window, window transform, `time_shift`, `consecutive_periods`, `first`/`last` and ranking key, `min`/`max`, join key, raw rows, spine
  - plus a DST date in each zone
  It also holds the AST-level read check (D18) and one focused test per known bypass: `_resolve_sql` aggregate inputs, derived-field inner reads, semi-join keys, `_anchor` string anchors, session-policy predicates.
- [ ] 1.4 Write failing tests for the context split (D8):
  - a derived field, and a dataset metric with `FILTER (WHERE …)`, match their inline equivalents
  - field filters (including through a derived field), dataset filters and RLS give zone-independent results
  - an unmapped source column warns
  - a derived timestamp built from no timestamp read is civil
- [ ] 1.5 Write failing tests for validation and warnings:
  - unknown IANA names on query, field and datasource
  - a field zone refused on `DateTimeTz`, `Date`, non-temporal and derived fields, at construction and on assignment, leaving the object unchanged
  - `time_zone_undeclared` deduplication across aliases, repeated references, derived fields, joins, stages, query-backed datasets and dry-run, with both surfaces
  - Ossie-l3 refusal for a fresh query, a refinement and a stored stage
- [ ] 1.6 Write failing tests for stages:
  - inheritance by unpinned stages and query-backed stages
  - a pinned stage converting into its reader
  - the zone-state rule for each temporal-producing key kind
  - the repeated-hour reconversion
  - the cross-zone re-bucketing error
  - query-backed `time_zone` stamping
- [ ] 1.7 Write failing tests for the clock deltas (`specs/queries/time-points`, `specs/queries/date-functions`):
  - aware and naive injected clocks
  - tokens, `whole_periods_only`, the spine and `now()`/`current_date()` under a zone
  - zone-less behaviour unchanged
  - cache: zoned `current_date()` cached, `now()` bypassed
- [ ] 1.8 Write failing tests for ingestion, re-ingest and storage:
  - per-dialect `DateTimeTz` typing, including ClickHouse zoned `DateTime64` and MySQL `TIMESTAMP`
  - re-ingest re-type that changes nothing else
  - BigQuery `SlayerModel` 30 → 31 load upgrade (BigQuery only, load never writes, no-zone SQL unchanged)
  - field and datasource `time_zone` round trip through storage, Ossie export and import, valid against the Ossie schema
- [ ] 1.9 Write per-dialect emission tests in `tests/dialects/` for all nine Tier-1 dialects:
  - naive and instant conversion
  - converted-type literals, custom origins, spine and window bounds, including BigQuery `DATETIME` and ClickHouse zoned literals
  - pre-filter literal types, and its absence on frame bounds
  - the SQL Server unmapped-zone error
  - the Tier-2 typed error only when a conversion is needed
- [ ] 1.10 Write integration tests:
  - Postgres executed through pytest-postgresql: `timestamp` and `timestamptz`, a session zone of `Asia/Kolkata`, the DST rule
  - MySQL: `DATETIME`, `TIMESTAMP` under a non-UTC session; missing time-zone tables give the typed error before execution; the probe goes through the client once per zone, with request counts
  - ClickHouse: conversion and zoned literals
- [ ] 1.11 Write failing tests for the refinement delta (`specs/queries/saved-query-refinement`), the response's `time_zone`, and the surfaces (REST, MCP query/datasource/dataset tools, CLI `--time-zone`, inspect)

## 2. Core fields and validation

- [ ] 2.1 Add `time_zone` to `SlayerQuery`, `QueryRefinement` (including the `refine_query` merge list), `DatasourceConfig` and `DatasetField`, with IANA, `civil` and field-shape validation at construction and assignment. Verify the 1.5 validation tests and `tests/test_migrations.py` (legacy documents load unchanged).
- [ ] 2.2 Add the `time_zone_undeclared` warning kind (payload, `AnySlayerWarning`, Python carrier) and the typed time-zone errors. Verify the 1.5 warning payload tests.
- [ ] 2.3 Store `DatasetField.time_zone` in the `SLAYER` field payload and export it. Store `DatasourceConfig.time_zone` in the datasource YAML. Verify the 1.8 round-trip tests.

## 3. Ingestion and storage

- [ ] 3.1 Type BigQuery `TIMESTAMP`, ClickHouse `DateTime(tz)` / `DateTime64(p, tz)` and MySQL `TIMESTAMP` as `DateTimeTz` in reflection, and re-key the BigQuery zoned read, `bare_comparands` and `coerce_zoned_comparisons` on `DateTimeTz`. Verify 1.8 typing and the 1.2 goldens.
- [ ] 3.2 Re-ingest re-type of `DateTime` → `DateTimeTz` in `slayer/live_schema/ingestion.py`. Verify 1.8.
- [ ] 3.3 `SlayerModel` 30 → 31 load upgrade for BigQuery `db_type: TIMESTAMP` fields. Verify 1.8 and `tests/test_law_load_never_writes.py`.

## 4. Zone resolution and the clock

- [ ] 4.1 Resolve each stage's effective zone (own, else the final stage's, including query-backed stored stages) and carry it through prebound → `PlannedQuery` → render context. Verify the 1.6 inheritance tests.
- [ ] 4.2 Make the engine clock reading an aware instant, with naive readings as host-local. Derive a per-stage Q "now" for relative tokens, `whole_periods_only` and the spine bound. Render `now()` / `current_date()` as literals under a zone, and narrow `plans_read_clock`. Verify 1.7 and the existing time-points, spine and `tests/test_dev1737_clock_cache.py` tests.

## 5. The one read path

- [ ] 5.1 Build the typed read path in `slayer/sql`, taking the `value` / `stored predicate` context. It resolves the source zone, `civil` and the undeclared warning, and wraps the read in the dialect conversion. Route every physical read through it, including the bypasses listed in 1.3. Split `_anchor(FieldSqlKey)` so value and filter expand in their own contexts, recursively. Verify 1.3 and 1.4.
- [ ] 5.2 Add the AST-level read validator (D18), enabled in the test harness. Verify 1.3's check fails on an injected untagged read and passes on the suite.
- [ ] 5.3 Deduplicate the warning by dataset, field and join path in the execution-level accumulator, and surface it on both channels. Verify 1.5.
- [ ] 5.4 Convert zoned casts at the cast under a zone (D17). Verify the cast scenario.

## 6. Dialects

- [ ] 6.1 Implement the conversion primitives (D15) for Postgres, DuckDB, Snowflake, BigQuery, Trino, SQL Server (with the vendored CLDR table and its version recorded), MySQL, ClickHouse and SQLite (`slayer_convert_tz` with the D13 DST rule, registered with the other SQLite functions). Verify 1.9 and the DST tests.
- [ ] 6.2 Carry the converted SQL type into literal, custom-origin, spine-bound and window-bound rendering, and route SQLite comparisons through normalisation. Verify the 1.9 emission tests and the executed tests.
- [ ] 6.3 Add the MySQL zone probe through the engine's client, cached per client and zone (D16). Verify the 1.10 MySQL tests, including request counts.
- [ ] 6.4 Add the Tier-2 typed error when a conversion is needed. Verify 1.9.

## 7. Stages and re-bucketing

- [ ] 7.1 Add the zone state to `StageField`, computed by the dependency-closure walker (D10), with Z → Q conversion at the reader and query-backed `time_zone` stamping. Verify 1.6.
- [ ] 7.2 Add the cross-zone re-bucketing check to `check_time_dimension_field` / `check_axis_rebucket`. Verify 1.6.

## 8. Pre-filter

- [ ] 8.1 Add the D14 raw-column pre-filter: a naive literal, or a UTC instant through a dialect hook; omitted when it can't be rendered; never on frame bounds. Verify the presence, absence and result-equality tests, including Postgres under a non-UTC session.

## 9. Response and surfaces

- [ ] 9.1 Add `SlayerResponse.time_zone`. Verify its scenario.
- [ ] 9.2 Add the surfaces: REST `QueryRequest` and `refine`; MCP `query`, `create_datasource` / `edit_datasource`, dataset tools; CLI `datasources create --time-zone`; inspect output. Verify 1.11.

## 10. Docs and architecture

- [ ] 10.1 Docs:
  - a "Time zones" section in `docs/concepts/time.md`: the read rule, `civil`, stored definitions vs predicates, stage inheritance, the clock, DST and per-engine notes, the `DateTimeTz`-naive-under-a-zone note, the repeated-hour note, dialect entries seeing converted values
  - one sentence each in `docs/concepts/datasets.md`, `docs/concepts/queries.md`, `docs/configuration/datasources.md`, `docs/reference/mcp.md`, `docs/reference/rest-api.md`
  - `docs/database-support.md`: fix the outdated `db_type` claim and describe instant typing
  - `docs/concepts/ingestion.md`
  - the help memory `slayer/memories/help_content/05_time.md`
  Confirm `zensical.toml` needs no new page.
- [ ] 10.2 Confirm system principle 12's test tag resolves to `tests/test_law_zoned_reads.py`, and run `la-arch-check` and `npx -y likec4@1.47.0 validate architecture`.

## 11. Gates

- [ ] 11.1 Run the local test scope per the `/la:pr` flow (`la-affected-tests` plus the goldens and the dialect, ingestion and storage suites touched), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors) and `openspec validate dev-2000-time-zones-source-column-zone-and-query-zone --strict`.
