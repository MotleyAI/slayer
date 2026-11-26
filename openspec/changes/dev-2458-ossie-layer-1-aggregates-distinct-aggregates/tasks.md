## 1. Tests first (pr-tests stage)

- [ ] 1.1 Parser tests: DISTINCT forms (count/sum/avg/min/max/array_agg ≡ their built-ins, refusals listing the accepted forms), WITHIN GROUP for PERCENTILE_CONT / PERCENTILE_DISC (MODE(), DESC, wrong fraction count refused), DISTINCT / WITHIN GROUP aggregate under `OVER` refused, canonical-text round trip of `sum_distinct(…)` / `percentile_disc(…, p=…)` — verify they fail before implementation
- [ ] 1.2 Executed DuckDB + SQLite scenarios per construct (spec `aggregations/distinct-and-percentiles`): values, FILTER, all-NULL group → NULL, boolean input, association dimension (each home row once), percentile_disc on text / date / boolean, endpoints and duplicates, positional p, invalid p refused at save time and at query time, default `allowed_aggregations` per type, a model's own `sum_distinct` override
- [ ] 1.3 Ossie compliance manifest (`tests/_dev2446_required_manifest.py`): flip `SUM(DISTINCT orders.qty)` and `PERCENTILE_DISC(0.25) …` gap → pass and `APPROX_PERCENTILE(orders.amount, 0.5)` refused → pass; the `AGGREGATES` family empties
- [ ] 1.4 Emission tests on every Tier-1 dialect for sum_distinct, avg_distinct, MIN/MAX(DISTINCT), percentile_disc (numeric, text, date; SQL Server type dispatch: native only for numeric), percentile_approx (native names, float cast), ClickHouse `quantileExactInclusive` for percentile / median with the non-NULL-count guard
- [ ] 1.5 Integration tests per server dialect (MySQL, Trino, SQL Server, BigQuery, ClickHouse, Postgres, Snowflake suites): percentile_disc grouped / ungrouped, NULLs, empty / all-NULL, p = 0 / 1, duplicates, two percentiles in one SELECT, FILTER, text / date; BigQuery NULL grouping keys, no GROUP BY, several keys; one association case per percentile_disc strategy and for sum_distinct; percentile_approx result types for INTEGER / DECIMAL on native paths; ClickHouse > 8192-row group equals the interpolated value, empty / all-NULL → NULL for nullable / non-nullable numeric and date
- [ ] 1.6 Strict tests: cross-product of written spelling × `slayer` / `ossie-l3` × FILTER, case variants, comments, nesting (spec `queries/ossie-l3-binding` scenarios); align `tests/test_dev2446_strict_filter.py` and `tests/test_dev2439_strict_windows.py` APPROX_PERCENTILE expectations mechanically
- [ ] 1.7 Importer tests: Cube / dbt SQL / Ossie dialect entries for SUM/AVG/MIN/MAX(DISTINCT), PERCENTILE_DISC, APPROX_PERCENTILE (Trino, Snowflake positive; BigQuery APPROX_QUANTILES dropped; STDDEV(DISTINCT), COUNT(DISTINCT a, b) dropped); dbt flags on the measure and the metric path (spec `importers/dbt-import`); align the old importer-refusal tests mechanically
- [ ] 1.8 Exporter tests: `sum_distinct` / `percentile_disc` portable and re-import with their values; `avg_distinct` / `percentile_approx` as datasource-dialect entries
- [ ] 1.9 Align `aggregations/functional-form` › "DISTINCT keyword rejected" tests to the new accepted forms
- [ ] 1.10 Library growth: `tests/test_library_growth.py` `MIGRATED` gains the four names → the SlayerModel 31 step; the stored-granularity and rename-gate guards cover them; the new capability's stored-granularity scenario

## 2. Core: names, typing, parser, strict

- [ ] 2.1 `core.enums`: add the four names to BUILTIN_AGGREGATIONS, value classes, numeric-only / boolean-lowered sets, per-type defaults (percentile_disc for text / date / timestamp; all four numeric / boolean), param order (`p`), type-preserving sets in `core.keys` / `engine.elaborate_env`; usage-reporting names — verify 1.2 typing scenarios pass
- [ ] 2.2 `core.syntax`: DISTINCT table, MIN/MAX collapse, DISTINCT recorded in the non-Ossie spelling report, WITHIN GROUP for PERCENTILE_DISC, OVER refusal, reworded errors — verify 1.1 passes
- [ ] 2.3 `core.ossie_conformance`: admit SUM(DISTINCT) / PERCENTILE_DISC WITHIN GROUP / APPROX_PERCENTILE(x, p); refuse AVG/MIN/MAX(DISTINCT) and the SLayer plain-call spellings by written callee — verify 1.6 passes
- [ ] 2.4 Storage: SlayerModel / SlayerQuery / Memory → 32 / 20 / 15 via no-op migrations; rename gate {32, 20, 15} for the four names — verify 1.10 passes

## 3. SQL emission

- [ ] 3.1 Render registry entries and dispatch for the four built-ins; `percentile_literal` reused for `p` — verify 1.2 passes on DuckDB / SQLite
- [ ] 3.2 Dialect hooks: base native PERCENTILE_DISC WITHIN GROUP; SQLite UDF widened to every ordered type; `_ordered_set_split` discrete pick for MySQL / Trino / BigQuery (analytic) / ClickHouse; SQL Server type-directed dispatch; percentile_approx native names + float cast, exact fallback — verify 1.4 and 1.5 pass
- [ ] 3.3 ClickHouse `quantileExactInclusive` for percentile / median with the non-NULL-count guard; `quantile(p)` for percentile_approx — verify 1.4 / 1.5 ClickHouse cases pass

## 4. Importers and exporter

- [ ] 4.1 `importers/aggregates.py`: DISTINCT, PERCENTILE_DISC, APPROX_PERCENTILE (literal fraction, no accuracy / weight) conversions; stale refusal messages removed — verify 1.7 passes
- [ ] 4.2 `dbt/converter.py`: percentile flags on measures and metrics — verify 1.7 dbt cases pass
- [ ] 4.3 `osi/exporter.py`: portable `SUM(DISTINCT …)` / `PERCENTILE_DISC …`; `avg_distinct` / `percentile_approx` non-portable — verify 1.8 passes

## 5. Surfaces and docs

- [ ] 5.1 MCP query-tool description (`core/query.py`) and help memory `03_aggregations` list the new built-ins — verify by reading the rendered tool description
- [ ] 5.2 Docs: aggregations concept doc + `docs/examples/07_aggregations` (re-run its notebook), `docs/interfaces/ossie-compliance.md` rows → supported (`tests/test_dev2220_compliance_page.py` passes), `docs/database-support.md` percentile notes incl. ClickHouse exactness, `docs/dbt/dbt_import.md`, `docs/cube/cube_import.md`, `docs/osi/osi_export.md`

## 6. Gates

- [ ] 6.1 Local test scope green (la-affected-tests + integration suites touched, incl. ClickHouse / MySQL / Trino via testcontainers); `ruff`, `basedpyright` (no new errors vs baseline), `la-arch-check`, `openspec validate dev-2458-ossie-layer-1-aggregates-distinct-aggregates --strict`
