# Semantic-layer probe suite

This suite reproduces the adversarial probing behind our comparison of SLayer with other semantic layers. Each
probe runs a query on one or more engines and checks the result against hand-written DuckDB SQL (`truth_sql`) on
the same data. Some probes expect a specific error or warning instead of a result.

## Engines

| Engine | Version | Runner |
|---|---|---|
| SLayer | this repository at HEAD, Python `duckdb` 1.5.2 | `slayer/run_slayer.py` |
| Malloy | `@malloydata/malloy` and `@malloydata/db-duckdb` 0.0.434 (bundled DuckDB 1.5.5) | `malloy/run_malloy.mjs` |
| Cube Core | `@cubejs-backend/server` and `@cubejs-backend/duckdb-driver` 1.7.46 (bundled DuckDB 1.5.5) | `cube/run_cube.py` |
| MetricFlow | `metricflow` 0.213.0, `dbt-metricflow` 0.15.0, `dbt-core` 1.12.5, `dbt-duckdb` 1.11.0 (DuckDB 1.5.5) | `metricflow/run_metricflow.py` |

The Node versions are pinned exactly in each `package.json` and `package-lock.json`; the MetricFlow versions in
`metricflow/requirements.txt`, installed into their own venv because of dbt-core's dependency pins.

## Contents

| Path | Purpose |
|---|---|
| `dataset.sql` | The shared dataset: `regions`, `customers`, `orders` and the `orders_flat` view |
| `probes.yaml` | One entry per probe, with one block per engine (field reference at the top of the file) |
| `slayer/models/` | SLayer models, including the saved query-backed `monthly_rev` |
| `malloy/model.malloy` | Malloy sources; each probe's query is appended to it |
| `cube/model/` | Cube base cubes: plain dimensions, base measures and joins |
| `cube/model_declared/` | Cube multi-stage measures, rolling windows and `sub_query` dimensions, layered on `cube/model/` |
| `cube/model_variants/` | Cube models that fail to compile, each loaded on its own |
| `metricflow/dbt_project/` | dbt project: staging views, a day time spine, semantic models with one simple metric per measure (`semantic.yml`), and model-declared metrics (`model_declared.yml`) |
| `run_all.sh` | Runs every engine |

The dataset keeps edge cases that break naive joins:

- Frank has no orders.
- Eve has a NULL region.
- Oslo appears in two regions.
- East has no customers.
- Order 20 has a NULL `customer_id`.
- There are no orders in 2024-04..11 or 2025-04.

Each runner seeds a fresh DuckDB file in a temp directory and computes the ground truth there.

## Running

Prerequisites: Poetry, Node 22 or later, and npm.

```bash
# From the repository root
poetry install -E all
poetry run python examples/comparisons/slayer/run_slayer.py [--row Q4] [--id Q10] [--verbose]

(cd examples/comparisons/malloy && npm ci && node run_malloy.mjs [--row Q4] [--id Q10] [--verbose])

(cd examples/comparisons/cube && npm ci)
poetry run python examples/comparisons/cube/run_cube.py [--planner tesseract|legacy|both] [--row Q4] [--id Q10] [--verbose]

# MetricFlow: its own venv (python3 -m venv, or uv venv where ensurepip is missing)
(cd examples/comparisons/metricflow && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt &&
  .venv/bin/python run_metricflow.py [--row Q4] [--id Q10] [--verbose])

# Everything
examples/comparisons/run_all.sh
```

`--row` keeps one feature-matrix row, `--id` keeps ids containing a substring, and `--verbose` prints the
generated SQL, the returned rows and the truth rows.

The Cube runner starts Cube Core itself (`node cube/node_modules/@cubejs-backend/server/bin/server`, dev mode,
DuckDB driver) and always stops it. It runs both SQL planners by default: Tesseract, and legacy via
`CUBEJS_TESSERACT_SQL_PLANNER=false`. It starts one server per planner and schema, because a compile error fails the
whole schema. Ports 4000 (REST) and 15432 (SQL API) must be free; `--port` and `--sql-port` change them. Dev mode
also starts Cube Store on ports 3030, 3031 and 13306. If `npm ci` fails to download the prebuilt native module, run
`npm rebuild @cubejs-backend/native`.

## Reading the output

Each probe prints one line: status, id, matrix row, title and a short detail. A summary table of counts per row
follows. The Cube runner prints one summary per planner for query-only probes and another for model-declared ones.

| Status | Meaning |
|---|---|
| `PASS` | Matches `truth_sql`, or fails with the expected error or warning |
| `KNOWN-BUG` | A known bug still reproduces: the result differs from the truth and shows the recorded buggy value |
| `FIXED` | A known bug no longer reproduces; update the probe's expectation |
| `FAIL` | Anything else |

Runners exit non-zero only on `FAIL`.

Some probes record a semantic difference rather than a bug. Examples are Malloy's row-based `lag` and Cube's root
selection. For these, `truth_sql` encodes what the engine does, and `contrast_sql` holds a plausible answer that the
engine must not return. Documented by-design limitations with an upstream ruling are reported as `KNOWN-BUG` with a
"by design" note.

Probes that appear on several engines share one `truth_sql`, so a PASS on one engine next to a KNOWN-BUG on another
compares them directly.

## Cube scope

- Cube Core only. No Cloud or Enterprise features.
- Query-semantics rows are graded only on what a query can express against the base model in `cube/model/`. That
  means REST `/load` (measures, dimensions, time dimensions with granularity, `dateRange` and `compareDateRange`,
  and/or filters, order, limit, timezone, `joinHints`) and the SQL API (plain queries, pushdown, and post-processing
  over a nested semantic query).
- Model-declared features are tagged `model_declared: true` and reported separately. These are multi-stage measures
  (`grain`, `time_shift`, `rank`), `rolling_window` and `sub_query` dimensions.
- Probes whose result depends on the planner are tagged `planners: [...]` or carry `planner_expect`. The two
  planners choose different root cubes for the same query: Tesseract usually roots at the dimension's cube, legacy
  at the measure's cube. In the SQL API, the first cube in `FROM` is the root on both planners.

## MetricFlow scope

- Open-source MetricFlow only (Apache-2.0). dbt platform features (JDBC/GraphQL APIs, caching, exports, the
  Semantic Layer MCP tools) are out of scope.
- The baseline model has one simple metric per measure. Query-semantics rows are graded on what `mf query` (or the
  Python `MetricFlowEngine`) can express against it: metrics, group-by items, Jinja `where` filters, order, limit,
  start and end.
- Derived, ratio, cumulative, offset and filtered metrics are model changes: they live in `model_declared.yml`, are
  tagged `model_declared: true`, and are reported separately.
- Measures on a table without a date column still need an aggregation time dimension, so `customers` and `regions`
  carry a constant one; `metric_time` means nothing for their metrics.

## Known issues

**SLayer**

1. A cross-model `count` returns NULL instead of 0 for a parent with no children.
   Probes: `Q8b`, `Q9a`, `B2`, `X1`.
2. Child rows with no parent (orders with a NULL `customer_id`) are added to the parent group whose dimension is
   NULL. Probes: `Q8c`, `B1`, `B1-assoc`.
3. `consecutive_periods` counts rows, so a streak continues across empty months. Probes: `Q14c`, `Q14d`.

**Malloy**

1. [malloydata/malloy#1762](https://github.com/malloydata/malloy/issues/1762): `join.count()` at a `join_many` leaf
   compiles to `COUNT(1)`, which counts a parent with no children as 1 and breaks the `orders.count() = 0`
   anti-join. `join.sum()` and `join.avg()` of a parent field include that parent too, with or without a primary
   key. Probes: `Q9a-*`, `Q10-antijoin-count`.
2. [malloydata/malloy#3105](https://github.com/malloydata/malloy/issues/3105): an analytic function inside an
   aggregate, such as `sum(row_number())`, passes the compiler and fails in the database. When it is the only
   aggregate, the compiler overflows its stack. Probes: `Q7-3105-*`.
3. [malloydata/malloydata.github.io#346](https://github.com/malloydata/malloydata.github.io/issues/346): the
   aggregates page swaps "forward" and "backward" for illegal asymmetric aggregates. The compiler accepts
   `orders.sum(credit)` (`Q9a-sum-parent`) and rejects `source.sum(orders.amount)` (`Q9a-traverses`).

**Cube Core**

1. [cube-js/cube#12020](https://github.com/cube-js/cube/issues/12020): with a `sub_query` dimension and a measure
   from the joined cube, Tesseract renders the measure as the subquery column and DuckDB rejects the SQL. Legacy
   returns the right answer. Probe: `Q5-cube-sub-query`.
2. [cube-js/cube#282](https://github.com/cube-js/cube/issues/282) (by design): ordering by a member that is not
   selected is dropped from the SQL without an error. Tesseract still honours an unselected measure; legacy drops
   both. Probes: `Q13-cube-order-*`.
3. [cube-js/cube#12024](https://github.com/cube-js/cube/issues/12024): `number_agg` without `multi_stage: true`
   fails schema validation, although the measures reference lists it as a plain type and Tesseract treats it as a
   built-in aggregation. Related: [#10799](https://github.com/cube-js/cube/issues/10799) (still reproduces on
   1.7.46), [#10798](https://github.com/cube-js/cube/issues/10798). Probe: `Q3-cube-number-agg-plain`.
4. [cube-js/cube#12025](https://github.com/cube-js/cube/issues/12025): the legacy planner silently ignores a
   multi-stage `grain` (`Q1-cube-keep-only`, `Q2-cube-share-of-region`, `Q3-cube-grain-include`), and drops
   months with no prior-year value from a `time_shift` query, along with their revenue
   (`Q4-cube-prior-year-with-revenue`).
5. [cube-js/cube#10166](https://github.com/cube-js/cube/issues/10166) (feature request, our repro added): the
   `timezone` query option treats `DATE` values as UTC midnight, so a negative-offset zone moves every date back
   one day. Probe: `Q21-cube-timezone`.

**MetricFlow**

1. Cumulative metrics at a coarser grain: `period_agg: first` / `last` take the first or last day that has a value,
   not the period's first or last calendar day, so a month whose last day has an empty window still reports a value.
   Probes: `Q11-mf-90d-last`, `Q4-mf-ytd-first`.
2. A `where` filter without Jinja (`revenue > 100`) binds to the row-level measure column named like the metric and
   filters individual orders before aggregation. Probe: `Q9-mf-raw-where-alias`.
3. By design: a count metric is NULL, not 0, for a parent with no children, so a metric filter `= 0` matches nothing
   (`Q10-mf-antijoin-eq0`). Multi-metric queries merge per-metric results with a full outer join, so orphan orders
   and a customer with a NULL region share one NULL row (`Q8c`, `Q8-mf-union-null`).
