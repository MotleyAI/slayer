# SLayer, Malloy, Cube and MetricFlow compared

All four let you describe *what* you want from data instead of writing the SQL. This page compares how directly
each one lets a person or an agent express common analytical intents: in one query, as one expression, or only
through extra steps. Each row comes from one product's own highlighted features and says which, so a product scores
well on its own rows by construction; read the per-row detail rather than the totals.

Cube and MetricFlow are judged as their open-source releases, Cube Core 1.7.46 and MetricFlow 0.213, and on the
query-semantics rows only by what a query can express against an existing model. Claims marked *verified* are backed
by a probe in the [comparison suite](https://github.com/MotleyAI/slayer/tree/main/examples/comparisons), which runs
one dataset through all four engines and checks every answer against hand-written SQL. Status as of October 2026.

✅ native, in one query or expression · 🟡 needs extra steps or stages, a derived source or custom SQL, or has
material limits · ❌ not expressible, or no protection · ❔ not documented

## Takeaways

- **Four ways to compose.** SLayer composes *expressions* in the query
  (`avg(sum(x, partition_by=[city, region]))`, `time_shift(ratio, -1, 'year')`) and the planner derives the stages.
  Malloy composes *queries*: aggregate-then-`calculate:` per stage, and anything deeper is another `->`. Cube's
  semantic composition lives in model-declared multi-stage measures; at query time, Cube Core composes only by
  writing SQL over the semantic result through its SQL API, where correctness (re-aggregating non-additive measures,
  say) is up to the author. MetricFlow composes *metric definitions*: ratio, derived, cumulative and offset metrics
  are declared in the model, and a query only picks metrics, group-by items and filters.
- **On the rows SLayer brought (Q1–Q19)**, SLayer does all of them in one ad-hoc query. Malloy does 7 natively,
  Cube Core 2 (fan-out-safe joins and the APIs) and MetricFlow 2 (fan-out-safe joins and time-bucket safety). Most
  of the rest are possible elsewhere with extra stages or SQL over the results.
- **On the rows Malloy brought** (Q20–Q23, C1–C4, C8–C11), Malloy leads or shares the lead everywhere except
  pre-aggregation, where Cube leads: nested data, a filter and time-range language, calendar functions, view
  refinement, `nest:`, a value index, IDE feedback and SQL interop. SLayer matches it on the time-range language and
  refinement, and is partial or missing on the rest.
- **On the rows Cube brought** (C12–C19), Cube Core leads or shares the lead on all of them: BI access over the
  Postgres wire protocol, curated views, field-level security and masking, custom calendars, drill-across and
  drill-down. SLayer matches it on wire-protocol BI access, custom calendars and drill-across.
- **On the rows MetricFlow brought** (C20–C25), MetricFlow leads or shares the lead throughout: a shared time axis
  across facts, gap filling from a time spine, semi-additive measures, point-in-time joins and an open interchange
  spec. SLayer matches it on entity-graph joins, the shared axis, gap filling and semi-additive measures;
  point-in-time joins and interchange are partial.
- **Time is where SLayer stands out.** It is the only one with calendar-aware shifts, time-based rolling windows and
  a gap-filled shared time axis expressible ad hoc in a query, whose lookback a date filter doesn't clip. Cube and
  MetricFlow have them only as model declarations (MetricFlow's `metric_time`, time spine and custom granularities
  come closest); Malloy's and Cube Core's ad-hoc windows count rows.
- **Silent-semantics pitfalls.** Malloy, Cube Core and MetricFlow each show verified wrong-number or
  silently-ignored cases, [listed below](#bugs-and-silent-cases-found); the SLayer bugs the suite found are fixed.

## At a glance

<!-- glance:start -->

| | ✅ Yes | 🟡 Partial | ❌ No | ❔ Unknown | Leads a row |
|---|---|---|---|---|---|
| **SLayer** | 31 | 10 | 7 | 0 | 31 |
| **Malloy** | 23 | 21 | 4 | 0 | 25 |
| **Cube Core** | 15 | 27 | 6 | 0 | 17 |
| **MetricFlow** | 13 | 16 | 19 | 0 | 13 |

*Leads a row* counts the rows where the tool has the best verdict, ties included.

**Query semantics**

| | Capability | SLayer | Malloy | Cube Core | MetricFlow |
|---|---|---|---|---|---|
| Q1 | [Coarser grain in the same query](#q1-coarser-grain-in-the-same-query) | ✅ | ✅ | 🟡 | ❌ |
| Q2 | [Arithmetic across grains](#q2-arithmetic-across-grains) | ✅ | ✅ | 🟡 | ❌ |
| Q3 | [Re-aggregation](#q3-re-aggregation) | ✅ | 🟡 | 🟡 | ❌ |
| Q4 | [Transforms / time shift](#q4-transforms-time-shift) | ✅ | 🟡 | 🟡 | ❌ |
| Q5 | [Calculated dimensions, filters, order](#q5-calculated-dimensions-filters-order) | ✅ | 🟡 | 🟡 | 🟡 |
| Q6 | [Fields from many models, fan-out safe](#q6-fields-from-many-models-fan-out-safe) | ✅ | ✅ | ✅ | ✅ |
| Q7 | [Deep composition](#q7-deep-composition) | ✅ | 🟡 | 🟡 | ❌ |
| Q8 | [Population inference](#q8-population-inference) | ✅ | ❌ | 🟡 | 🟡 |
| Q9 | [No silently wrong numbers](#q9-no-silently-wrong-numbers) | ✅ | 🟡 | 🟡 | 🟡 |
| Q10 | [Filters across one-to-many joins](#q10-filters-across-one-to-many-joins) | ✅ | ✅ | 🟡 | 🟡 |
| Q11 | [Rolling time windows](#q11-rolling-time-windows) | ✅ | 🟡 | 🟡 | ❌ |
| Q12 | [Aggregates as arguments](#q12-aggregates-as-arguments) | ✅ | 🟡 | 🟡 | ❌ |
| Q13 | [Order by what you don't show](#q13-order-by-what-you-dont-show) | ✅ | 🟡 | 🟡 | ❌ |
| Q14 | [Ranking and streaks](#q14-ranking-and-streaks) | ✅ | 🟡 | 🟡 | ❌ |
| Q15 | [Multi-stage queries](#q15-multi-stage-queries) | ✅ | ✅ | 🟡 | ❌ |
| Q16 | [Inline model extension](#q16-inline-model-extension) | ✅ | ✅ | 🟡 | ❌ |
| Q17 | [Saved measures that compose](#q17-saved-measures-that-compose) | ✅ | 🟡 | 🟡 | 🟡 |
| Q18 | [Time-bucket safety](#q18-time-bucket-safety) | ✅ | ❌ | ❌ | ✅ |
| Q19 | [Agent & API surface](#q19-agent-api-surface) | ✅ | ✅ | ✅ | 🟡 |
| Q20 | [Relative & typed filters](#q20-relative-typed-filters) | ✅ | ✅ | ✅ | 🟡 |
| Q21 | [Calendar expressions](#q21-calendar-expressions) | 🟡 | ✅ | 🟡 | 🟡 |
| Q22 | [Nested source data](#q22-nested-source-data) | ❌ | ✅ | ❌ | ❌ |
| Q23 | [Saved queries & refinement](#q23-saved-queries-refinement) | ✅ | ✅ | ❌ | 🟡 |

**Beyond query semantics**

| | Capability | SLayer | Malloy | Cube Core | MetricFlow |
|---|---|---|---|---|---|
| C1 | [Nested / hierarchical results](#c1-nested-hierarchical-results) | ❌ | ✅ | ❌ | ❌ |
| C2 | [Explicit aggregate locality](#c2-explicit-aggregate-locality) | 🟡 | ✅ | 🟡 | 🟡 |
| C3 | [Rendering & tooling](#c3-rendering-tooling) | ❌ | ✅ | 🟡 | 🟡 |
| C4 | [Pre-aggregation & materialization](#c4-pre-aggregation-materialization) | ❌ | 🟡 | ✅ | ❌ |
| C5 | [Parameters & row-level security](#c5-parameters-row-level-security) | ✅ | 🟡 | ✅ | ❌ |
| C6 | [Database coverage](#c6-database-coverage) | ✅ | ✅ | ✅ | ✅ |
| C7 | [Model authoring & reuse](#c7-model-authoring-reuse) | ✅ | ✅ | ✅ | ✅ |
| C8 | [Value index](#c8-value-index) | 🟡 | ✅ | ❌ | 🟡 |
| C9 | [Authoring feedback loop](#c9-authoring-feedback-loop) | 🟡 | ✅ | 🟡 | ✅ |
| C10 | [SQL interop](#c10-sql-interop) | 🟡 | ✅ | ✅ | 🟡 |
| C11 | [Portable expression library](#c11-portable-expression-library) | 🟡 | ✅ | 🟡 | 🟡 |
| C12 | [BI connectivity](#c12-bi-connectivity) | ✅ | ❌ | ✅ | ❌ |
| C13 | [Curated semantic surfaces](#c13-curated-semantic-surfaces) | 🟡 | ✅ | ✅ | 🟡 |
| C14 | [Member-level security & masking](#c14-member-level-security-masking) | ❌ | 🟡 | ✅ | ❌ |
| C15 | [Embedded analytics](#c15-embedded-analytics) | ❌ | 🟡 | 🟡 | ❌ |
| C16 | [Custom calendars & granularities](#c16-custom-calendars-granularities) | ✅ | 🟡 | ✅ | ✅ |
| C17 | [Drill-across fact tables](#c17-drill-across-fact-tables) | ✅ | ✅ | ✅ | ✅ |
| C18 | [Cross-datasource queries](#c18-cross-datasource-queries) | ❌ | 🟡 | 🟡 | ❌ |
| C19 | [Drill-down to records](#c19-drill-down-to-records) | 🟡 | ✅ | ✅ | 🟡 |
| C20 | [Entity-graph join inference](#c20-entity-graph-join-inference) | ✅ | 🟡 | ✅ | ✅ |
| C21 | [Shared time axis across facts](#c21-shared-time-axis-across-facts) | ✅ | 🟡 | 🟡 | ✅ |
| C22 | [Gap filling / time spine](#c22-gap-filling-time-spine) | ✅ | 🟡 | 🟡 | ✅ |
| C23 | [Semi-additive measures](#c23-semi-additive-measures) | ✅ | 🟡 | 🟡 | ✅ |
| C24 | [Point-in-time (SCD Type II) joins](#c24-point-in-time-scd-type-ii-joins) | 🟡 | 🟡 | 🟡 | ✅ |
| C25 | [Open interchange spec](#c25-open-interchange-spec) | 🟡 | ❌ | ❌ | ✅ |

<!-- glance:end -->

## Query semantics

*Grains, transforms, joins, filters and composition.*

### Q1 Coarser grain in the same query

Region totals on rows grouped by region and city. *From SLayer's list.*

- **SLayer** ✅ `sum(revenue, partition_by=region)` — `partition_by=[]` gives the grand total; takes lists, dotted join paths and time buckets.
- **Malloy** ✅ `all(revenue, region)` · `exclude(revenue, city)` — Named dimensions must be output names. `exclude()` can reach outer levels of a `nest:`.
- **Cube Core** 🟡 `SUM(rev) OVER (PARTITION BY region)` · `SELECT ... MEASURE(revenue)` — Ad hoc only via the SQL API: SQL over the semantic result, without its guarantees. A model-declared `grain` measure does it in one query (not counted).
- **MetricFlow** ❌ No query-time partitioned aggregate; a metric filter joins only on the queried model's entities (verified). Mixed-grain metrics are an open request ([#1998](https://github.com/dbt-labs/metricflow/issues/1998)).

Malloy's `all/exclude` and SLayer's `partition_by=` are ad hoc; Cube Core ad hoc needs SQL over the results.

**Probes:** SLayer [Q1a][] [Q1b][] [Q1c][] [Q1d][] [Q1e][] [Q1f][] [S1][] [S3][] · Malloy [Q1a][] [Q1a-exclude][] [Q1a-not-output][] [Q1b][] [Q1c][] [Q1d][] [Q1e][] [Q1f][] · Cube [Q1a][] [Q1b][] [Q1d][] [Q1-cube-keep-only][] · MetricFlow [S1][] [Q1-mf-metric-filter-region][]

### Q2 Arithmetic across grains

Each city's share of its region, and of the grand total. *From SLayer's list.*

- **SLayer** ✅ `sum(revenue) / sum(revenue, partition_by=region)` — Grains union in one expression. A finer grain than the query works when aggregated back up; a bare finer-grain aggregate is rejected with `PartitionKeyError`.
- **Malloy** ✅ `revenue / all(revenue, region)` — Filtered forms: `all(revenue { where: ... })`. Finer grains need a pipeline stage.
- **Cube Core** 🟡 Window ratio over a nested semantic query in the SQL API. Not counted: model-declared multi-stage measures.
- **MetricFlow** ❌ Ratio and derived metrics share one group-by, so share of parent isn't expressible. Open request [#1998](https://github.com/dbt-labs/metricflow/issues/1998).

Same capability for share-of-parent.

**Probes:** SLayer [Q2a][] [Q2a-total][] [Q2b][] [Q2c][] [F4][] [F5][] [F5b][] [F10][] [F11][] [F13][] [X5][] · Malloy [Q2a][] [Q2a-total][] [Q2a-filtered][] [Q2b][] · Cube [Q2a][] [Q2a-total][] [Q2-cube-share-of-region][]

### Q3 Re-aggregation

Per region, the unweighted average of city revenue totals. *From SLayer's list.*

- **SLayer** ✅ `avg(sum(amount, partition_by=[city, region]))`
- **Malloy** 🟡 One stage fails with `aggregate-of-aggregate`.
- **Cube Core** 🟡 `SELECT region, AVG(rev) FROM (SELECT region, city, MEASURE(revenue) rev ...) GROUP BY 1` — Verified. The docs warn that re-aggregating non-additive measures this way is wrong unless the inner grain matches.
- **MetricFlow** ❌ No aggregate of an aggregate, in a query or in a metric definition. Two-step aggregation is an open request ([#653](https://github.com/dbt-labs/metricflow/issues/653)).

One expression in SLayer; a second stage or nested SQL query elsewhere.

**Probes:** SLayer [Q3a][] [F1b][] [F1c][] [F2][] [F2b][] [F3][] [F7][] [F9][] [F9b][] [F9c][] [F9d][] [F9e][] [F9f][] [X6][] · Malloy [Q3a][] [Q3a-one-stage][] · Cube [Q3a][] [X6][] [Q3-cube-grain-include][] [Q3-cube-avg-of-avg][] [Q3-cube-sum-of-distinct][] [Q3-cube-sum-of-avg-measure][] [Q3-cube-number-agg-plain][] [Q3-cube-number-agg-ms-column][] [Q3-cube-number-agg-ms-grain][]

### Q4 Transforms / time shift

"This measure one year earlier" while the date filter is the last 3 months; period-over-period change; running totals; lag. *From SLayer's list.*

- **SLayer** ✅ `time_shift(sum(revenue), -1, 'year')` · `change_pct(...)` · `cumsum(...)` · `lag/lead` — Calendar-aware per group; the date range doesn't clip the lookback (probe-verified). `lag/lead` are row-based by design.
- **Malloy** 🟡 `calculate: prev is lag(revenue)` — `lag` counts rows, and `where:` clips its input. Calendar shift = self-join on a query source.
- **Cube Core** 🟡 REST `compareDateRange` for fixed ranges side by side; SQL `LAG()` over a nested query. Ad hoc shifts are row-based. Calendar-aware `time_shift` and `rolling_window` exist only as model declarations (not counted).
- **MetricFlow** ❌ No query-time shift or lag. Model-declared (not graded): an `offset_window` metric is calendar-aware and not clipped by start/end (verified).

Only SLayer shifts by calendar ad hoc; the others shift by rows or need a self-join.

**Probes:** SLayer [Q4a][] [Q4a2][] [Q4b][] [Q4c][] [Q4d][] [Q4e][] [Q4f][] [Q4f2][] [Q4g][] [Q4h][] [Q4i][] [Q4j][] [Q4k][] [S2][] · Malloy [Q4a][] [Q4e][] [Q4f][] [Q4f-where][] [Q4f-later-stage][] [Q4-filtered-escape][] · Cube [Q4a][] [Q4f][] [Q4-cube-compare-date-range][] [Q4-cube-lag-change][] [Q4-cube-prior-year-only][] [Q4-cube-prior-year-with-revenue][] · MetricFlow [Q4a][] [Q4a2][] [Q4-mf-offset-no-row][] [Q4-mf-change][] [Q4-mf-cum-day][] [Q4-mf-ytd-first][]

### Q5 Calculated dimensions, filters, order

Group revenue by whether each customer's spend is high or low; filter and sort on computed values. *From SLayer's list.*

- **SLayer** ✅ 
- **Malloy** 🟡 Query-derived source + `join_one` + `pick` dimension. `aggregate-in-dimension` in one stage; `order_by:` takes output names only.
- **Cube Core** 🟡 Nested SQL: per-customer `MEASURE(revenue)` inside, `CASE` band and `SUM` outside; REST measure filters become `HAVING` The model-declared `sub_query` path hits a planner bug ([#12020](https://github.com/cube-js/cube/issues/12020)).
- **MetricFlow** 🟡 `--where "{{ Metric('revenue', group_by=['customer']) }} > 300"` — Filters at an entity grain (verified). A band can't be a group-by; metric filters can't use `metric_time` ([#1659](https://github.com/dbt-labs/metricflow/issues/1659)); order only by selected items.

One inline dimension vs a derived source or nested SQL.

**Probes:** SLayer [Q5a][] [Q5a2][] [Q5b][] [Q5d][] [F8][] [X4][] · Malloy [Q5a][] [Q5a2][] [Q5a-in-dimension][] [Q5-having][] [Q5-agg-in-where][] [Q5-calc-in-having][] [Q5-order-by-expr][] · Cube [Q5a][] [Q5-cube-sub-query][] [Q5-cube-sub-query-count][] · MetricFlow [Q5b][] [Q5-having][] [Q5-mf-band-high][]

### Q6 Fields from many models, fan-out safe

Combine fields across joined models without multiplying rows. *From SLayer's list.*

- **SLayer** ✅ `avg(customers.score)` — Each aggregate computed at its own model; unproven to-one dimensions broadcast with a warning.
- **Malloy** ✅ `orders.customers.count()` — Symmetric aggregates. **Bug ([#1762](https://github.com/malloydata/malloy/issues/1762)):** a `join_many` leaf count counts a childless parent as 1.
- **Cube Core** ✅ Declared joins with `relationship: many_to_one` Verified: primary-key dedup and one CTE per cube, even for raw `SUM` measures.
- **MetricFlow** ✅ Entity-based joins, many-to-one and one-to-one only; fan-out paths are never offered (verified). Up to two hops. One dimension gets different names from different facts (`region__name` vs `customer__region__name`).

Four sound designs; the differences are in defaults and in who picks the aggregation point. MetricFlow refuses fan-out paths outright.

**Probes:** SLayer [Q6a][] [Q6a-assoc][] [Q6a-error][] [Q6b][] [Q6c][] [Q6d][] [Q6e][] [Q6f][] [X1][] [X3][] [X7][] [Q9d-assoc][] · Malloy [Q6-locality][] [Q6-multihop][] [Q9d-assoc][] · Cube [Q6a-assoc][] [Q6-cube-raw-sum][] [Q6-cube-raw-sum-legacy][] [Q6-cube-region][] [Q6-cube-region-legacy][] [Q9d-assoc][] · MetricFlow [Q6a][] [Q6b][] [Q6c][] [Q6d][] [Q6e][] [X1][] [X3][] [Q6-mf-two-hop-city][]

### Q7 Deep composition

A mixed-grain expression, then a time shift, then a re-aggregation, all in one expression. *From SLayer's list.*

- **SLayer** ✅ `cumsum(change(sum(revenue)))` · `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -1)` — Uncompilable shapes fail with a typed error before the database (probe-verified).
- **Malloy** 🟡 One stage per layer. `sum(row_number())` reaches the database ([#3105](https://github.com/malloydata/malloy/issues/3105)).
- **Cube Core** 🟡 Stacked nested SQL queries. Each layer is hand-written SQL SQL over the semantic result, without its guarantees.
- **MetricFlow** ❌ Each composition layer is a model-declared metric.

The biggest design difference. Stages and named measures are easier to inspect individually.

**Probes:** SLayer [Q7a][] [Q7b][] [Q7c][] [Q7c2][] [Q7d][] [Q7e][] [Q7f][] [Q7g][] [Q7h][] [Q7i][] [Q9c-1][] [Q9c-2][] [Q9c-3][] · Malloy [Q7-lag-of-calc][] [Q7-lag-of-all][] [Q7-calc-of-agg][] [Q7-analytic-measure][] [Q7-3105-row-number][] [Q7-3105-lag-sum][] [Q7-3105-measure][] [Q7-3105-sole][] [Q7-3105-variants][] [Q7-lag-input][] [Q7-lag-input-stage2][] [Q7-avg-moving-input][] [Q7-lag-output-ref][] [Q7-method-syntax][] · Cube [Q7-cube-cumsum-of-change][] [Q7-cube-rank-of-share][]

### Q8 Population inference

Omit the root model; measures never change which rows come back. *From SLayer's list.*

- **SLayer** ✅ Inferred from dimensions and plain filters only; ties fail with `PopulationInferenceError`.
- **Malloy** ❌ A source is always named; its rows are the result's rows.
- **Cube Core** 🟡 Queries never name a root; Cube picks it from the join graph. The default planner usually roots at the dimension's cube (verified). Pin with the `joinHints` query option.
- **MetricFlow** 🟡 Each metric is rooted at its own model; multi-metric queries full-outer-join the per-metric results (verified). Revenue by region is rooted at orders: no row for a region without orders.

Cube also infers, but heuristically; SLayer's rule is fixed and fails closed on ties.

**Probes:** SLayer [Q8a][] [Q8b][] [Q8c][] · Malloy [Q8-from-orders][] [Q8-from-customers][] · Cube [Q8a][] [Q8-cube-root-tesseract][] [Q8-cube-root-legacy][] [Q8-cube-join-hints][] [Q8-cube-region-root][] [Q8-cube-region-root-legacy][] [Q8-cube-region-legacy][] [Q8-cube-count-legacy][] · MetricFlow [Q8b][] [Q8c][] [Q8-mf-root-orders][] [Q8-mf-union-null][] [Q8-mf-fill-nulls][]

### Q9 No silently wrong numbers

Fail loudly with a reason, or warn about what was approximated. *From SLayer's list.*

- **SLayer** ✅ `"to_many_handling": "broadcast" | "associate" | "error"` — Typed errors and structured warnings; every SLayer probe returns the right numbers or fails with a typed error (verified).
- **Malloy** 🟡 Uncomputable `join_many` aggregates are rejected; an ambiguous `join_one` `sum`/`avg` warns `bad-join-usage`. **Verified gaps:** [#1762](https://github.com/malloydata/malloy/issues/1762), [#3105](https://github.com/malloydata/malloy/issues/3105); docs state the rule backwards ([docs #346](https://github.com/malloydata/malloydata.github.io/issues/346)).
- **Cube Core** 🟡 Strong fan-out dedup; joins without primary keys fail to compile. No warnings channel. **Verified silent cases:** an ignored order-by (Q13, [#282](https://github.com/cube-js/cube/issues/282)), re-truncation accepted (Q18), a second fact grouped by another cube's time dimension (C21); SQL post-processing can be wrong past 50,000 rows (documented).
- **MetricFlow** 🟡 Fan-out is prevented structurally: MetricFlow refuses rather than multiplies. **Verified:** NULL (not 0) counts for childless parents, and the `period_agg` and raw-`where` bugs below.

SLayer has the most explicit machinery (modes, typed errors, warnings) and no open silent case in its probes; Cube and MetricFlow prevent fan-out by construction.

**Probes:** SLayer [Q9a][] [Q9a2][] [Q9b][] [Q9b2][] [Q9b3][] [Q9d][] [F6][] [F6-assoc][] [F6-error][] [B1][] [B1-assoc][] [B1-tier][] [B2][] [B2-coalesce][] · Malloy [Q9a][] [Q9a-count-leaf][] [Q9a-count-leaf-total][] [Q9a-count-filtered][] [Q9a-sum-parent][] [Q9a-sum-per-customer][] [Q9a-nopk-count][] [Q9a-nopk-sum][] [Q9a-traverses][] [Q9a-no-locality][] [Q9-bad-join-usage][] [Q9a2][] · Cube [Q9a][] [Q9b][] [Q9b2][] [B1-tier][] [B1-cube-tier-legacy][] [Q9-cube-no-pk][] · MetricFlow [Q9a][] [Q9a2][] [Q9-mf-raw-where-alias][]

### Q10 Filters across one-to-many joins

Customers with at least one OK order, counted once, including under OR / NOT. *From SLayer's list.*

- **SLayer** ✅ `"filters": ["tier = 'bronze' or orders.status = 'ok'"]` — Pushed down as `EXISTS`; each customer once. `orders.id is null` = "no orders".
- **Malloy** ✅ `where: orders.status = 'ok'` — Each customer once. No `exists`; the `count() = 0` anti-join is broken by [#1762](https://github.com/malloydata/malloy/issues/1762).
- **Cube Core** 🟡 Verified: `customers.count` filtered on `orders.status` counts each customer once; OR across cubes works row by row. "Has no orders" (`notSet`) works only when customers is the chosen root.
- **MetricFlow** 🟡 To-many filters via `{{ Metric(...) }}` at an entity: at least one order counted once, OR across models, "has no orders" via `IS NULL` (verified). `Dimension('order__status')` in a customers query is refused; "at least one OK order" needs a filtered metric in the model.

For a plain condition, SLayer, Malloy and Cube agree. `NOT` is "has none" in none of them; Malloy and Cube count a customer with no orders as matching a negated condition, SLayer (SQL NULL logic) doesn't.

**Probes:** SLayer [Q10a][] [Q10b][] [Q10c][] [Q10d][] [Q10e][] [Q10f][] [Q10g][] [Q10h][] [X2][] · Malloy [Q10a][] [Q10a-restricts-orders][] [Q10b][] [Q10c][] [Q10d-malloy][] [Q10e][] [Q10-exists-having][] [Q10-antijoin-count][] · Cube [Q10a][] [Q10b][] [Q10c][] [Q10h][] [Q10-cube-not-equals-null][] [Q10-cube-not-set][] [Q10-cube-not-set-measure-only][] [Q10-cube-not-set-measure-only-legacy][] [Q10-cube-not-equals-measure-only-legacy][] · MetricFlow [Q10a][] [Q10b][] [Q10e][] [Q10-mf-any-order][] [Q10-mf-gold-or-any-order][] [Q10-mf-rowlevel-or][] [Q10-mf-antijoin-eq0][] [X2-mf-any][]

### Q11 Rolling time windows

Trailing 90 days of any aggregate, including count distinct. *From SLayer's list.*

- **SLayer** ✅ `count_distinct(customer_id, window='90d')` — Time-based, every aggregation; date bounds don't clip the lookback; composes with arithmetic and transforms.
- **Malloy** 🟡 `avg_moving(revenue, 2)` — Row frames only; no rolling count distinct.
- **Cube Core** 🟡 SQL window frames over a daily nested query. Row frames only ad hoc; no rolling count distinct. Time-based `rolling_window` is a model declaration (not counted).
- **MetricFlow** ❌ No query-time windows. Model-declared `window: 90 days` is exact by day, incl. count distinct; at month grain it hits the `period_agg` bug.

Only SLayer has time-based windows ad hoc; the others count rows.

**Probes:** SLayer [Q11a][] [Q11-count_distinct][] [Q11-min][] [Q11-max][] [Q11-count][] [Q11-avg][] [Q11-last][] [Q11-first][] [Q11-range][] [Q11-range-cd][] [Q11-status][] [W1][] [W3][] · Malloy [Q11-moving][] [Q11-distinct-sum][] · Cube [Q11-range][] [Q11-range-cd][] [Q11-cube-rows-frame][] [Q11-cube-range-frame][] [Q11-cube-rolling-no-range][] · MetricFlow [Q11-mf-90d-day][] [Q11-mf-90d-cd-day][] [Q11-mf-90d-last][]

### Q12 Aggregates as arguments

Aggregate an expression spanning models; weight by a coarser-grain aggregate. *From SLayer's list.*

- **SLayer** ✅ `sum(amount - customers.discount)` · `weighted_avg(amount, weight=sum(amount, partition_by=region))`
- **Malloy** 🟡 `source.sum(amount * (1 - customers.discount))` — No aggregate inside an aggregate.
- **Cube Core** 🟡 Pushdown expressions over cube members; a weight from a nested aggregate in SQL. Cross-cube expression measures exist only in the model (not counted).
- **MetricFlow** ❌ A measure's `expr` sees only its own model's columns; no aggregate as a weight.

The cross-model expression works everywhere; the aggregate-as-parameter is ad hoc only in SLayer.

**Probes:** SLayer [Q12a][] [Q12b][] [Q12c][] [Q12d][] · Malloy [Q12a][] [Q12-agg-in-agg][] · Cube [Q12a][]

### Q13 Order by what you don't show

Top 10 statuses by revenue, displaying only the count. *From SLayer's list.*

- **SLayer** ✅ `"order": [{"column": "sum(amount)", "direction": "desc"}]`
- **Malloy** 🟡 A second stage; `order-by-not-found-in-output` otherwise.
- **Cube Core** 🟡 Verified: ordering by an unselected measure works. **Silently ignored:** an unselected dimension in `order` is left out of the SQL without an error (see [#282](https://github.com/cube-js/cube/issues/282)).
- **MetricFlow** ❌ Order only by selected metrics and group-by items (verified).

A common top-N pattern.

**Probes:** SLayer [Q13a][] [Q13a-order][] [Q13b][] [Q13c][] · Malloy [Q13a-order][] [Q13-not-in-output][] [Q13-dotted][] [Q13-calc][] · Cube [Q13-cube-order-measure][] [Q13-cube-order-dimension][] · MetricFlow [Q13a][] [Q13-mf-order-unselected-dim][]

### Q14 Ranking and streaks

Filter on rank in the same query; "3+ consecutive positive months". *From SLayer's list.*

- **SLayer** ✅ `rank(sum(revenue)) 0) >= 3` — Streaks count calendar periods, so an empty month breaks them (verified).
- **Malloy** 🟡 `nest: top is { ...; limit: 1 }` — Rank filters need a second stage; streaks 2+.
- **Cube Core** 🟡 `RANK() OVER (...)` — Verified; needed a `CAST` around the rank. Streaks by hand in SQL.
- **MetricFlow** ❌ Global order plus limit only.

For top-N per group, Malloy's `nest:` is a fair match.

**Probes:** SLayer [Q14a][] [Q14b][] [W2][] [Q14c][] [Q14d][] · Malloy [Q14a][] [Q14a-nest][] [Q14c][] · Cube [Q14-cube-rank][] [Q14-cube-rank-uncast][] [Q14-cube-rank-measure][] [Q14-cube-rank-measure-filtered][]

### Q15 Multi-stage queries

A query feeding another; save a query as a reusable model; re-bucket dates downstream. *From SLayer's list.*

- **SLayer** ✅ Named stages form a dependency graph; save as a query-backed model; re-bucket at equal or coarser grain.
- **Malloy** ✅ `query: by_day is orders -> {...}` — Each `->` a CTE; views refine with `+`.
- **Cube Core** 🟡 An outer SQL query over an inner semantic query (verified) Saving a result as a model means changing the model.
- **MetricFlow** ❌ Saved queries can't be used as sources. Exports need the dbt platform.

Malloy's is the richest.

**Probes:** SLayer [Q15a][] [Q15b][] [Q15c][] [Q15d][] [Q15e][] [Q15f][] [Q15g][] [Q15h][] · Malloy [Q15a][] [Q15h][] · Cube [Q15a][]

### Q16 Inline model extension

Add a column, measure or join for one query without editing the model. *From SLayer's list.*

- **SLayer** ✅ `"source_model": {"source_name": "orders", "columns": [...], "joins": [...]}`
- **Malloy** ✅ `extend: { join_one: ...; measure: ... }`
- **Cube Core** 🟡 SQL API pushdown expressions over dimensions and aggregations (verified) No ad-hoc members over REST; no ad-hoc joins.
- **MetricFlow** ❌ No expressions in group-by; raw SQL only inside a `where`, around a Jinja reference (verified).

Cube's model-first design shows here.

**Probes:** SLayer [Q16a][] [Q16b][] [Q16c][] · Malloy [Q16a][] · Cube [Q16a][] [Q16-cube-dow][] [Q16-cube-dow-direct][] · MetricFlow [Q16-mf-filter-expression][] [Q16-mf-groupby-expression][]

### Q17 Saved measures that compose

Reuse a named formula in transforms and arithmetic; save transforms as measures. *From SLayer's list.*

- **SLayer** ✅ `cumsum(aov)` · `aov_pct_change: change_pct(aov)`
- **Malloy** 🟡 `calculate: prev_aov is lag(aov)` — A window expression can't be saved as a measure.
- **Cube Core** 🟡 Existing measures combine in SQL arithmetic (`MEASURE(a) / MEASURE(b)`) Saving a period-over-period formula needs a model change.
- **MetricFlow** 🟡 Queries use named metrics. Ratio, derived and offset composition must be declared in the model.

Only SLayer saves a transform as a reusable measure without writing model code.

**Probes:** SLayer [Q17a][] [Q17b][] [Q17c][] [Q17d][] [Q17e][] · Malloy [Q17a][] [Q17d][] [Q17-lag-aov][] · Cube [Q17a][] [Q17d][] [Q17-lag-aov][] · MetricFlow [Q17a][]

### Q18 Time-bucket safety

Refuse a day breakdown of a column already truncated to month. *From SLayer's list.*

- **SLayer** ✅ Typed error; `Column.granularity` set on query-backed models (probe-verified).
- **Malloy** ❌ `m.day` returns month starts labelled as days.
- **Cube Core** ❌ Verified: a `day` query on a month-truncated dimension returns first-of-month "days".
- **MetricFlow** ✅ A month-grain dimension refuses a day or week breakdown and a day-grain filter (verified).

SLayer and MetricFlow refuse; the others return mislabelled rows.

**Probes:** SLayer [Q18-create][] [Q18a][] [Q18b][] [Q18c][] [Q18d][] [Q18e][] · Malloy [Q18-malloy-day][] [Q18-malloy-inline][] [Q18-malloy-week][] [Q18-malloy-filter][] [Q18-malloy-year][] · Cube [Q18-cube-day][] · MetricFlow [Q18a][] [Q18b][] [Q18c][] [Q18d][] [Q18-mf-filter-day-grain][]

### Q19 Agent & API surface

How an agent actually issues queries. *From SLayer's list.*

- **SLayer** ✅ MCP with help, search and memories; REST; CLI; Python. Queries are schema-validated JSON.
- **Malloy** ✅ Publisher MCP and REST; `loadRestrictedQuery()` sandboxes LLM-written queries.
- **Cube Core** ✅ REST (JSON), GraphQL and a Postgres-wire SQL API. No MCP server in Cube Core (it's a Cloud feature).
- **MetricFlow** 🟡 `mf` · `MetricFlowEngine` — No MCP server or network API in open source; those are dbt platform features.

JSON (SLayer, Cube), a text language (Malloy), or CLI and Python arguments (MetricFlow).

**Probes:** Cube [Q19-cube-rest][] [Q19-cube-sql][] [Q19-cube-rest-legacy][] [Q19-cube-sql-orders-first][]

### Q20 Relative & typed filters

"Last 3 days", "this quarter", value lists and ranges without hand-computing bounds. *From Malloy's list.*

- **SLayer** ✅ `"date_range": "last 3 months"` · `order_date in 'this year'` · `order_date = '2025-Q1'` — Relative tokens, periods (day to year, ISO week) and open-ended ranges, verified with a pinned clock; a period bound covers the whole period.
- **Malloy** ✅ `ts ~ f'last 3 days'` · `dep_time ? @2003`
- **Cube Core** ✅ `"dateRange": "last 3 months"` — Verified, incl. `"from 500 days ago to now"`; typed operators like `inDateRange`, `notSet`.
- **MetricFlow** 🟡 Absolute start/end, widened to the query grain (verified). Relative ranges need raw SQL date math in a `where`.

All but MetricFlow take relative ranges in the query.

**Probes:** SLayer [Q20-slayer-last-n][] [Q20-slayer-ago][] [Q20-slayer-this-year][] [Q20-slayer-quarter-literal][] [Q20-slayer-period-upper][] [Q20-slayer-open-ended][] · Cube [Q20-cube-days-ago][] [Q20-cube-last-years][] · MetricFlow [Q20-mf-sql-date-math][] [Q20-mf-start-snaps][]

### Q21 Calendar expressions

Day-of-week, date differences, date arithmetic and time zones as portable expressions. *From Malloy's list.*

- **SLayer** 🟡 `date_part('day_of_week', t)` · `date_diff` · `date_add` — Portable across engines (verified). No time zones.
- **Malloy** ✅ `day_of_week(t)` · `days(a to b)` · `t + 3 days`
- **Cube Core** 🟡 Query `timezone`; `EXTRACT(DOW ...)` in the SQL API over a nested query (verified) No day-of-week granularity; date math in raw SQL. `timezone` moves `DATE` values back a day in negative-offset zones ([#10166](https://github.com/cube-js/cube/issues/10166)).
- **MetricFlow** 🟡 `TimeDimension('metric_time', date_part_name='dow')` — Verified. Date math only as raw SQL in filters; time zones not documented.

Malloy's portable date algebra is richest.

**Probes:** SLayer [Q21-slayer-day-of-week][] [Q21-slayer-date-diff][] [Q21-slayer-date-add][] [Q21-slayer-interval][] · Cube [Q21-cube-timezone][] [Q21-cube-day-of-week][] · MetricFlow [Q21-mf-dow][] [Q21-mf-dow-dunder][]

### Q22 Nested source data

Query arrays and repeated records with fan-out-safe aggregation. *From Malloy's list.*

- **SLayer** ❌ Array and struct columns are opaque.
- **Malloy** ✅ `tags.each.sum()`
- **Cube Core** ❌ No nested types in queries. Arrays must be unnested in the model.
- **MetricFlow** ❌ Not documented.

A headline Malloy feature.

**Probes:** none; graded from documentation.

### Q23 Saved queries & refinement

Reuse a saved query and extend the query itself with dimensions, measures or filters, not only post-process its output. *From Malloy's list.*

- **SLayer** ✅ `query("monthly_rev", refine={"dimensions": ["customers.tier"]})` — Dimensions, measures, filters, order and limit merge into the saved query's final stage; a conflicting redefinition is a typed error (verified).
- **Malloy** ✅ `flights -> by_carrier + { where: ... }`
- **Cube Core** ❌ No saved query shapes in Core. Views curate members; they aren't query shapes.
- **MetricFlow** 🟡 A saved query takes an extra `where`, order and limit (verified). Not extra metrics or group-bys.

Malloy and SLayer merge any clause into a saved query; MetricFlow's saved queries take a few extra clauses.

**Probes:** SLayer [Q23-slayer-refine-groupby][] [Q23-slayer-refine-where][] [Q23-slayer-refine-measure][] [Q23-slayer-refine-top][] [Q23-slayer-refine-final-stage][] [Q23-slayer-refine-aggregated-away][] [Q23-slayer-refine-conflict][] · MetricFlow [Q23-mf-saved-where][] [Q23-mf-saved-groupby][]

## Beyond query semantics

*Results, tooling, security, serving and platform.*

### C1 Nested / hierarchical results

Per region, a sub-table of top customers, in one result. *From Malloy's list.*

- **SLayer** ❌ Flat results.
- **Malloy** ✅ `nest: by_customer is { ...; limit: 5 }`
- **Cube Core** ❌ Flat results, GraphQL included.
- **MetricFlow** ❌ Flat results.

Malloy's flagship feature.

**Probes:** Malloy [C1][]

### C2 Explicit aggregate locality

From orders, average a customer field per customer vs per order. *From Malloy's list.*

- **SLayer** 🟡 No per-aggregate switch; the reading follows where the value lives (all verified): `avg(customers.credit)` is per customer, all customers; add an orders filter for customers with orders; an orders-side column for per order.
- **Malloy** ✅ `customers.credit.avg()` · `source.avg(customers.credit)` — The per-customer form covers only customers reached through orders.
- **Cube Core** 🟡 Measures aggregate at their own cube by default (per entity) A per-order reading of a customer field needs SQL over joined members or a model change.
- **MetricFlow** 🟡 A measure lives in its own model, so an average of a customer field is per customer by construction. A per-order weighting needs a joining dbt model.

From orders, SLayer's plain cross-model average includes customers with no orders; Malloy's includes only those reached through the join.

**Probes:** none; graded from documentation.

### C3 Rendering & tooling

Charts, dashboards, notebooks, drill-down, delivery. *From Malloy's list.*

- **SLayer** ❌ Headless; returns data.
- **Malloy** ✅ Rendering tags, notebooks, VS Code, Explorer.
- **Cube Core** 🟡 Playground only. Workbooks and dashboards are Cloud features.
- **MetricFlow** 🟡 CLI tables, CSV and a plan SVG; no charts.

A product-focus difference.

**Probes:** none; graded from documentation.

### C4 Pre-aggregation & materialization

Rollup tables chosen automatically; caching and refresh. *From Malloy's list.*

- **SLayer** ❌ An opt-in in-memory result cache only.
- **Malloy** 🟡 Composite sources, `#@ persist` (experimental).
- **Cube Core** ✅ Rollups, refresh keys, partitions, Cube Store.
- **MetricFlow** ❌ Not in open source.

Cube's defining feature.

**Probes:** none; graded from documentation.

### C5 Parameters & row-level security

Runtime parameters; scope every query of a session to one tenant. *From all lists.*

- **SLayer** ✅ Engine policy the agent can't override; unlisted tables fail closed (probe-verified). `{var}` parameters.
- **Malloy** 🟡 Source parameters, givens (experimental).
- **Cube Core** ✅ Access policies with `row_level`; security context; per-tenant data sources. `userAttributes` is Cloud-only; Core uses the security context.
- **MetricFlow** ❌ No parameters or row-level security in open source.

Matters most for agent and embedded deployments.

**Probes:** SLayer [C5a][] [C5b][] [C5c][] [C5d][] [C5e][] [C5f][]

### C6 Database coverage

Where it runs. *From all lists.*

- **SLayer** ✅ 8 engines tested live, 4 more by SQL generation.
- **Malloy** ✅ 8 engines plus files via DuckDB.
- **Cube Core** ✅ 26 sources, incl. streaming (ksqlDB, Materialize) and MongoDB.
- **MetricFlow** ✅ Nine SQL renderers: BigQuery, DuckDB, Redshift, Postgres, Snowflake, Databricks, Trino, Vertica, Athena.

Cube's list is the longest.

**Probes:** none; graded from documentation.

### C7 Model authoring & reuse

How models get written, shared and kept in sync. *From Malloy's list.*

- **SLayer** ✅ YAML/JSON; auto-ingest with FK joins; dbt, Cube and OSI importers; drift detection.
- **Malloy** ✅ A typed language with `import` and `extend`.
- **Cube Core** ✅ YAML/JS with Jinja/Python generation, `extends`, `cube_dbt`
- **MetricFlow** ✅ dbt YAML semantic models next to the dbt models they read.

Models as data (SLayer), as code (Malloy), generated (Cube), or beside the dbt models they read (MetricFlow).

**Probes:** none; graded from documentation.

### C8 Value index

Find which field holds "SANTA CRUZ", for filter suggestions and LLM grounding. *From Malloy's list.*

- **SLayer** 🟡 Sampled top values per column feed search.
- **Malloy** ✅ `index: * by total_ratings`
- **Cube Core** ❌ No documented value index in Core.
- **MetricFlow** 🟡 `mf list dimension-values`

Relevant to agent accuracy on filter values.

**Probes:** none; graded from documentation.

### C9 Authoring feedback loop

Catch bad references and types before running. *From Malloy's list.*

- **SLayer** 🟡 Save-time validation, `dry_run`, typed errors; no IDE support.
- **Malloy** ✅ Language server; `malloy_compile` over MCP.
- **Cube Core** 🟡 Compile errors on reload (verified) No language-server support documented.
- **MetricFlow** ✅ `dbt parse` · `mf validate-configs` · `--explain`

SLayer has no editor tooling.

**Probes:** none; graded from documentation.

### C10 SQL interop

Native SQL anywhere, and semantic queries inside SQL. *From Malloy's list.*

- **SLayer** 🟡 SQL models and columns; a Postgres-wire facade accepting a SQL subset. No full SQL over semantic queries.
- **Malloy** ✅ `conn.sql("""... %{ malloy_query } ...""")`
- **Cube Core** ✅ SQL API with `MEASURE()`, pushdown and post-processing (verified)
- **MetricFlow** 🟡 Raw SQL in expressions and filters. Semantic queries inside SQL need the dbt platform (JDBC).

Malloy and Cube go both ways.

**Probes:** none; graded from documentation.

### C11 Portable expression library

The same function means the same thing on every engine. *From Malloy's list.*

- **SLayer** 🟡 Portable scalar functions, including date parts, differences and arithmetic; no regex.
- **Malloy** ✅ Broad standard library plus a native escape.
- **Cube Core** 🟡 Model SQL is warehouse-specific; only SQL API functions are transpiled.
- **MetricFlow** 🟡 MetricFlow renders its own time, window and offset constructs per dialect. User expressions are raw SQL.

Overlaps with Q21.

**Probes:** none; graded from documentation.

### C12 BI connectivity

Tableau, Metabase, Excel or Power BI query the semantic layer directly. *From Cube's list.*

- **SLayer** ✅ Postgres-wire SQL API and Arrow Flight SQL. Read-only, a SQL subset; no MDX/DAX.
- **Malloy** ❌ REST, React SDK and MCP only.
- **Cube Core** ✅ Postgres-wire SQL API (verified) MDX and DAX APIs are Enterprise-only.
- **MetricFlow** ❌ A dbt platform feature.

Both speak the Postgres wire protocol; SLayer adds Arrow Flight SQL.

**Probes:** none; graded from documentation.

### C13 Curated semantic surfaces

Expose a vetted subset of members with pinned join paths. *From Cube's list.*

- **SLayer** 🟡 Facade models with named join edges.
- **Malloy** ✅ `source: s is x extend { accept: ... }`
- **Cube Core** ✅ Views with `join_path`, `includes`, folders.
- **MetricFlow** 🟡 Saved queries.

Also Cube's answer to join-path ambiguity (Q8).

**Probes:** none; graded from documentation.

### C14 Member-level security & masking

Hide or mask specific fields per user. *From Cube's list.*

- **SLayer** ❌ Row-level only.
- **Malloy** 🟡 `#(authorize)` on whole sources; no masking.
- **Cube Core** ✅ `member_level` · `member_masking`
- **MetricFlow** ❌ Not in open source.

Enterprise-governance territory.

**Probes:** none; graded from documentation.

### C15 Embedded analytics

Serve the layer's results inside your own product. *From Cube's list.*

- **SLayer** ❌ APIs only; embedding is the host app's job.
- **Malloy** 🟡 Publisher embed / React SDK; token check unfinished per its docs.
- **Cube Core** 🟡 REST/GraphQL plus the open-source JS client libraries. iframe embedding with sessions is a Cloud feature.
- **MetricFlow** ❌ Not in open source.

Cube Core ships client libraries; hosted embedding is a Cloud feature.

**Probes:** none; graded from documentation.

### C16 Custom calendars & granularities

Fiscal years, retail 4-5-4, 15-minute buckets. *From Cube's list.*

- **SLayer** ✅ `{"name": "fiscal_year", "base": "month", "multiple": 12, "origin": "2024-04-01"}` — Datasource granularities work as time dimensions, `time_shift` units and spine buckets: fiscal years and 15-minute buckets verified. No retail 4-5-4 calendars.
- **Malloy** 🟡 Hand-written dimensions.
- **Cube Core** ✅ `granularities`
- **MetricFlow** ✅ Custom granularities from the time spine, e.g. an April fiscal year (verified).

Interval-based grains in SLayer and Cube; MetricFlow's spine columns can hold any calendar.

**Probes:** SLayer [C16-slayer-fiscal-year][] [C16-slayer-fiscal-year-shift][] [C16-slayer-fiscal-year-spine][] [C16-slayer-quarter-hour][] [C16-slayer-quarter-hour-spine][] · MetricFlow [C16-mf-fiscal-year][]

### C17 Drill-across fact tables

Combine unrelated facts (orders, returns) over shared dimensions. *From Cube's list.*

- **SLayer** ✅ `"measures": ["sum(orders.amount)", "sum(returns.amount)"]` · `customers`
- **Malloy** ✅ A hub source with two `join_many`s.
- **Cube Core** ✅ Multi-fact queries and views, one CTE per fact (verified)
- **MetricFlow** ✅ Metrics from two fact models on one axis (verified). Refused when the dimension's entity path differs per fact.

Fan-out safety makes this work in all four.

**Probes:** MetricFlow [C17-mf-drill-across-tier][]

### C18 Cross-datasource queries

Join models living in different databases. *From Cube's list.*

- **SLayer** ❌ Refused explicitly.
- **Malloy** 🟡 Via a DuckDB connection that attaches other databases.
- **Cube Core** 🟡 `rollup_join`
- **MetricFlow** ❌ One warehouse connection.

Nobody does this natively.

**Probes:** none; graded from documentation.

### C19 Drill-down to records

From an aggregate, fetch the underlying rows. *From Cube's list.*

- **SLayer** 🟡 Raw-rows query mode; no declared drill members.
- **Malloy** ✅ Publisher click-through; `select: *`.
- **Cube Core** ✅ `drill_members` · `ungrouped: true`
- **MetricFlow** 🟡 Dimension-only queries list rows of declared dimensions (verified).

Mostly a UI concern.

**Probes:** MetricFlow [C19-mf-dimension-only][]

### C20 Entity-graph join inference

Joins found automatically from declared keys, including multi-hop. *From MetricFlow's list.*

- **SLayer** ✅ Join edges traversed both ways, with dotted multi-hop paths inferred.
- **Malloy** 🟡 Joins declared per source; dotted paths follow them.
- **Cube Core** ✅ Path-finding over the join graph, plus `joinHints`.
- **MetricFlow** ✅ An entity graph, up to two hops.

Similar power; the differences are direction and hop limits.

**Probes:** none; graded from documentation.

### C21 Shared time axis across facts

One time dimension for metrics from different fact tables. *From MetricFlow's list.*

- **SLayer** ✅ `"time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "month", "date_range": [...]}]` — A built-in spine each model joins through its own date column; per group too; two equally short routes fail loudly (verified).
- **Malloy** 🟡 A hand-made calendar source joined to each fact; symmetric aggregates keep it exact (verified).
- **Cube Core** 🟡 Needs a model-declared dates cube. Query-only, a second fact lands in the months of the time dimension's own cube: returns counted in their customers' order months (verified).
- **MetricFlow** ✅ `metric_time` — Months with either fact; empty months only with a model change (verified).

SLayer and MetricFlow give every fact one time axis; elsewhere it is a hand-made calendar.

**Probes:** SLayer [C21-axis-two-facts][] [C21-axis-two-facts-region][] [C21-axis-ambiguous-route][] [C21-calendar-model-parity][] · Malloy [C21-malloy-calendar][] · Cube [C21-cube-two-facts][] · MetricFlow [C21-mf-two-facts][]

### C22 Gap filling / time spine

Rows for empty periods, zero-filled. *From MetricFlow's list.*

- **SLayer** ✅ `coalesce(sum(orders.amount), 0)` — Every bucket in range, per group too; change, cumsum, lag, streaks and windows see the empty months (verified). A lower bound is required.
- **Malloy** 🟡 A manual calendar join.
- **Cube Core** 🟡 Client-side `fillMissingDates` only.
- **MetricFlow** ✅ `join_to_timespine` · `fill_nulls_with` — Declared on the metric; a query's start/end alone doesn't fill (verified).

SLayer fills in any query; MetricFlow per declared metric.

**Probes:** SLayer [C22-spine-fill-month][] [C22-spine-fill-coalesce][] [C22-spine-no-lower-bound][] [C22-spine-per-group][] [C22-spine-fact-filter][] [C22-spine-change][] [C22-spine-cumsum][] [C22-spine-consecutive][] [C22-spine-window][] [C22-spine-lag][] [C22-spine-stage][] · MetricFlow [C22-mf-no-fill][] [C22-mf-join-to-timespine][]

### C23 Semi-additive measures

Balances: sum across accounts, last value over time. *From MetricFlow's list.*

- **SLayer** ✅ `sum(last(balance, snapshot_date, partition_by=[account_id, customer_id]))` — One expression per query: the latest balance per account, then summed.
- **Malloy** 🟡 A pipeline.
- **Cube Core** 🟡 No recipe.
- **MetricFlow** ✅ `non_additive_dimension`

Declared once in MetricFlow; one query expression in SLayer; a pipeline elsewhere.

**Probes:** none; graded from documentation.

### C24 Point-in-time (SCD Type II) joins

Join a fact to the dimension row valid at that time. *From MetricFlow's list.*

- **SLayer** 🟡 A custom SQL join.
- **Malloy** 🟡 A range join in SQL.
- **Cube Core** 🟡 Custom SQL.
- **MetricFlow** ✅ `validity_params`

A MetricFlow specialty.

**Probes:** none; graded from documentation.

### C25 Open interchange spec

Import and export semantic definitions in a shared format. *From MetricFlow's list.*

- **SLayer** 🟡 Imports OSI, dbt and Cube; no export.
- **Malloy** ❌ None.
- **Cube Core** ❌ None.
- **MetricFlow** ✅ Writes an Open Semantic Interchange document (verified); cumulative and conversion metrics can't be represented.

OSI has been renamed Apache Ossie.

**Probes:** none; graded from documentation.

## Bugs and silent cases found

Each entry links the upstream report, where there is one, and the probes that reproduce it.

**SLayer.** None open. The suite's NULL child counts, row-based `consecutive_periods` and the internal error on a
one-expression semi-additive measure are fixed. By design, a child row with no parent lands in the group whose
dimension is NULL, since a cross-model aggregate is a field of a model keyed on the query grain ([Q8c][],
[B1][], [C22-spine-per-group][]).

**Malloy 0.0.434**

- `join.count()` over a `join_many` counts an unmatched parent as 1, and `join.sum()` / `join.avg()` of a parent
  field include it ([malloy#1762](https://github.com/malloydata/malloy/issues/1762); [Q9a-count-leaf][],
  [Q9a-count-leaf-total][], [Q9a-sum-per-customer][], [Q9a-nopk-sum][], [Q10-antijoin-count][]).
- An analytic function inside an aggregate, such as `sum(row_number())`, reaches the database or crashes the
  compiler ([malloy#3105](https://github.com/malloydata/malloy/issues/3105); [Q7-3105-row-number][],
  [Q7-3105-lag-sum][], [Q7-3105-measure][], [Q7-3105-sole][], [Q7-3105-variants][]).
- The aggregates docs describe the illegal "forward" and "backward" cases the wrong way round
  ([malloydata.github.io#346](https://github.com/malloydata/malloydata.github.io/issues/346); [Q9a-sum-parent][],
  [Q9a-traverses][]).

**Cube Core 1.7.46**

- A `sub_query` dimension queried with its source measure generates invalid SQL under the default planner
  ([cube#12020](https://github.com/cube-js/cube/issues/12020); [Q5-cube-sub-query][]).
- Ordering by an unselected dimension is dropped from the SQL without an error. A 2019 maintainer reply explains why
  it can't be honoured under `GROUP BY` ([cube#282](https://github.com/cube-js/cube/issues/282)), but not why it isn't
  rejected ([Q13-cube-order-dimension][], [Q13-cube-order-measure][]).
- `type: number_agg` fails schema validation unless `multi_stage: true` is set, although the docs list it as a
  regular type ([cube#12024](https://github.com/cube-js/cube/issues/12024); [Q3-cube-number-agg-plain][]); a related
  `number_agg` SQL error still reproduces ([cube#10799](https://github.com/cube-js/cube/issues/10799)).
- The deprecated legacy planner silently ignores a multi-stage `grain` and drops rows from `time_shift` queries. The
  report was closed as not planned because the legacy planner is to be removed in the next minor release, so grades
  use the default planner ([cube#12025](https://github.com/cube-js/cube/issues/12025); [Q1-cube-keep-only][],
  [Q2-cube-share-of-region][], [Q3-cube-grain-include][], [Q4-cube-prior-year-with-revenue][]).
- The `timezone` query option treats `DATE` values as UTC midnight, so a negative-offset zone moves every date back
  a day ([cube#10166](https://github.com/cube-js/cube/issues/10166), repro added; [Q21-cube-timezone][]).
- A finer time granularity over an already-truncated dimension is accepted without warning ([Q18-cube-day][]).
- By design, measures from two facts under one time dimension are grouped by that dimension's own cube, so returns
  land in their customers' order months without a warning ([C21-cube-two-facts][]).

**MetricFlow 0.213.0**

- Cumulative metrics at a coarser grain: `period_agg: first` / `last` take the first or last day that has a value,
  not the period's first or last day, so a month whose last-day window is empty still reports a value
  ([Q11-mf-90d-last][], [Q4-mf-ytd-first][]).
- A `where` without Jinja (`revenue > 100`) binds to the row-level measure column and filters individual orders
  before aggregation ([Q9-mf-raw-where-alias][]).
- By design, count metrics are NULL, not 0, for childless parents, so a metric filter `= 0` matches nothing
  ([Q10-mf-antijoin-eq0][], [Q9a][]).

## How this was checked

- **Dataset.** One DuckDB dataset with deliberate edge cases: a customer with no orders, a NULL region, a city in two
  regions, month gaps, an orphan order, multi-hop joins, a second fact with a returns-only month, and sub-hour
  timestamps. Every probe compares an engine's answer with hand-written SQL, and the suite reports each known bug as
  still present or fixed. To rerun it, see the
  [suite README](https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/README.md).
- **SLayer and Malloy.** SLayer from this repository, Malloy 0.0.434 (the latest published release).
- **Cube.** Cube Core 1.7.46 against DuckDB, over REST and the Postgres-wire SQL API, graded on the default
  (Tesseract) planner; the deprecated legacy planner was also run and its differences are noted. Only Core features
  count, and query-semantics rows count only what a query can do against an existing model: model-declared
  multi-stage measures are noted but not graded.
- **MetricFlow.** Open-source `metricflow` 0.213.0 with `dbt-core` 1.12.5 and `dbt-duckdb`, through the Python
  `MetricFlowEngine`. The baseline model has one simple metric per measure; derived, cumulative and offset metrics
  are model-declared (noted, not graded). dbt platform features are out of scope.
- **Syntax in the examples.** SLayer: JSON and formula strings. Malloy: Malloy source. Cube: YAML model plus JSON REST
  query. MetricFlow: `mf query` arguments and Jinja filters.

## Sources

- **SLayer:** [Queries](../concepts/queries.md) · [Formulas](../concepts/formulas.md) ·
  [Time](../concepts/time.md) · [Models](../concepts/models.md) ·
  [Row-level security](../concepts/row-level-security.md) · [SQL API](../interfaces/pg-facade.md)
- **Malloy:** [Ungrouped aggregates](https://docs.malloydata.dev/documentation/language/ungrouped-aggregates) ·
  [Aggregates and locality](https://docs.malloydata.dev/documentation/language/aggregates) ·
  [Calculations and windows](https://docs.malloydata.dev/documentation/language/calculations_windows) ·
  [Filter expressions](https://docs.malloydata.dev/documentation/language/filter-expressions) ·
  [Views](https://docs.malloydata.dev/documentation/language/views) ·
  [Dimensional index](https://docs.malloydata.dev/documentation/patterns/dim_index) ·
  [Publisher MCP](https://docs.malloydata.dev/documentation/user_guides/publishing/mcp_agents) ·
  [malloydata/malloy](https://github.com/malloydata/malloy)
- **Cube:** [Measures](https://docs.cube.dev/reference/data-modeling/measures) ·
  [Joins](https://docs.cube.dev/docs/data-modeling/joins) · [Views](https://docs.cube.dev/reference/data-modeling/view) ·
  [Pre-aggregations](https://docs.cube.dev/reference/data-modeling/pre-aggregations) ·
  [Access policies](https://docs.cube.dev/reference/data-modeling/data-access-policies) ·
  [SQL API](https://docs.cube.dev/reference/core-data-apis/sql-api/query-format) ·
  [v1.7.46](https://github.com/cube-js/cube/releases/tag/v1.7.46)
- **MetricFlow:** [About MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow) ·
  [Join logic](https://docs.getdbt.com/docs/build/join-logic) ·
  [Cumulative metrics](https://docs.getdbt.com/docs/build/cumulative) ·
  [Commands](https://docs.getdbt.com/docs/build/metricflow-commands) ·
  [Saved queries](https://docs.getdbt.com/docs/build/saved-queries) ·
  [dbt-labs/metricflow](https://github.com/dbt-labs/metricflow)
- **Secondary:** [Credible Data: Why Malloy](https://www.credibledata.com/docs/concepts/why-malloy) ·
  [Strata: Full review of Cube](https://blog.strata.do/p/full-review-of-cube). Neither contradicts the official
  documentation or code.

<!-- Probe links below are generated by examples/comparisons/update_comparison_doc.py; edit rows, then rerun it. -->
<!-- probe-links:start -->
[Q1a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L34
[Q1a-exclude]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L50
[Q1a-not-output]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L61
[Q1b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L69
[Q1c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L84
[Q1d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L100
[Q1e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L118
[Q1f]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L134
[S1]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L150
[S3]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L164
[Q1-cube-keep-only]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L174
[Q2a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L191
[Q2a-total]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L207
[Q2a-filtered]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L222
[Q2b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L233
[Q2c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L247
[F4]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L259
[F5]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L267
[F5b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L275
[F10]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L284
[F11]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L292
[F13]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L301
[X5]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L310
[Q2-cube-share-of-region]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L319
[Q3a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L336
[Q3a-one-stage]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L352
[F1b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L360
[F1c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L370
[F2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L380
[F2b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L391
[F3]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L402
[F7]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L413
[F9]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L423
[F9b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L432
[F9c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L444
[F9d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L454
[F9e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L470
[F9f]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L483
[X6]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L497
[Q3-cube-grain-include]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L514
[Q3-cube-avg-of-avg]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L529
[Q3-cube-sum-of-distinct]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L541
[Q3-cube-sum-of-avg-measure]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L552
[Q3-cube-number-agg-plain]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L562
[Q3-cube-number-agg-ms-column]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L575
[Q3-cube-number-agg-ms-grain]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L586
[Q4a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L598
[Q4a2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L635
[Q4b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L657
[Q4c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L670
[Q4d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L681
[Q4e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L692
[Q4f]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L707
[Q4f2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L728
[Q4f-where]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L739
[Q4f-later-stage]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L754
[Q4-filtered-escape]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L766
[Q4g]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L781
[Q4h]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L794
[Q4i]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L806
[Q4j]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L819
[Q4k]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L830
[S2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L841
[Q4-cube-compare-date-range]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L849
[Q4-cube-lag-change]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L862
[Q4-cube-prior-year-only]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L876
[Q4-cube-prior-year-with-revenue]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L895
[Q5a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L912
[Q5a2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L940
[Q5a-in-dimension]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L962
[Q5b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L970
[Q5-having]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L990
[Q5-agg-in-where]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1009
[Q5-calc-in-having]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1017
[Q5-order-by-expr]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1025
[Q5d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1033
[F8]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1044
[X4]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1056
[Q5-cube-sub-query]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1069
[Q5-cube-sub-query-count]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1084
[Q6a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1097
[Q6a-assoc]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1112
[Q6a-error]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1129
[Q6b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1137
[Q6c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1155
[Q6d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1169
[Q6e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1184
[Q6f]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1199
[Q6-locality]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1209
[Q6-multihop]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1219
[X1]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1231
[X3]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1250
[X7]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1268
[Q6-cube-raw-sum]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1279
[Q6-cube-raw-sum-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1292
[Q6-cube-region]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1303
[Q6-cube-region-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1316
[Q7a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1329
[Q7b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1341
[Q7c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1353
[Q7c2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1361
[Q7d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1374
[Q7e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1388
[Q7f]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1401
[Q7g]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1413
[Q7h]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1426
[Q7i]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1439
[Q9c-1]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1451
[Q9c-2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1462
[Q9c-3]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1473
[Q7-lag-of-calc]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1481
[Q7-lag-of-all]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1489
[Q7-calc-of-agg]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1497
[Q7-analytic-measure]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1505
[Q7-3105-row-number]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1513
[Q7-3105-lag-sum]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1522
[Q7-3105-measure]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1531
[Q7-3105-sole]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1540
[Q7-3105-variants]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1549
[Q7-lag-input]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1558
[Q7-lag-input-stage2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1566
[Q7-avg-moving-input]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1574
[Q7-lag-output-ref]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1582
[Q7-method-syntax]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1590
[Q7-cube-cumsum-of-change]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1598
[Q7-cube-rank-of-share]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1611
[Q8a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1624
[Q8b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1639
[Q8c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1662
[Q8-from-orders]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1683
[Q8-from-customers]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1693
[Q8-cube-root-tesseract]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1704
[Q8-cube-root-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1714
[Q8-cube-join-hints]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1724
[Q8-cube-region-root]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1733
[Q8-cube-region-root-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1745
[Q8-cube-region-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1754
[Q8-cube-count-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1766
[Q9a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1776
[Q9a-count-leaf]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1805
[Q9a-count-leaf-total]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1815
[Q9a-count-filtered]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1825
[Q9a-sum-parent]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1835
[Q9a-sum-per-customer]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1846
[Q9a-nopk-count]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1858
[Q9a-nopk-sum]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1868
[Q9a-traverses]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1881
[Q9a-no-locality]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1890
[Q9-bad-join-usage]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1898
[Q9a2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1908
[Q9b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1928
[Q9b2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1941
[Q9b3]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1954
[Q9d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1964
[Q9d-assoc]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1975
[F6]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L1997
[F6-assoc]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2007
[F6-error]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2020
[B1]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2029
[B1-assoc]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2046
[B1-tier]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2062
[B1-cube-tier-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2076
[B2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2085
[B2-coalesce]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2095
[Q9-cube-no-pk]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2104
[Q10a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2113
[Q10a-restricts-orders]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2136
[Q10b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2150
[Q10c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2178
[Q10d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2197
[Q10d-malloy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2209
[Q10e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2221
[Q10f]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2240
[Q10g]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2250
[Q10h]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2260
[Q10-exists-having]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2276
[Q10-antijoin-count]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2286
[X2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2296
[Q10-cube-not-equals-null]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2307
[Q10-cube-not-set]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2319
[Q10-cube-not-set-measure-only]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2328
[Q10-cube-not-set-measure-only-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2341
[Q10-cube-not-equals-measure-only-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2350
[Q11a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2363
[Q11-count_distinct]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2375
[Q11-min]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2387
[Q11-max]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2399
[Q11-count]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2411
[Q11-avg]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2423
[Q11-last]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2435
[Q11-first]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2448
[Q11-range]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2461
[Q11-range-cd]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2482
[Q11-status]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2502
[W1]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2515
[W3]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2529
[Q11-moving]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2542
[Q11-distinct-sum]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2556
[Q11-cube-rows-frame]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2572
[Q11-cube-range-frame]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2587
[Q11-cube-rolling-no-range]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2596
[Q12a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2608
[Q12-agg-in-agg]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2624
[Q12b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2632
[Q12c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2641
[Q12d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2652
[Q13a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2662
[Q13a-order]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2680
[Q13-not-in-output]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2696
[Q13-dotted]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2704
[Q13-calc]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2712
[Q13b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2720
[Q13c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2731
[Q13-cube-order-measure]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2740
[Q13-cube-order-dimension]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2752
[Q14a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2763
[Q14a-nest]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2782
[Q14b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2795
[W2]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2807
[Q14c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2820
[Q14d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2843
[Q14-cube-rank]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2859
[Q14-cube-rank-uncast]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2869
[Q14-cube-rank-measure]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2876
[Q14-cube-rank-measure-filtered]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2887
[Q15a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2901
[Q15b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2916
[Q15c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2929
[Q15d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2940
[Q15e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2954
[Q15f]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2968
[Q15g]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2978
[Q15h]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L2988
[Q16a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3005
[Q16b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3021
[Q16c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3030
[Q16-cube-dow]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3039
[Q16-cube-dow-direct]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3047
[Q17a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3055
[Q17b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3078
[Q17c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3089
[Q17d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3100
[Q17-lag-aov]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3118
[Q17e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3133
[Q18-create]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3142
[Q18a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3155
[Q18-malloy-day]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3168
[Q18-malloy-inline]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3178
[Q18-malloy-week]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3188
[Q18-malloy-filter]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3199
[Q18-malloy-year]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3210
[Q18b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3219
[Q18c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3232
[Q18d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3247
[Q18e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3264
[Q18-cube-day]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3275
[Q19-cube-rest]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3285
[Q19-cube-sql]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3297
[Q19-cube-rest-legacy]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3309
[Q19-cube-sql-orders-first]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3321
[Q20-cube-days-ago]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3332
[Q20-cube-last-years]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3342
[Q21-cube-timezone]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3354
[Q21-cube-day-of-week]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3367
[C1]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3375
[C5a]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3391
[C5b]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3400
[C5c]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3409
[C5d]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3423
[C5e]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3435
[C5f]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3443
[Q1-mf-metric-filter-region]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3453
[Q4-mf-offset-no-row]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3464
[Q4-mf-change]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3483
[Q4-mf-cum-day]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3503
[Q4-mf-ytd-first]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3520
[Q5-mf-band-high]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3542
[Q6-mf-two-hop-city]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3557
[Q8-mf-root-orders]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3569
[Q8-mf-union-null]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3584
[Q8-mf-fill-nulls]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3602
[Q9-mf-raw-where-alias]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3618
[Q10-mf-any-order]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3639
[Q10-mf-gold-or-any-order]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3650
[Q10-mf-rowlevel-or]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3663
[Q10-mf-antijoin-eq0]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3674
[X2-mf-any]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3688
[Q11-mf-90d-day]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3704
[Q11-mf-90d-cd-day]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3721
[Q11-mf-90d-last]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3738
[Q13-mf-order-unselected-dim]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3761
[Q16-mf-filter-expression]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3771
[Q16-mf-groupby-expression]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3782
[Q18-mf-filter-day-grain]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3791
[Q20-mf-sql-date-math]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3801
[Q20-mf-start-snaps]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3812
[Q21-mf-dow]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3831
[Q21-mf-dow-dunder]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3843
[Q23-mf-saved-where]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3852
[Q23-mf-saved-groupby]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3866
[C16-mf-fiscal-year]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3875
[C17-mf-drill-across-tier]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3889
[C19-mf-dimension-only]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3904
[Q20-slayer-last-n]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3916
[Q20-slayer-ago]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3928
[Q20-slayer-this-year]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3937
[Q20-slayer-quarter-literal]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3946
[Q20-slayer-period-upper]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3955
[Q20-slayer-open-ended]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3964
[Q23-slayer-refine-groupby]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3976
[Q23-slayer-refine-where]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3987
[Q23-slayer-refine-measure]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L3996
[Q23-slayer-refine-top]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4005
[Q23-slayer-refine-final-stage]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4014
[Q23-slayer-refine-aggregated-away]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4023
[Q23-slayer-refine-conflict]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4031
[C22-spine-fill-month]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4040
[C22-spine-fill-coalesce]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4052
[C22-spine-no-lower-bound]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4064
[C22-spine-per-group]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4072
[C22-spine-fact-filter]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4088
[C22-spine-change]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4100
[C22-spine-cumsum]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4113
[C22-spine-consecutive]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4126
[C22-spine-window]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4140
[C22-spine-lag]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4153
[C22-spine-stage]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4166
[C21-axis-two-facts]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4179
[C21-axis-two-facts-region]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4196
[C21-axis-ambiguous-route]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4215
[C21-calendar-model-parity]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4224
[C16-slayer-fiscal-year]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4240
[C16-slayer-fiscal-year-shift]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4251
[C16-slayer-fiscal-year-spine]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4263
[C16-slayer-quarter-hour]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4278
[C16-slayer-quarter-hour-spine]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4287
[C21-mf-two-facts]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4301
[C22-mf-no-fill]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4316
[C22-mf-join-to-timespine]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4332
[C21-cube-two-facts]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4351
[C21-malloy-calendar]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4370
[Q21-slayer-day-of-week]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4391
[Q21-slayer-date-diff]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4400
[Q21-slayer-date-add]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4409
[Q21-slayer-interval]: https://github.com/MotleyAI/slayer/blob/main/examples/comparisons/probes.yaml#L4420
<!-- probe-links:end -->
