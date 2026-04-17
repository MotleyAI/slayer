# SLayer vs dbt Semantic Layer — Comparison

This document compares SLayer and the dbt Semantic Layer (MetricFlow), highlighting where each system is more or less expressive than the other.

Both are semantic layers that sit between a database and consumers (LLMs, BI tools, applications). They share core concepts — models/tables, dimensions (GROUP BY columns), measures (aggregatable expressions), and joins — but differ significantly in design philosophy:

- **dbt Semantic Layer** bakes aggregation into the measure definition and uses a separate "metrics" layer for business KPIs. Joins are implicit via entity matching.
- **SLayer** keeps measures as raw expressions and specifies aggregation at query time. There is no separate metrics layer — filtered measures and composable formulas handle the same use cases. Joins are explicit.

For details on importing dbt definitions into SLayer, see [dbt Import](dbt_import.md).

---

## Where SLayer Expresses dbt Constructs Differently

Some dbt constructs that look missing at first glance are in fact expressible in SLayer — just via different primitives. The three building blocks are query-time [aggregations](../examples/07_aggregations/aggregations.md), multi-stage queries via [query-as-model](../examples/06_multistage_queries/multistage_queries.md), and dynamic [`ModelExtension`](../concepts/queries.md#modelextension).

### Semi-Additive Measures (`non_additive_dimension`)

dbt supports measures like account balances where `SUM` across time is wrong — you need `MAX` or `MIN` over the time dimension, then `SUM` across other dimensions. The `non_additive_dimension` with `window_choice` and `window_groupings` handles this.

In SLayer, this is a two-stage query:

- **Stage 1** — group by `window_groupings` plus the time bucket, and pick the latest (or earliest) value per group using the [`first` / `last` aggregations](../examples/07_aggregations/aggregations.md#first-and-last) with an explicit time column: `balance:last(snapshot_date)` for `window_choice: max`, `balance:first(snapshot_date)` for `window_choice: min`.
- **Stage 2** — feed stage 1 into the next query via a [query list](../concepts/queries.md#query-lists); because [any SLayer query automatically becomes a model](../concepts/models.md#creating-models-from-queries), the outer query can aggregate additively (`sum`, `avg`, …) across the remaining dimensions.

Example — account balances rolled up to customer-level monthly totals:

```json
[
  {
    "name": "latest_balance_per_account",
    "source_model": "account_snapshots",
    "fields": ["balance:last(snapshot_date)"],
    "dimensions": ["account_id", "customer_id"],
    "time_dimensions": [{"dimension": "snapshot_date", "granularity": "month"}]
  },
  {
    "source_model": "latest_balance_per_account",
    "fields": ["balance_last:sum"],
    "dimensions": ["customer_id"],
    "time_dimensions": [{"dimension": "snapshot_date", "granularity": "month"}]
  }
]
```

If the source model does not already expose the grouping entities, pull them in via [`ModelExtension`](../concepts/queries.md#modelextension) on the inner query — see [Joins](../examples/05_joins/joins.md) and [Measures from joined models](../examples/05_joined_measures/joined_measures.md) for the cross-model semantics.

### Per-Measure `agg_time_dimension`

dbt allows each measure within a semantic model to have its own default time dimension. SLayer has one [`default_time_dimension`](../concepts/models.md) per model, and the user picks the time dimension at query time via [`time_dimensions`](../concepts/queries.md#timedimension) — same outcome, specified one layer later. For aggregations that inherently need a time column — notably [`first` / `last`](../examples/07_aggregations/aggregations.md#first-and-last) — you can pin the column directly on the field: `measure_name:last(time_col)`. This explicit argument overrides both the query's `time_dimensions` and the model's `default_time_dimension`.

---

## Where SLayer Cannot Express dbt Constructs

### No Rolling-Window Cumulative

SLayer's [`cumsum()`](../concepts/formulas.md) accumulates from the beginning of the result set. dbt supports `window: {count: 30, period: day}` for trailing windows. A self-join over [`ModelExtension`](../concepts/queries.md#modelextension) could emulate this but is awkward; see the [Time example](../examples/04_time/time.md) for the transforms that are natively supported.

### No `grain_to_date` Cumulative Reset

dbt supports resetting cumulative at grain boundaries (e.g., month-to-date resets each month). SLayer's [`cumsum`](../concepts/formulas.md) has no partition-reset variant.

### No Conversion Metrics

Entity-based sequential event tracking (e.g., "users who visited then purchased within 7 days"). SLayer has no equivalent — this requires entity-based pre-aggregated joins with time windows.

---

## Where SLayer Is Simpler Than dbt

### Aggregation at Query Time

dbt: Want `revenue` summed AND averaged? Define two separate metrics. 20 columns x 3 aggregations = 60 metric definitions.

SLayer: One measure `revenue`. Query `revenue:sum`, `revenue:avg`, `revenue:min` as needed. Zero duplication.

### Composable Formula Syntax

dbt requires separate metric type definitions for each analytical pattern:
- Simple metric for `revenue_sum`
- Derived metric for `revenue_per_order = revenue / orders`
- Cumulative metric for `running_revenue`
- Another derived for `revenue_growth = (current - previous) / previous`

SLayer handles all of these inline in a single query:
```json
"fields": [
  "revenue:sum",
  {"formula": "revenue:sum / *:count", "name": "aov"},
  {"formula": "cumsum(revenue:sum)", "name": "running"},
  {"formula": "change_pct(revenue:sum)", "name": "growth"}
]
```

### No Jinja Templating

dbt filters require `{{ Dimension('entity__name') }}` syntax with entity resolution. SLayer filters are plain SQL-like strings: `"status = 'active'"`. More readable, no template engine needed.

### Explicit Joins (Predictable)

dbt's entity-based implicit join resolution is powerful but opaque — you must understand the entity graph to predict which tables will be joined. SLayer's explicit `join_pairs` are visible in the model definition.

### Query-as-Model (Multi-Stage Without New Concepts)

dbt: Multi-stage analytics require creating new dbt models (SQL files) and new semantic model definitions.

SLayer: Any query can be used as a model in the next query via query lists. No new files needed.

### Flatter Concept Stack

dbt has 3 layers: semantic models, metrics, saved queries.

SLayer has 2 layers: models, queries (with queries optionally becoming models).
