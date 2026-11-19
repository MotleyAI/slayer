# SLayer vs dbt Semantic Layer — Comparison

This document compares SLayer and the dbt Semantic Layer (MetricFlow), highlighting where each system is more or less expressive than the other.

Both are semantic layers that sit between a database and consumers (LLMs, BI tools, applications). They share core concepts — datasets/tables, dimensions (GROUP BY columns), metrics (aggregatable expressions), and relationships — but differ significantly in design philosophy:

- **dbt Semantic Layer** bakes aggregation into the measure definition and uses a separate "metrics" layer for business KPIs. Joins are implicit via entity matching.
- **SLayer** keeps fields as raw expressions and specifies aggregation at query time. There is no separate metrics layer — filtered fields and composable expressions handle the same use cases. Relationships are explicit.

For details on importing dbt definitions into SLayer, see [dbt Import](dbt_import.md).

---

## Where SLayer Expresses dbt Constructs Differently

Some dbt constructs that look missing at first glance are in fact expressible in SLayer — just via different primitives. The three building blocks are query-time [aggregations](../examples/07_aggregations/aggregations.md), multi-stage queries via [query-as-dataset](../examples/06_multistage_queries/multistage_queries.md), and dynamic [query extensions](../concepts/queries.md#query-extensions).

### Semi-Additive Measures (`non_additive_dimension`)

dbt supports measures like account balances where `SUM` across time is wrong — you need `MAX` or `MIN` over the time dimension, then `SUM` across other dimensions. The `non_additive_dimension` with `window_choice` and `window_groupings` handles this.

In SLayer this is one expression, `sum(last(balance, snapshot_date, partition_by=[account_id, customer_id, snapshot_date]))` grouped by `customer_id` and a month `snapshot_date` time dimension (the time field in `partition_by` stands for its bucket), or equivalently a two-stage query:

- **Stage 1** — group by `window_groupings` plus the time bucket, and pick the latest (or earliest) value per group using the [`first` / `last` aggregations](../examples/07_aggregations/aggregations.md#first-and-last) with an explicit time field: `last(balance, snapshot_date)` for `window_choice: max`, `first(balance, snapshot_date)` for `window_choice: min`.
- **Stage 2** — feed stage 1 into the next query via a [query list](../concepts/queries.md#query-lists); because [any SLayer query automatically becomes a dataset](../concepts/datasets.md#creating-datasets-from-queries), the outer query can aggregate additively (`sum`, `avg`, …) across the remaining dimensions.

Example — account balances rolled up to customer-level monthly totals:

```json
[
  {
    "name": "latest_balance_per_account",
    "population": "account_snapshots",
    "metrics": ["last(balance, snapshot_date)"],
    "dimensions": ["account_id", "customer_id"],
    "time_dimensions": [{"field": "snapshot_date", "granularity": "month"}]
  },
  {
    "population": "latest_balance_per_account",
    "metrics": ["sum(balance_last_snapshot_date)"],
    "dimensions": ["customer_id"],
    "time_dimensions": [{"field": "snapshot_date", "granularity": "month"}]
  }
]
```

If the source dataset does not already expose the grouping entities, pull them in via [query extensions](../concepts/queries.md#query-extensions) on the inner query — see [Relationships](../examples/05_relationships/relationships.md) and [Metrics from joined datasets](../examples/05_joined_metrics/joined_metrics.md) for the cross-dataset semantics.

### Per-Measure `agg_time_dimension`

dbt allows each measure within a semantic model to have its own default time dimension. SLayer has one [`default_time_dimension`](../concepts/datasets.md) per dataset, and the user picks the time dimension at query time via [`time_dimensions`](../concepts/queries.md#timedimension) — same outcome, specified one layer later. For aggregations that inherently need a time field — notably [`first` / `last`](../examples/07_aggregations/aggregations.md#first-and-last) — you can pin the time field directly in the aggregation: `last(field_name, time_col)`. This explicit argument overrides both the query's `time_dimensions` and the dataset's `default_time_dimension`.

---

## Where SLayer Cannot Express dbt Constructs

These constructs are **failed cleanly** by the importer — routed to the [conversion report](dbt_import.md#clean-fail-and-unsupported) with a workaround and stashed into the owning entity's `meta`, never converted to approximate or wrong SQL.

### Rolling-Window Cumulative (not imported)

SLayer expresses trailing windows at query time with [`window=`](../concepts/expressions.md#windowed-aggregations) (e.g. `sum(revenue, window='30d')`), but the importer does not yet convert dbt's windowed cumulative metrics and fails them cleanly.

### No `grain_to_date` Cumulative Reset

dbt supports resetting cumulative at grain boundaries (e.g., month-to-date resets each month). SLayer's [`cumsum`](../concepts/expressions.md) has no partition-reset variant — put the grain dimension in the query instead.

### No Conversion Metrics

Entity-based sequential event tracking (e.g., "users who visited then purchased within 7 days"). SLayer has no equivalent — this requires entity-based pre-aggregated joins with time windows.

### Other clean-fails

`offset_to_grain`, an `offset_window` on a multi-aggregate (ratio/derived) input, non-standard granularities, discrete/approximate percentile flags, and measure-less simple metrics (`metric_aggregation_params`) are all failed cleanly with a documented workaround — see the [clean-fail table](dbt_import.md#clean-fail-and-unsupported).

---

## Where SLayer Is Simpler Than dbt

### Aggregation at Query Time

dbt: Want `revenue` summed AND averaged? Define two separate metrics. 20 columns x 3 aggregations = 60 metric definitions.

SLayer: One field `revenue`. Query `sum(revenue)`, `avg(revenue)`, `min(revenue)` as needed. Zero duplication.

### Composable Expression Syntax

dbt requires separate metric type definitions for each analytical pattern:
- Simple metric for `revenue_sum`
- Derived metric for `revenue_per_order = revenue / orders`
- Cumulative metric for `running_revenue`
- Another derived for `revenue_growth = (current - previous) / previous`

SLayer handles all of these inline in a single query:
```json
"metrics": [
  "sum(revenue)",
  {"metric": "sum(revenue) / count(*)", "alias": "aov"},
  {"metric": "cumsum(sum(revenue))", "alias": "running"},
  {"metric": "change_pct(sum(revenue))", "alias": "growth"}
]
```

### No Jinja Templating

dbt filters require `{{ Dimension('entity__name') }}` syntax with entity resolution. SLayer filters are plain SQL-like strings: `"status = 'active'"`. More readable, no template engine needed.

### Explicit Relationships (Predictable)

dbt's entity-based implicit join resolution is powerful but opaque — you must understand the entity graph to predict which tables will be joined. SLayer's explicit `from_columns` and `to_columns` are visible in the dataset definition.

### Query-as-Dataset (Multi-Stage Without New Concepts)

dbt: Multi-stage analytics require creating new dbt models (SQL files) and new semantic model definitions.

SLayer: Any query can be used as a dataset in the next query via query lists. No new files needed.

### Flatter Concept Stack

dbt has 3 layers: semantic models, metrics, saved queries.

SLayer has 2 layers: datasets, queries (with queries optionally becoming datasets).
