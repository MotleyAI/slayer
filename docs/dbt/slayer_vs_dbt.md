# SLayer vs dbt Semantic Layer — Comparison

This document compares SLayer and the dbt Semantic Layer (MetricFlow), highlighting where each system is more or less expressive than the other.

Both are semantic layers that sit between a database and consumers (LLMs, BI tools, applications). They share core concepts — models/tables, dimensions (GROUP BY columns), measures (aggregatable expressions), and joins — but differ significantly in design philosophy:

- **dbt Semantic Layer** bakes aggregation into the measure definition and uses a separate "metrics" layer for business KPIs. Joins are implicit via entity matching.
- **SLayer** keeps measures as raw expressions and specifies aggregation at query time. There is no separate metrics layer — filtered measures and composable formulas handle the same use cases. Joins are explicit.

For details on importing dbt definitions into SLayer, see [dbt Import](dbt_import.md).

---

## Where SLayer Cannot Express dbt Constructs

### No `non_additive_dimension` (Semi-Additive Measures)

dbt supports measures like account balances where `SUM` across time is wrong — you need `MAX` or `MIN` over the time dimension, then `SUM` across other dimensions. The `non_additive_dimension` with `window_choice` and `window_groupings` handles this.

SLayer partial equivalent: `balance:last(updated_at)` gets the latest value per group per time bucket. The full pattern requires a multi-stage query. **Not used in the dbt benchmark.**

### No Rolling-Window Cumulative

SLayer's `cumsum()` accumulates from the beginning of the result set. dbt supports `window: {count: 30, period: day}` for trailing windows. **Not used in the dbt benchmark.**

### No `grain_to_date` Cumulative Reset

dbt supports resetting cumulative at grain boundaries (e.g., month-to-date resets each month). SLayer has no equivalent. **Not used in the dbt benchmark.**

### No Conversion Metrics

Entity-based sequential event tracking (e.g., "users who visited then purchased within 7 days"). SLayer has no equivalent — this requires entity-based pre-aggregated joins with time windows. **Not used in the dbt benchmark.**

### Per-Measure `agg_time_dimension`

dbt allows each measure within a semantic model to have its own default time dimension. SLayer has one `default_time_dimension` per model. Minor gap — the user specifies the time dimension at query time. **Not used in the dbt benchmark.**

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
