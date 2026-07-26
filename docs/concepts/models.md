# Models

A model is SLayer's view of a database table or an underlying SQL query. It declares the columns, named metric formulas, joins, and always-applied filters that queries can build on. Models are defined as YAML (one file per model under `models/<data_source>/`) or created via API/MCP — the two paths produce the same persisted object.

A tiny example to anchor what follows:

```yaml
name: orders
sql_table: public.orders
data_source: my_postgres
columns:
  - {name: id, type: number, primary_key: true}
  - {name: status, type: string}
  - {name: revenue, sql: amount, type: number}
measures:
  - {name: aov, formula: "revenue:sum / *:count"}
```

A query then asks for `revenue:sum` (aggregate the `revenue` column), `aov` (the saved formula), or `status` (group by it). Same model, different roles per query.

## Fields at a glance

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique model name. In the default YAML storage, names differing only by letter case (`Orders` vs `orders`) are rejected at save time (`IdCollisionError`) — they would collide as filenames on macOS / Windows |
| `sql_table` | string | One of | A physical database table (e.g. `public.orders`) |
| `sql` | string | these | A SQL subquery to use as the source |
| `source_queries` | list[SlayerQuery] | three | Saved query stages — makes the model **query-backed** |
| `data_source` | string | Yes | Datasource name |
| `columns` | list[Column] | No | Column definitions. For query-backed models this is an engine-managed cache |
| `measures` | list[ModelMeasure] | No | Named formula library — referenced by bare name in queries |
| `aggregations` | list[Aggregation] | No | Custom aggregation operators usable via colon syntax |
| `joins` | list[ModelJoin] | No | LEFT JOIN relationships to other models |
| `filters` | list[str] | No | Model-level WHERE filters (always applied) |
| `default_time_dimension` | string | No | Default time dim for time-dependent formulas |
| `query_variables` | dict | No | Defaults for `{var}` placeholders (query-backed models only) |
| `backing_query_sql` | string | No | Engine-managed cache of the rendered backing query |
| `description` | string | No | Helps agents and users understand the model |
| `hidden` | bool | No | Hide from listings |
| `meta` | dict | No | Arbitrary JSON metadata for caller bookkeeping |
| `version` | int | No | Schema version stamp (currently `6`) |

## Source modes

A model has exactly one source — set by one of three mutually exclusive fields:

- **`sql_table`** — a physical database table.
- **`sql`** — an explicit SQL subquery (a `SELECT` statement). Useful when the model's underlying shape requires cleaning or joining beyond what SLayer expresses natively.
- **`source_queries`** — one or more saved `SlayerQuery` stages. Makes the model **query-backed**: see [Query-backed models](#query-backed-models).

Validators reject empty `source_queries=[]`, multiple sources, or missing names on non-final stages.

## Columns

A column is the unit of structure on the model. The same column entry can serve as a group-by key in one query and as input to an aggregation in another — the role is decided per query, not declared up front. What the column *carries* is its identity (name), how to compute it from the source (`sql`), what data type to expect, and a handful of policy fields (which aggregations are allowed, whether it's a primary key, whether it's hidden).

### Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique within the model. Must not contain `.` |
| `description` | string | No | — | Clarifies meaning for agents and users |
| `label` | string | No | — | Display name; propagates into query result metadata |
| `sql` | string | No | (bare column name) | SQL expression — defaults to the column's name |
| `type` | string | No | `string` | `string`, `number`, `boolean`, `time`, `date` |
| `primary_key` | bool | No | `false` | Restricts aggregation to `count` / `count_distinct` |
| `hidden` | bool | No | `false` | Hide from listings |
| `format` | dict | No | — | `NumberFormat` used by response metadata |
| `allowed_aggregations` | list[str] | No | — | Whitelist (must be a subset of the type-default eligibility set, or a custom aggregation defined on this model) |
| `filter` | string | No | — | SQL condition applied inside `CASE WHEN` at aggregation time. See [Filtered columns](#filtered-columns) |
| `meta` | dict | No | — | Arbitrary JSON metadata |
| `sampled` | string | No | — | Cached sample-value text snapshot (top-20 by frequency joined, or `top20 ... (50+ distinct)` on overflow, or `min .. max` for numeric/temporal); populated lazily on the first `inspect` of the column (or via `slayer search refresh-samples`), not at ingest time |
| `sampled_values` | list[str] | No | — | Structured top-50-by-frequency list (categorical only); the unambiguous counterpart to `sampled` for consumers that need to compare predicate literals against stored values. `None` for numeric/temporal columns |
| `distinct_count` | int | No | — | Exact distinct count when ≤ 50 (categorical only). `None` on overflow (> 50 distinct — one scan only, no secondary `count_distinct` query) and for numeric/temporal columns |

### Data types

| Type | Description | SQL examples |
|------|-------------|--------------|
| `string` | Text values | VARCHAR, TEXT, CHAR |
| `number` | Numeric values | INTEGER, FLOAT, NUMERIC |
| `boolean` | True/false | BOOLEAN |
| `time` | Timestamp | TIMESTAMP, DATETIME |
| `date` | Date only | DATE |

### Aggregation eligibility

A column with no explicit `allowed_aggregations` whitelist gets a default set based on its data type. The defaults are deliberately liberal for numerics and strict for the rest, so an agent doesn't accidentally `SUM` a string column.

| Type | Default eligible aggregations |
|------|-------------------------------|
| `number` | sum, avg, min, max, count, count_distinct, count_distinct_approx, median, weighted_avg, percentile, first, last, stddev_samp, stddev_pop, var_samp, var_pop, corr, covar_samp, covar_pop |
| `string` | count, count_distinct, count_distinct_approx, first, last, min, max |
| `boolean` | count, count_distinct, count_distinct_approx, sum, min, max, first, last |
| `date` / `time` | count, count_distinct, count_distinct_approx, first, last, min, max |

`count_distinct_approx` is dialect-aware: it emits the database-native approximate-distinct function where one exists and falls back to an exact `COUNT(DISTINCT)` where it does not (Postgres / SQLite / MySQL). Primary-key columns are always restricted to `count` / `count_distinct` / `count_distinct_approx` regardless of type. When `allowed_aggregations` is set, every entry must already be eligible under the type-default map (or be a custom aggregation defined on this model); violations are caught at model construction time, so query-time validation is a single membership check.

### Filtered columns

A column can carry a `filter` — a SQL condition wrapped around the column inside an aggregation via `CASE WHEN`. This is how you express business metrics that apply to a row subset without a separate model:

```yaml
columns:
  - name: active_revenue
    sql: amount
    type: number
    filter: "status = 'active'"
  - name: completed_count
    sql: id
    type: number
    filter: "status = 'completed'"
```

`active_revenue:sum` then generates `SUM(CASE WHEN status = 'active' THEN amount END)`. The filter does nothing when the column is used as a group-by dimension — it fires only inside aggregations.

Filters can reference joined columns via dot syntax (`categories.type = 'electronics'`). Filtered and unfiltered columns coexist freely in the same query and combine cleanly in arithmetic formulas (e.g. `{"formula": "active_revenue:sum / total_revenue:sum"}`).

### Derived Columns Referencing Other Derived Columns

A `Column.sql` may reference any other column on the same model or on a joined model — including columns that are themselves *derived* (have their own `sql` expression rather than being a bare base-table column). The engine recursively inlines those references at query time, so chains stay DRY.

```yaml
# Model: stations
columns:
  - name: foo_raw                 # base column
    sql: "foo_raw"
    type: number

  - name: foo_normalized          # derived on stations
    sql: "foo_raw / 100.0"
    type: number

# Model: telescopes — joined to stations
columns:
  - name: aoi_ratio               # derived on telescopes, references the
                                  # *derived* stations.foo_normalized
    sql: "telescopes.aperture / stations.foo_normalized"
    type: number
joins:
  - target_model: stations
    join_pairs: [["station_id", "id"]]
```

At query time, `aoi_ratio` expands to `telescopes.aperture / (stations.foo_raw / 100.0)`. The same applies to local-model chains (a column on the source model referencing another derived column on the same model) and to multi-hop join paths (use the `__`-delimited form, e.g., `B__C.x_derived`, when crossing more than one join).

Same-model references may be written **bare** (just the column name) or qualified with the host alias — both forms expand the same way. So given `bucket.sql = "raw_a / 10"`, a sibling `rn.sql = "ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY id)"` correctly expands `bucket` to the inlined body. Bare references inside a nested scope (sub-query, `UNION` branch, CTE, `VALUES`) are NOT inlined — those identifiers belong to the inner rowset, not the host model — so `Column.sql = "(SELECT MAX(score) FROM other) + score"` inlines the outer `score` but leaves the inner one alone.

Cycles in the reference graph (e.g., `c1.sql = "c2 + 1"` and `c2.sql = "c1 - 1"`) are rejected at `save_model` time and raise `ColumnCycleError` (which subclasses both `SlayerError` and `ValueError`) with the cycle path in the message — so a broken chain never reaches a query. The compile-time guard remains as defence in depth. Save-time validation stays within the model's `data_source`; unresolved cross-datasource refs are silently skipped. The same expansion is applied to filters and to colon-aggregated measures, so `"B.foo_normalized:sum"` produces `SUM(B.foo_raw / 100.0)`.

### Window functions in `Column.sql`

A column's `sql` may contain a window function (`row_number() over (...)`, `dense_rank() over (...)`, etc.). The column behaves like any other column when used in `dimensions` / SELECT.

> **Filtering on a windowed column is rejected.** A query filter naming a `Column` whose `sql` contains a window function (e.g. `{"filters": ["rn <= 3"]}` against a column whose `sql` is `row_number() over (...)`) raises with a clear message. Use `{"filters": ["rank(<measure>) <= 3"]}` (see [formulas.md](formulas.md#rank)) — the rank-family transforms cover the top-N case in pure DSL — or factor the column into a multi-stage `source_queries` model.

### SQL expression conventions

Bare column names auto-qualify against the model's own table — single references (`"amount"`) and arithmetic alike (`"amount * quantity"`). Explicit `model.column` works too but adds nothing. For joined columns inside a model's own `sql`, use the `__` alias form (`customers__regions.name`) — see [Joins](#joins).

### SQLite JSON extraction

`json_extract(col, '$.path')` in a `Column.sql` is preserved verbatim on SQLite — SLayer does **not** rewrite it to `col -> '$.path'`. The `->` operator in SQLite returns the JSON-*quoted* form (`'"Owned"'`, with literal quotes), which silently breaks equality and `CASE WHEN` matches against bare-string literals. The function form returns the unquoted scalar.

```yaml
columns:
  - name: tier
    type: string
    sql: "json_extract(payload, '$.tier')"
  - name: is_gold
    type: number
    sql: "CASE LOWER(json_extract(payload, '$.tier')) WHEN 'gold' THEN 1 ELSE 0 END"
```

If you specifically want SQLite's JSON-scalar operator, write `->>` (`exp.JSONExtractScalar`) directly — SLayer leaves it untouched.

## Measures vs aggregations — the disambiguation

SLayer has two list fields on a model that both relate to metrics, and the names don't make the difference obvious. The split is real and load-bearing:

- **`measures`** is a library of named **formulas** — saved expressions like `aov = revenue:sum / *:count`. Queries reference them by bare name and the formula expands inline. Think *what to compute*, at the metric level.
- **`aggregations`** is a registry of custom **operators** — definitions like `trimmed_mean(p)` or `weighted_avg(weight=…)`. Once defined, they become usable as colon suffixes inside any formula: `revenue:trimmed_mean(p=0.1)`. Think *how to aggregate*, at the operator level.

A typical model uses zero or a few entries in each. They compose:

```yaml
aggregations:
  - name: trimmed_mean
    formula: "AVG(CASE WHEN {expr} BETWEEN {low} AND {high} THEN {expr} END)"

measures:
  - name: clean_aov
    formula: "revenue:trimmed_mean(low=0, high=1e6) / *:count"
```

## Measures (named formulas)

A measure is a saved formula. Its shape is identical to an inline `SlayerQuery.measures` entry; the only difference is scope — model-level measures are reusable across queries.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `formula` | string | Yes | e.g. `"revenue:sum / *:count"`, `"cumsum(revenue:sum)"` |
| `name` | string | No | Queries reference this by bare name (auto-derived if omitted) |
| `label` | string | No | Display name |
| `description` | string | No | Explanatory text |
| `type` | DataType | No | Declares the formula's result type (drives outer CAST) |
| `meta` | dict | No | Arbitrary JSON metadata |

Column and measure names share a namespace within a model — you can't have a column `aov` *and* a measure `aov`. A measure can use any other measure by bare name, including inside transforms and arithmetic:

```json
"measures": [
  {"formula": "aov"},
  {"formula": "cumsum(aov)"},
  {"formula": "aov * 1.1", "name": "aov_with_markup"}
]
```

Expansion is purely textual at parse time, so the SQL is identical to writing the longhand formula. Cycles (`a → b → a`) are detected and rejected with the cycle in the error message. Names that would shadow built-in transforms (`cumsum`, `change`, `rank`, `first`, `last`, etc.) are rejected at model construction time.

See [formulas.md](formulas.md) for the full formula grammar — operators, transforms, time-shifted forms, and the `Mode B` DSL rules.

## Aggregations

Aggregations are the operators that turn a column expression into a value: `:sum`, `:avg`, `:percentile(p=…)`, and so on. They're applied at query time via colon syntax — `measure:aggregation` — and the same operator works on any compatible column.

### Built-in aggregations

| Aggregation | Colon syntax | SQL |
|-------------|--------------|-----|
| `count` | `*:count` | `COUNT(*)` — counts all rows |
| `count` | `col:count` | `COUNT(col)` — counts non-null values |
| `count_distinct` | `col:count_distinct` | `COUNT(DISTINCT col)` |
| `sum` | `revenue:sum` | `SUM(revenue)` |
| `avg` | `revenue:avg` | `AVG(revenue)` |
| `min` / `max` | `revenue:min` | `MIN(revenue)` / `MAX(revenue)` |
| `first` / `last` | `col:first(time_col)` | Earliest / latest record's value, time-ordered |
| `weighted_avg` | `price:weighted_avg(weight=quantity)` | `SUM(price * quantity) / SUM(quantity)` |
| `median` | `revenue:median` | Median value |
| `percentile` | `revenue:percentile(p=0.95)` | 95th percentile |
| `stddev_samp` / `stddev_pop` | `latency:stddev_samp` | Sample / population standard deviation |
| `var_samp` / `var_pop` | `latency:var_samp` | Sample / population variance |
| `corr` | `price:corr(other=quantity)` | Pearson correlation between two columns |
| `covar_samp` / `covar_pop` | `price:covar_samp(other=quantity)` | Sample / population covariance |

`*:count` is always available with no measure definition. `*` means "all rows" and is **only** valid with `count` — `*:sum` and friends are rejected. Detailed NULL / N=1 semantics for the statistical aggregations are documented in [database-support.md](../database-support.md).

### The `first` and `last` aggregations

`first` and `last` return the value from the **earliest or most recent record** within each grouped bucket — like `min`/`max`, but ordered by time instead of value. Useful for snapshot metrics like balances, inventory counts, or status fields where you want the latest state:

```yaml
columns:
  - {name: balance, sql: balance, type: number}
```

`balance:last(updated_at)` gives the most recent balance per group; `balance:first(updated_at)` the earliest. When grouped by month, each month returns the latest (or earliest) record's balance in that month. If no time column is specified, ordering resolves via: query's `main_time_dimension` → first time/date dimension in the query → first time dimension in filters → model's `default_time_dimension`.

Not to be confused with the [`last()` formula function](formulas.md#last-function) — a window-function transform that broadcasts a value across all rows. Same name, different layer.

## Custom aggregations

Add your own operators via the `aggregations` list. Each entry has a name and a SQL formula template using `{expr}` for the measure expression and named placeholders for kwargs:

```yaml
aggregations:
  - name: weighted_avg
    formula: "sum({expr} * {weight}) / sum({weight})"
  - name: trimmed_mean
    formula: "avg(CASE WHEN {expr} BETWEEN {low} AND {high} THEN {expr} END)"
```

Use at query time: `price:weighted_avg(weight=quantity)`, `revenue:trimmed_mean(low=10, high=1000)`. An aggregation entry can also override a built-in's default parameters without redefining the SQL. Like columns and measures, aggregations accept an optional `meta` dict for caller bookkeeping.

## Joins

Models declare explicit LEFT JOINs to other models:

```yaml
name: orders
sql_table: public.orders
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
  - target_model: products
    join_pairs: [["product_id", "id"]]
```

Joins enable **cross-model measures** — querying a measure from a joined model alongside the main model's data. See [Cross-Model Measures](queries.md#cross-model-measures). During [auto-ingestion](ingestion.md), joins are generated automatically from foreign-key relationships; multi-hop paths are resolved at query time by walking each intermediate model's own joins.

### Path-based table aliases

Joined tables use `__`-delimited path aliases in generated SQL so **diamond joins** stay unambiguous — when the same table is reachable via multiple paths. For example, if `orders` joins both `customers` and `warehouses`, each referencing `regions`:

- `customers.regions.name` → table alias `customers__regions`
- `warehouses.regions.name` → table alias `warehouses__regions`

The convention is split by context: **queries use dots** for paths (`customers.regions.name`), **model-internal SQL uses `__`** for the alias (`customers__regions.name`). See [Diamond Joins](ingestion.md#diamond-joins) for details.

## Model filters

Model filters are SQL conditions always-applied to the underlying table:

```yaml
name: active_orders
sql_table: public.orders
filters:
  - "deleted_at IS NULL"
  - "status <> 'test'"
```

These are SQL-mode expressions (Mode A): any valid SQL the underlying dialect accepts — function calls (`json_extract`, `coalesce`, …), `CASE WHEN`, joined-column references via the `__` alias. Aggregation colon syntax and SLayer transforms are rejected here — those are DSL constructs (Mode B) and belong in query-level filters or `ModelMeasure.formula`. See [references.md](references.md) for the full Mode A / Mode B table. Multi-hop joined references (`customers.regions.name`) are auto-converted to the `__` form with a warning.

## Query-backed models

A query-backed model is a queryable relation whose rows are the final-stage result of one or more saved `SlayerQuery` stages. You can save any query as a model, then run it directly by name or use it as `source_model` in another query — exactly like any table-backed model.

### Saving a query as a model

```python
await engine.create_model_from_query(
    query={
        "source_model": "orders",
        "measures": [{"formula": "amount:sum"}],
        "dimensions": ["region"],
        "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}],
    },
    name="monthly_revenue",
    description="Monthly revenue by region",
    variables={"region": "US"},  # default placeholder values, optional
)
```

This saves the query structure in `model.source_queries`, saves any defaults in `model.query_variables`, runs save-time validation (any unresolved `{var}` placeholder defaults to `'0'` so SQL generation succeeds), and caches the resulting `columns` and rendered `backing_query_sql` on the model for fast inspection.

`create_model_from_query` accepts a single `SlayerQuery` or a list of stages; for multi-stage queries, every non-final stage must have a `name` so it can be referenced. Stages form a DAG: any stage may use a *prior* named sibling as `source_model` or as `joins.target_model`. Forward and self references are rejected.

### Two ways to use a saved query

Run the backing query directly by name — returns the final-stage result:

```python
await engine.execute("monthly_revenue", variables={"region": "US"})
```

Or use the saved result as a model in another query:

```json
{
  "source_model": "monthly_revenue",
  "measures": [{"formula": "amount_sum:avg"}],
  "dimensions": ["region"]
}
```

### Variable precedence

When a query-backed model references `{var}` placeholders, values flow in this order (highest first):

1. **Runtime kwarg** — `variables=` on `engine.execute(...)` (also via REST `/query`, MCP `query` / `create_model`, CLI `--variables` / `--variables-json`). Wins at every nesting level.
2. **Stage `.variables`** — set on an individual `SlayerQuery` stage.
3. **Outer query `.variables`** — when a query-backed model is used as `source_model` in another query.
4. **Model defaults** — `model.query_variables`.

Unresolved placeholders raise a clear error at execute time, naming the model and the missing variable. Runtime-kwarg variables that don't appear anywhere are silently ignored.

### What gets cached

For a query-backed model the engine caches `model.columns` (final-stage output columns — a discoverability snapshot) and `model.backing_query_sql` (the rendered backing query). The cache is populated **only** on save through `engine.save_model` (REST `POST`/`PUT /models`, MCP `create_model`/`edit_model`). **Read operations never write storage** — `engine.execute`, `inspect_model`, `get_column_types`, MCP `query`, and REST `/query` will never modify the persisted cache. Writing a query-backed model directly to storage outside the engine leaves the cache stale until the next engine save.

You **cannot** supply `columns` or `backing_query_sql` yourself when creating a query-backed model — both are engine-managed, and any user-supplied value is rejected with a clear error.

### Column naming in query-derived models

A query result is a self-contained table — it no longer has the joins the source model may have had. Dimensions and measures that came from joined models use `__` to encode the original join path in their name:

| Inner query field | Virtual model column name |
|----------------------|--------------------------|
| `stores.name` | `stores__name` |
| `customers.regions.name` | `customers__regions__name` |
| `customer_id` | `customer_id` |
| `*:count` (measure) | `count` |
| `revenue:sum` (measure) | `revenue_sum` |
| `{"formula": "revenue:sum", "name": "rev"}` | `rev` |

This uses the same `__` convention as SQL-level join path aliases. When referencing these columns in an outer query, use the `__` name directly (e.g., `{"name": "stores__name"}`), not dot syntax — dots would imply a join to a model that doesn't exist on the virtual table.

An explicit `name` on a measure spec **overrides** the canonical naming above, for both arithmetic/transform formulas and simple aggregations. This is what lets multi-stage `source_queries` rename inner-stage outputs cleanly:

```json
{
  "source_queries": [
    {
      "name": "raw",
      "source_model": "orders",
      "dimensions": ["region"],
      "measures": [{"formula": "amount:sum", "name": "rev"}]
    },
    {
      "source_model": "raw",
      "measures": [{"formula": "rev:sum"}]
    }
  ]
}
```

The inner stage emits a column named `rev` (not `amount_sum`), and the outer stage references it by that chosen name. See the [multistage queries example](../examples/06_multistage_queries/multistage_queries.md) for working examples.

## Result column format

Query results use `model_name.column_name` keys. Colon syntax is converted: `revenue:sum` becomes `orders.revenue_sum`; `*:count` becomes `orders._count` (leading underscore so the alias never collides with a user column literally named `count`). Multi-hop joined dimensions keep their full path:

```json
{"orders.status": "completed", "orders._count": 42, "orders.revenue_sum": 1500}
{"orders.customers.regions.name": "US", "orders._count": 3}
```

## Schema versioning

Every persisted SLayer entity (`SlayerModel`, `SlayerQuery`, `DatasourceConfig`) carries a `version: int` field — currently `6` for `SlayerModel`, `3` for `SlayerQuery`, `1` for `DatasourceConfig`. Behaviour:

- **On save**, SLayer always writes the current schema version.
- **On load**, an older `version` triggers a chain of pure dict→dict converters before Pydantic validation. Hand-edited and older files keep working as the schema evolves.
- **Round-tripping** an older file (load → save) upgrades it on disk to the current schema.
- **Forward tolerance.** A file with a higher `version` than this SLayer knows about loads on a best-effort basis. For `SlayerModel` and `DatasourceConfig`, unknown fields are ignored. `SlayerQuery` v3 sets `extra="forbid"`, so any unknown field on a future-version query raises a `ValidationError` rather than being silently dropped — this catches typos but means a future schema's new fields will not load on an older SLayer.

The v2→v3 converter drops the legacy `dry_run` / `explain` fields from `SlayerQuery` — they were execution-mode flags that had no business being persisted; pass them as kwargs to `engine.execute(query, dry_run=..., explain=...)` instead. Each migrated query emits one `logger.warning` and one `DeprecationWarning` on first load.

Migrations are defined in `slayer/storage/migrations.py` and apply at the Pydantic-validation layer, so every storage backend (YAML, SQLite, third-party backends registered via `register_storage`, plus HTTP API, MCP server, and dbt importer) gets them automatically.

## Keeping models in sync with the live schema

When the live database schema changes — a column drops, a type bucket flips, a table goes away — persisted models stop being valid. SLayer surfaces this as a first-class concept: `slayer validate-models` returns a structured diff, `slayer ingest` is idempotent (additive only), and query-time DBAPI errors get attributed to the right model with a `SchemaDriftError`. See [Schema Drift](schema-drift.md) for the full diff / cascade contract and the `--force-clean` apply path.
