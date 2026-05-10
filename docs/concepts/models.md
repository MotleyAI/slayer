# Models

A model maps a database table (or SQL subquery) to queryable **columns** and **measures**. Models are defined as YAML files or created via the API/MCP.

In v2 the schema unifies what were previously separate `dimensions` and `measures` lists into a single `columns` list. Each column carries a data type, can be used as a group-by key OR as the input to an aggregation (gated by `allowed_aggregations` and the type/PK eligibility rules), and may carry a `filter` that applies inside CASE-WHEN at aggregation time. The new `measures` list is repurposed to hold **named formulas** — a library of saved metrics queries can reference by bare name.

## YAML Structure

```yaml
name: orders                    # Required: unique model name
description: "Order data"       # Optional — helps agents and users understand the model
sql_table: public.orders        # One of: sql_table or sql
# sql: "SELECT * FROM ..."     # Alternative: custom SQL subquery
data_source: my_postgres        # Required: datasource name
hidden: false                   # Optional: hide from listings
default_time_dimension: created_at  # Optional: default for time-dependent formulas

columns:
  - name: id                    # Required
    description: "Order ID"     # Optional
    sql: "id"                   # SQL expression (bare column name); defaults to name
    type: number                # Required: string, number, boolean, time, date
    primary_key: true           # Optional — restricts aggregation to count/count_distinct
    hidden: false               # Optional

  - name: status
    type: string

  - name: revenue
    description: "Order amount"
    sql: "amount"
    type: number
    allowed_aggregations: [sum, avg]   # Optional whitelist (must be a subset of the type-default eligibility set)

  - name: completed_revenue
    sql: "amount"
    type: number
    filter: "status = 'completed'"     # Applied as CASE WHEN inside aggregation

measures:                       # Optional: library of named formulas
  - name: aov
    description: "Average order value"
    formula: "revenue:sum / *:count"
    label: "AOV"

aggregations:                   # Optional: custom aggregation definitions
  - name: weighted_avg
    formula: "sum({expr} * {weight}) / sum({weight})"
```

## Columns

Each column carries the metadata needed to use it either as a GROUP BY key (a "dimension") or as an aggregation source (a "measure"). The role is decided per query.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique column name within the model. Must not contain `.` |
| `description` | string | No | — | Clarifies meaning for agents and users |
| `label` | string | No | — | Human-readable display name. Propagated to query results and MCP summaries |
| `sql` | string | No | (bare column name) | SQL expression — defaults to the column's name |
| `type` | string | No | `string` | Data type: `string`, `number`, `boolean`, `time`, `date` |
| `primary_key` | bool | No | `false` | Is this a primary key? Restricts aggregation to `count` / `count_distinct` |
| `hidden` | bool | No | `false` | Hide from listings |
| `format` | dict | No | — | Optional `NumberFormat` used by response metadata |
| `allowed_aggregations` | list[str] | No | — | Whitelist of permitted aggregations. Must be a subset of the type-default eligibility set (or be a custom aggregation defined on this model). Validated at model construction time |
| `filter` | string | No | — | SQL condition applied inside CASE-WHEN at aggregation time. See [Filtered Columns](#filtered-columns) below |
| `meta` | dict | No | — | Arbitrary JSON metadata (e.g., `{"source": "CRM", "team": "analytics"}`) |
| `sampled` | string | No | — | DEV-1375: cached sample-value snapshot (distinct values for low-cardinality categorical, `min .. max` for numeric/temporal, `> 20 distinct` for high-cardinality). Auto-populated by `slayer ingest`, `slayer search refresh-samples`, and `inspect_model`'s lazy fill. Read by `inspect_model` and the [search index](search.md). |

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

Cycles in the reference graph (e.g., `c1.sql = "c2 + 1"` and `c2.sql = "c1 - 1"`) are detected at enrichment time and raise a clear `ValueError`. The same expansion is applied to filters and to colon-aggregated measures, so `"B.foo_normalized:sum"` produces `SUM(B.foo_raw / 100.0)`.

### Column Data Types

| Type | Description | SQL Examples |
|------|-------------|--------------|
| `string` | Text values | VARCHAR, TEXT, CHAR |
| `number` | Numeric values | INTEGER, FLOAT, NUMERIC |
| `boolean` | True/false | BOOLEAN |
| `time` | Timestamp | TIMESTAMP, DATETIME |
| `date` | Date only | DATE |

### Aggregation Eligibility

A column with no explicit `allowed_aggregations` whitelist gets a default set based on its data type (`slayer/core/enums.py:DEFAULT_AGGREGATIONS_BY_TYPE`):

| Type | Default eligible aggregations |
|------|-------------------------------|
| `number` | sum, avg, min, max, count, count_distinct, median, weighted_avg, percentile, first, last, stddev_samp, stddev_pop, var_samp, var_pop, corr, covar_samp, covar_pop |
| `string` | count, count_distinct, first, last, min, max |
| `boolean` | count, count_distinct, sum, min, max, first, last |
| `date` / `time` | count, count_distinct, first, last, min, max |

Primary-key columns are always restricted to `count` / `count_distinct` regardless of type.

When `allowed_aggregations` is set, it intersects with the type-default set: every entry must already be eligible under the type-default map (or be a custom aggregation defined on this model). Whitelist entries that violate the type-default or PK rule are rejected at model construction time. This means at query time, a single whitelist-membership check is sufficient — no separate type-default re-check is needed.

### Window functions in `Column.sql`

A column's `sql` may contain a window function (`row_number() over (...)`, `dense_rank() over (...)`, etc.). The column behaves like any other column when used in `dimensions` / SELECT.

> **DEV-1369:** filtering directly on a window-function `Column.sql` from a query (e.g. `{"filters": ["rn <= 3"]}` against a column whose `sql` is `row_number() over (...)`) used to auto-promote to a post-aggregation outer `WHERE`. That escape hatch is removed — the rank-family transforms cover the top-N case in pure DSL, and a query filter naming a windowed column now raises with a clear message. Use `{"filters": ["rank(<measure>) <= 3"]}` (see [formulas.md](formulas.md#rank)) or factor the column into a multi-stage `source_queries` model.

## Measures (Named Formulas)

`SlayerModel.measures` is a library of named formulas. Each measure has the same shape as an inline `SlayerQuery.measures` entry: `{formula, name, label, description}`. Queries can reference them by bare name in any formula context.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `formula` | string | Yes | — | Formula string (e.g., `"revenue:sum / *:count"`, `"cumsum(revenue:sum)"`) |
| `name` | string | No | (auto-derived) | Measure name; queries reference this by bare name |
| `label` | string | No | — | Human-readable display name |
| `description` | string | No | — | Explanatory text |
| `meta` | dict | No | — | Arbitrary JSON metadata for caller bookkeeping (e.g., `{"kb_id": "abc-123"}`) |

Column and measure names share a namespace within a model — a model cannot have a column named `aov` and a measure named `aov` at the same time (validated at save time).

A query can use a saved measure name in any formula position — root, inside a transform, or inside arithmetic:

```json
"measures": [
  {"formula": "aov"},
  {"formula": "cumsum(aov)"},
  {"formula": "aov * 1.1", "name": "aov_with_markup"}
]
```

Bare references are inline-expanded into the saved formula's text at parse time, so the SQL is identical to writing the formula longhand. Saved formulas can reference other saved formulas; cycles (`a → b → a`) are detected and rejected with the chain in the error message. Names that would shadow built-in transforms (`cumsum`, `change`, `change_pct`, `time_shift`, `lag`, `lead`, `rank`, `percent_rank`, `dense_rank`, `ntile`, `first`, `last`) are rejected at model construction time.

### Filtered Columns

A column can have a `filter` — a SQL condition applied via `CASE WHEN` inside an aggregation. Useful for business metrics that apply to a subset of rows:

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

When queried, the filter wraps the column inside the aggregation:
- `active_revenue:sum` generates `SUM(CASE WHEN status = 'active' THEN amount END)`
- `completed_count:count` generates `COUNT(CASE WHEN status = 'completed' THEN id END)`

The filter has no effect when the column is used as a group-by dimension — it only fires inside aggregations.

Filters can reference columns on joined models using dot syntax:

```yaml
joins:
  - target_model: categories
    join_pairs: [["category_id", "id"]]
columns:
  - name: electronics_revenue
    sql: amount
    type: number
    filter: "categories.type = 'electronics'"
```

Multiple filtered and unfiltered columns can coexist in the same query. Filtered columns can be combined in arithmetic formulas:

```json
{"formula": "active_revenue:sum / total_revenue:sum", "name": "active_share"}
```

### Built-in Aggregations

Aggregation is applied at query time via colon syntax: `measure_name:aggregation`. For example, `revenue:sum` means "SUM the `revenue` measure."

| Aggregation | Colon syntax | SQL Generated |
|-------------|-------------|---------------|
| `count` | `*:count` | `COUNT(*)` — counts all rows |
| `count` | `col:count` | `COUNT(col)` — counts non-null values |
| `count_distinct` | `col:count_distinct` | `COUNT(DISTINCT col)` |
| `sum` | `revenue:sum` | `SUM(revenue)` |
| `avg` | `revenue:avg` | `AVG(revenue)` |
| `min` | `revenue:min` | `MIN(revenue)` |
| `max` | `revenue:max` | `MAX(revenue)` |
| `first` | `col:first(time_col)` | Earliest record's value (ordered by `time_col`) |
| `last` | `col:last(time_col)` | Latest record's value (ordered by `time_col`) |
| `weighted_avg` | `price:weighted_avg(weight=quantity)` | `SUM(price * quantity) / SUM(quantity)` |
| `median` | `revenue:median` | Median value |
| `percentile` | `revenue:percentile(p=0.95)` | 95th percentile |
| `stddev_samp` | `latency:stddev_samp` | Sample standard deviation (Bessel-corrected; NULL when N ≤ 1) |
| `stddev_pop` | `latency:stddev_pop` | Population standard deviation (NULL at N=0; 0 at N=1) |
| `var_samp` | `latency:var_samp` | Sample variance (NULL when N ≤ 1) |
| `var_pop` | `latency:var_pop` | Population variance (NULL at N=0; 0 at N=1) |
| `corr` | `price:corr(other=quantity)` | Pearson correlation between two columns; NULL when fewer than 2 non-null pairs OR either side has zero variance |
| `covar_samp` | `price:covar_samp(other=quantity)` | Sample covariance between two columns (Bessel-corrected); NULL when N ≤ 1 |
| `covar_pop` | `price:covar_pop(other=quantity)` | Population covariance between two columns; NULL at N=0, 0 at N=1 |

`*:count` is always available — no measure definition needed. `*` means "all rows" and can **only** be used with `count` (i.e., `*:count` for `COUNT(*)`). Other aggregations like `*:sum` or `*:avg` are not valid.

### The `first` and `last` Aggregations

`first` and `last` return the value from the **earliest or most recent record** within each grouped bucket — like `min`/`max`, but ordered by time instead of value. Useful for snapshot metrics like balances, inventory counts, or status fields where you want the latest state.

```yaml
columns:
  - name: balance
    sql: balance
    type: number
```

At query time, use `balance:last(updated_at)` to get the most recent balance per group, or `balance:first(updated_at)` for the earliest. When grouped by month, each month returns the `balance` value from the latest (or earliest) record in that month. If no time column is specified, the time column for ordering is resolved via: query's `main_time_dimension` → first time/date dimension in the query → first time dimension in filters → model's `default_time_dimension`.

Not to be confused with the [`last()` formula function](formulas.md#last-function), which is a window-function transform that broadcasts a single value across all rows.

### Custom Aggregations

Models can define custom aggregations in the `aggregations` list. Each custom aggregation has a name and a formula template using `{expr}` for the measure expression and named placeholders for kwargs:

```yaml
aggregations:
  - name: weighted_avg
    formula: "sum({expr} * {weight}) / sum({weight})"
  - name: trimmed_mean
    formula: "avg(CASE WHEN {expr} BETWEEN {low} AND {high} THEN {expr} END)"
```

Use at query time: `price:weighted_avg(weight=quantity)`, `revenue:trimmed_mean(low=10, high=1000)`.

Like columns and measures, aggregations also accept an optional `meta` dict for caller bookkeeping (e.g., `{"owner": "analytics"}`).

## SQL Expressions

### In Dimensions and Measures

Use **bare column names** (e.g., `"amount"`) — SLayer automatically qualifies them with the model's table reference at query time.

For complex expressions, use the model name as a table prefix: `"orders.amount * orders.quantity"`.

### SQLite JSON extraction

`json_extract(col, '$.path')` in a `Column.sql` is preserved as the function-call form on SQLite — SLayer does **not** rewrite it to `col -> '$.path'`. The `->` operator in SQLite returns the JSON-quoted form (e.g. `'"Owned"'` with literal quotes), so equality and `CASE WHEN` matches against bare-string literals would silently fail. The function form returns the unquoted scalar.

```yaml
columns:
  - name: tier
    type: string
    sql: "json_extract(payload, '$.tier')"           # works on SQLite (preserved)
  - name: is_gold
    type: number
    sql: "CASE LOWER(json_extract(payload, '$.tier')) WHEN 'gold' THEN 1 ELSE 0 END"
```

If you specifically want the SQLite JSON-scalar operator, write `->>` (`exp.JSONExtractScalar`) directly — SLayer leaves it untouched.

## Joins

Models can declare explicit LEFT JOIN relationships to other models:

```yaml
name: orders
sql_table: public.orders
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
  - target_model: products
    join_pairs: [["product_id", "id"]]
```

Joins enable **cross-model measures** — querying a measure from a joined model alongside the main model's data. See [Cross-Model Measures](queries.md#cross-model-measures).

During [auto-ingestion](ingestion.md), joins are generated automatically from foreign key relationships — one join per FK on the source table. Multi-hop dimensions are auto-resolved at query time by walking the join graph — `customers.regions.name` in a query on `orders` follows `orders → customers → regions` by traversing each intermediate model's own joins.

### Path-Based Table Aliases

Joined tables use `__`-delimited path aliases in generated SQL to disambiguate **diamond joins** — when the same table is reachable via multiple paths. For example, if `orders` joins both `customers` and `warehouses`, each referencing `regions`:

- `customers.regions.name` → table alias `customers__regions`
- `warehouses.regions.name` → table alias `warehouses__regions`

In queries, use dots to denote paths (`customers.regions.name`). In model SQL definitions (dimension/measure `sql` fields), use the `__` alias convention (`customers__regions.name`). See [Diamond Joins](ingestion.md#diamond-joins) for details.

## Model Filters

Models can have always-applied WHERE filters on the underlying table:

```yaml
name: active_orders
sql_table: public.orders
filters:
  - "deleted_at IS NULL"
  - "status <> 'test'"
```

Model filters are SQL-mode expressions (DEV-1369): any valid SQL expression for the underlying dialect is accepted, including function calls (`json_extract`, `coalesce`, `lower`, …), `CASE WHEN`, and joined-column references via the `__` alias syntax. Aggregation colon syntax (`revenue:sum`) and SLayer transform calls (`cumsum`, `change`, …) are rejected — those are DSL constructs and belong in query-level filters or `ModelMeasure.formula`. See [references.md](references.md) for the full Mode A / Mode B table.

Multi-hop joined column references use the `__` alias syntax (e.g., `customers__regions.name`); single-dot `table.column` references are left as-is, and multi-dot input (`customers.regions.name`) is auto-converted to `customers__regions.name` with a warning. The same auto-conversion applies to `Column.sql` and `Column.filter`.

## Source modes

A `SlayerModel` has exactly one **source mode**, set by the field that's populated:

- **`sql_table`** — the model is backed by a physical database table, e.g. `public.orders`.
- **`sql`** — the model is backed by an explicit SQL subquery (a SELECT statement).
- **`source_queries`** — the model is **query-backed**: its rows are the result of one or more `SlayerQuery` stages.

The three are mutually exclusive — exactly one must be populated; others must be empty.

## Query-backed models

A query-backed model is a queryable relation whose rows are the final-stage result of one or more saved `SlayerQuery` stages. You can save any query as a model and then run it directly by name, or use it as `source_model` in another query (just like any table-backed model).

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

This:
- saves the query structure in `model.source_queries`,
- saves any defaults in `model.query_variables`,
- runs save-time validation (any unresolved `{var}` placeholder defaults to `'0'` so SQL generation succeeds),
- caches the resulting `columns` and the rendered `backing_query_sql` on the model for fast inspection.

`create_model_from_query` accepts a single `SlayerQuery` or a list of stages; for multi-stage queries, every non-final stage must have a `name` so it can be referenced. Stages form a DAG: any stage may use a *prior* named sibling as `source_model` or as `joins.target_model`. Forward and self references are rejected with a clear error.

### Two ways to use a saved query

**Run the backing query directly by name** — returns the final-stage result:

```python
await engine.execute("monthly_revenue", variables={"region": "US"})
```

**Use the saved result as a model in another query**:

```python
{
    "source_model": "monthly_revenue",
    "measures": [{"formula": "amount_sum:avg"}],
    "dimensions": ["region"],
}
```

### Variable precedence

When a query-backed model references `{var}` placeholders, values flow in this order (highest precedence first):

1. **Runtime kwarg** — the `variables=` argument to `engine.execute(...)` (also exposed by REST `/query`, MCP `query`/`create_model`, and CLI `--variables` / `--variables-json`). Wins at every nesting level.
2. **Stage `.variables`** — set on an individual `SlayerQuery` stage.
3. **Outer query `.variables`** — when a query-backed model is used as `source_model` in another query.
4. **Model defaults** — `model.query_variables`.

Unresolved placeholders raise a clear error at execute time, naming the model and the missing variable. Variables in the runtime kwarg that don't appear anywhere are silently ignored.

### What gets cached

For a query-backed model, the engine caches:

- `model.columns` — the final-stage output columns (a discoverability snapshot).
- `model.backing_query_sql` — the rendered backing-query SQL.

The cache is populated **only** when the model is saved via `engine.save_model` (also reached by REST `POST/PUT /models` and MCP `create_model` / `edit_model`). **Read operations do not write to storage** — `engine.execute`, `inspect_model`, `get_column_types`, MCP `query`, and REST `/query` will never modify the persisted cache. If a query-backed model is written directly to storage outside `engine.save_model` (bypassing the engine), `model.columns` and `model.backing_query_sql` will remain stale until the next save through the engine. Inspect tools (`inspect_model`, `models_summary`, REST `GET /models/{name}`) read the cache directly.

You **cannot** supply `columns` or `backing_query_sql` yourself when creating a query-backed model — they're engine-managed, and any user-supplied value is rejected at save with a clear error.

Via MCP, use `create_model` with `query=` (and optional `variables=`); via REST, `POST /models` with `source_queries`; via CLI, `slayer models create model.yaml` where the YAML has a `source_queries` field.

### Column naming in query-derived models

A query result is a self-contained table — it no longer has the joins that the source model may have had. Dimensions and measures that came from joined models use `__` to encode the original join path in their name:

| Inner query field | Virtual model column name |
|----------------------|--------------------------|
| `stores.name` | `stores__name` |
| `customers.regions.name` | `customers__regions__name` |
| `customer_id` | `customer_id` |
| `*:count` (measure) | `count` |
| `revenue:sum` (measure) | `revenue_sum` |
| `{"formula": "revenue:sum", "name": "rev"}` | `rev` |

This uses the same `__` convention as SQL-level join path aliases. When referencing these columns in an outer query, use the `__` name directly (e.g., `{"name": "stores__name"}`), not dot syntax — dots would imply a join to a model that doesn't exist on the virtual table.

An explicit `name` on a measure spec **overrides** the canonical naming above for both arithmetic/transform formulas and simple aggregations. This is what lets multi-stage `source_queries` rename inner-stage outputs cleanly:

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

The inner stage emits a column named `rev` (not `amount_sum`), and the outer stage references it by that chosen name.

See the [multistage queries example](../examples/06_multistage_queries/multistage_queries.md) for working examples.

## Model Fields Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `version` | int | No | `3` | Schema version stamp (see [Schema versioning](#schema-versioning)) |
| `name` | string | Yes | — | Unique model name |
| `sql_table` | string | One of | — | Database table (e.g. `public.orders`) |
| `sql` | string | these | — | Custom SQL subquery |
| `source_queries` | list[SlayerQuery] | three | — | Saved query stages — makes the model **query-backed**. Multi-stage queries: every non-final stage must have a `name`. |
| `data_source` | string | Yes | — | Datasource name |
| `columns` | list | No | `[]` | Column definitions (`Column`). For query-backed models this is an **engine-managed cache** auto-derived from the backing query — supplying it on save raises a clear error. |
| `measures` | list | No | `[]` | Library of named formulas (`ModelMeasure`) — referenced by bare name in queries |
| `joins` | list | No | `[]` | JOIN relationships to other models |
| `filters` | list[str] | No | `[]` | Model-level WHERE filters (always applied, e.g., `"deleted_at IS NULL"`) |
| `query_variables` | dict | No | `{}` | Default values for `{var}` placeholders in `source_queries`. Lowest layer of the variable-precedence stack (see [Variable precedence](#variable-precedence)). Only meaningful for query-backed models. |
| `backing_query_sql` | string | No | — | Engine-managed cache of the rendered backing query (canonical placeholder-fill render). Read-only; user-supplied values are rejected at save. |
| `description` | string | No | — | Helps agents and users understand the model |
| `hidden` | bool | No | `false` | Hide from model listings |
| `default_time_dimension` | string | No | — | Default time dimension name for time-dependent formulas (e.g. `"created_at"`) |
| `meta` | dict | No | — | Arbitrary JSON metadata (e.g., `{"owner": "analytics", "version": 2}`) |

## Schema versioning

Every persisted SLayer entity (`SlayerModel`, `SlayerQuery`, `DatasourceConfig`) carries a `version: int` field that records the schema it was written against. The current schema is `3` for `SlayerModel` and `SlayerQuery`, and `1` for `DatasourceConfig`.

```yaml
version: 3
name: orders
sql_table: public.orders
...
```

Behaviour:

- **On save**, SLayer always writes the current schema version. New `SlayerModel` and `SlayerQuery` objects default `version` to `3`; new `DatasourceConfig` objects default to `1`.
- **On load**, if the file's version is older than the current schema, SLayer runs a chain of pure dict→dict converters before Pydantic validates the data. This means hand-edited or older files keep working when the schema evolves.
- **Forward tolerance.** A file with a higher `version` than this SLayer knows about loads on a best-effort basis. For `SlayerModel` and `DatasourceConfig`, unknown fields are ignored. `SlayerQuery` v3 sets `extra="forbid"`, so any unknown field on a future-version query raises a `ValidationError` rather than being silently dropped — this catches typos but means a future schema's new fields will not load on an older SLayer.
- **Round-tripping** an older file (load → save) upgrades it on disk to the current schema.

The v2→v3 converter (in `slayer/storage/v3_migration.py`) drops the legacy `dry_run` and `explain` fields from `SlayerQuery` — they were execution-mode flags that had no business being persisted. Pass them as kwargs to `engine.execute(query, dry_run=..., explain=...)` instead. Each migrated query emits one `logger.warning` and one `DeprecationWarning` on first load. `SlayerQuery` v3 is also strict (`extra="forbid"`), so unknown fields raise a `ValidationError`.

Migrations are defined in `slayer/storage/migrations.py` and apply at the Pydantic-validation layer, so every storage backend (YAML, SQLite, third-party backends registered via `register_storage`, plus the HTTP API, MCP server, and dbt importer) gets them automatically.

## Result Column Format

Query results use `model_name.column_name` format for column keys. Colon syntax in field names is converted: `revenue:sum` becomes `orders.revenue_sum`, and `*:count` becomes `orders._count` (the leading underscore is kept so the alias never collides with a user-defined column literally named `count`). For multi-hop joined dimensions, the full path is included:

```json
{"orders.status": "completed", "orders._count": 42, "orders.revenue_sum": 1500}
{"orders.customers.regions.name": "US", "orders._count": 3}
```

## Keeping models in sync with the live schema

When the live database schema changes (a column drops, a type bucket flips, a table goes away), persisted models stop being valid. SLayer surfaces this as a first-class concept — `slayer validate-models` returns a structured diff, `slayer ingest` is idempotent (additive only), and query-time DBAPI errors get attributed to the right model with a `SchemaDriftError`. See [Schema Drift](schema-drift.md) for the full diff / cascade contract and the `--force-clean` apply path.
