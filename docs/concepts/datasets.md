# Datasets

A dataset is SLayer's view of a database table or an underlying SQL query. It declares the fields, named metric expressions, relationships, and always-applied filters that queries can build on. Datasets are defined as YAML (one file per dataset under `models/<data_source>/`) or created via API/MCP — the two paths produce the same persisted object. A dataset can also be declared in a [semantic model](semantic-models.md), an Ossie document that holds several datasets, their relationships and model-scoped metrics; it then works exactly like a standalone dataset, and the dataset tools edit it inside its document.

A tiny example to anchor what follows:

```yaml
name: orders
sql_table: public.orders
data_source: my_postgres
fields:
  - {name: id, datatype: Integer, primary_key: true}
  - {name: status, datatype: String}
  - {name: revenue, expression: amount, datatype: Decimal}
metrics:
  - {name: aov, expression: "sum(revenue) / count(*)"}
```

A query then asks for `sum(orders.revenue)` (aggregate the `revenue` field), `orders.aov` (the saved expression), or `orders.status` (group by it). Same dataset, different roles per query.

## Fields at a glance

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique dataset name, across standalone datasets and [semantic models](semantic-models.md#one-namespace-per-datasource) of the datasource. In the default YAML storage, names differing only by letter case (`Orders` vs `orders`) are rejected at save time (`IdCollisionError`) — they would collide as filenames on macOS / Windows |
| `sql_table` | string | One of | A physical database table or view (e.g. `public.orders`) |
| `sql` | string | these | A SQL subquery to use as the source |
| `source_queries` | list[SlayerQuery] | three | Saved query stages — makes the dataset **query-backed** |
| `source_kind` | string | No | What kind of object `sql_table` names: `table`, `view`, or `materialized_view`. Set by auto-ingestion; `null` means unknown |
| `data_source` | string | Yes | Datasource name |
| `fields` | list[DatasetField] | No | Field definitions. For query-backed datasets this is an engine-managed cache |
| `metrics` | list[DatasetMetric] | No | Named expression library — referenced as `<dataset>.<metric>` in queries |
| `aggregations` | list[Aggregation] | No | Custom aggregation operators |
| `relationships` | list[DatasetRelationship] | No | LEFT JOIN relationships to other datasets |
| `filters` | list[str] | No | Dataset-level WHERE filters (always applied) |
| `default_time_dimension` | string | No | Default time dim for time-dependent expressions; when unset, a dataset's only DATE / TIMESTAMP field serves, and it is the dataset's [time spine](time.md#time-spine) axis; saving a dataset whose declared default isn't a DATE / TIMESTAMP field fails |
| `query_variables` | dict | No | Defaults for `{var}` placeholders (query-backed datasets only) |
| `description` | string | No | Helps agents and users understand the dataset |
| `hidden` | bool | No | Hide from listings (still queryable by name and joinable). Set automatically at ingest for recognised ELT/migration internals — see [Recognised internals](../reference/cli.md#recognised-internals) |
| `access_tags` | list[str] | No | Access groups: through an [access-filtered store](../configuration/storage.md#access-filtered-storage) a caller sees the dataset only if it is untagged or shares a tag, else it reads as deleted (unlike `hidden`, which curates listings for everyone) |
| `meta` | dict | No | Arbitrary JSON metadata for caller bookkeeping. Ingestion writes `internal_table: <tool>` on auto-hidden internals |
| `version` | int | No | Schema version stamp (currently `19`) |

## Source modes

A dataset has exactly one source — set by one of three mutually exclusive fields:

- **`sql_table`** — a physical database table or view.
- **`sql`** — an explicit SQL subquery (a `SELECT` statement). Useful when the dataset's underlying shape requires cleaning or joining beyond what SLayer expresses natively.
- **`source_queries`** — one or more saved `SlayerQuery` stages. Makes the dataset **query-backed**: see [Query-backed datasets](#query-backed-datasets).

Validators reject empty `source_queries=[]`, multiple sources, or missing names on non-final stages.

`sql_table` may name a **view** as well as a table — auto-ingestion creates
datasets for views by default and records which it was in `source_kind`. Views
expose no primary key and no foreign keys, so a view-backed dataset has no
primary-key field and no auto-generated relationships; `source_kind` is what tells you
that re-ingesting will never produce them. A re-ingest refreshes `source_kind`, so a
view later rebuilt as a table (dbt's `+materialized: table`) is picked up.

Dataset names may contain `__`; it is matched as part of the exact name, not a
relationship-path delimiter (dots are the only path separator). Auto-ingestion preserves
the faithful object name: an object named `reports__patient__drug` becomes a
dataset named `reports__patient__drug`, and `a__b` and `a_b` are distinct datasets.
Only the `__slayer_` prefix is reserved for SLayer-internal identifiers.

## Fields

A field is the unit of structure on the dataset. The same field entry can serve as a group-by key in one query and as input to an aggregation in another — the role is decided per query, not declared up front. A [list field](filters.md#list-fields) is the exception. What the field *carries* is its identity (name), how to compute it from the source (`expression`), what data type to expect (`datatype`), and a handful of policy attributes (which aggregations are allowed, whether it's a primary key, whether it's hidden).

### Attributes

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique within the dataset. Must not contain `.` or `:`; the `__slayer_` prefix is reserved. `__` is otherwise allowed |
| `description` | string | No | — | Clarifies meaning for agents and users |
| `label` | string | No | — | Display name; propagates into query result metadata |
| `expression` | string or object | No | (the source column of the field's name) | The definition: portable SQL as a string (shorthand for `{sql: …}`), or Ossie's `{sql, dialects: [{dialect, expression}]}`. See [Field expressions](#field-expressions) |
| `sql` | string | No | — | Deprecated spelling of `expression`, converted on load; removed in 3.0 |
| `datatype` | string | No | `String` | One of the [data types](#data-types), matched by exact case. A semantic-model field that declares none has an [unknown type](semantic-models.md#fields-without-a-datatype), read from the live column at query time |
| `element_type` | string | With `Array` on a table-backed dataset | `Opaque` on an `sql`-mode dataset's `Array` | A list's element type (any data type but `Array`; `Opaque` for nested lists and records, which support only NULL tests and `count`); rejected on any other type |
| `db_type` | string | No | — | The raw database type, recorded by ingestion where `datatype` loses information: `Opaque`, exact `NUMERIC` / `DECIMAL`, JSON, zoned timestamps and lists |
| `primary_key` | bool | No | `false` | A dataset's sole primary key is an identifier, restricted to counting, `min` / `max` and `collect`; members of a composite key are ordinary fields |
| `unique` | bool | No | `false` | Single-field uniqueness (non-PK). A sole `primary_key` implies unique; a composite-key member does not. Auto-set from `UNIQUE` constraints / unique indexes; used to infer one-to-one relationships |
| `hidden` | bool | No | `false` | Hide from listings |
| `format` | dict | No | — | `NumberFormat` used by response metadata |
| `allowed_aggregations` | list[str] | No | — | Whitelist (must be a subset of the type-default eligibility set, or a custom aggregation defined on this dataset) |
| `filter` | string | No | — | SQL condition wrapping the field value in `CASE WHEN` — a value mask that fires in every position. See [Filtered fields](#filtered-fields) |
| `granularity` | string | No | — | Time bucket a temporal field is already truncated to (`month`, `year`, …); a finer or non-nesting time dimension over it is a typed error. Query-backed datasets stamp it automatically; set it by hand only when the values are truly bucketed at that grain |
| `time_zone` | string | No | — | The zone a `DateTime` column's values are stored in: an IANA name, or `civil` for zone-free time never converted; overrides the datasource's `time_zone`, see [time zones](time.md#time-zones) |
| `meta` | dict | No | — | Arbitrary JSON metadata |
| `sampled` | string | No | — | Cached sample-value text snapshot (top-20 by frequency joined, or `top20 ... (50+ distinct)` on overflow, or `min .. max` for numeric/temporal); populated lazily on the first `inspect` of the field (or via `slayer datasources refresh-samples`), not at ingest time |
| `sampled_values` | list[str] | No | — | Structured top-50-by-frequency list (categorical values only); the unambiguous counterpart to `sampled` for consumers that need to compare predicate literals against stored values. `None` for numeric/temporal fields |
| `distinct_count` | int | No | — | Exact distinct count when ≤ 50 (categorical only). `None` on overflow (> 50 distinct — one scan only, no secondary `count_distinct` query) and for numeric/temporal fields |

A list field is profiled over its non-NULL elements: `sampled_values` and `distinct_count` for `String` / `Boolean` elements, a `min .. max` range for numeric and temporal ones; a whole list is never sampled, and a list with `Opaque` elements is never profiled.

A JSON function reads a field over a native JSON column (Postgres `json` / `jsonb`, `JSON`, Snowflake `VARIANT`) in its native form only when its `db_type` is recorded; a field without one is read as text.

A field's entry for its datasource's dialect is rendered into the executed statement, which reaches the database verbatim — `:name` is never read as a bind parameter, nor `%` as a format directive — so regex literals (`(?:…)`) and date formats (`%Y-%m`) pass through unchanged.

### Data types

Data types are Ossie's datatype names plus `Array`, stored and shown as written.

| Type | Description | SQL examples |
|------|-------------|--------------|
| `String` | Text values | VARCHAR, TEXT, CHAR |
| `Integer` | Integers | INTEGER, BIGINT, SMALLINT |
| `Decimal` | Exact decimal numbers; behave as `Float` | NUMERIC, DECIMAL |
| `Float` | Floating-point numbers | FLOAT, DOUBLE, REAL |
| `Boolean` | True/false | BOOLEAN |
| `Date` | Date only | DATE |
| `DateTime` | Date and time | TIMESTAMP, DATETIME |
| `DateTimeTz` | An instant; behaves as `DateTime`, and under a query [time zone](time.md#time-zones) reads as that zone's wall clock | TIMESTAMPTZ, TIMESTAMP WITH TIME ZONE, BigQuery / MySQL `TIMESTAMP`, ClickHouse `DateTime('UTC')` |
| `Time` | Time of day; opaque, like `Opaque` | TIME |
| `Opaque` | Stored and displayed, never grouped, aggregated or cast | Postgres `point`, `json`, range types |
| `Array` | A list of `element_type` values, see [list fields](filters.md#list-fields); re-ingest re-types a `String` field whose live type is a list, keeping its other attributes | `TEXT[]`, BigQuery `REPEATED STRING`, `Array(String)` |

The pre-2.0 values `TEXT`, `INT`, `BOOLEAN`, `DATE`, `UNKNOWN` and `ARRAY` (and their lowercase spellings) still convert, with a deprecation; `DOUBLE` becomes `Decimal` when the field's `db_type` is an exact numeric, else `Float`, and `TIMESTAMP` becomes `DateTimeTz` when the `db_type` carries a time zone, else `DateTime`.

### Aggregation eligibility

A field with no explicit `allowed_aggregations` whitelist gets a default set based on its data type. The defaults are deliberately liberal for numerics and strict for the rest, so an agent doesn't accidentally `SUM` a string field.

| Type | Default eligible aggregations |
|------|-------------------------------|
| `Integer` / `Decimal` / `Float` | sum, avg, min, max, count, count_distinct, count_distinct_approx, median, weighted_avg, percentile, first, last, stddev_samp, stddev_pop, var_samp, var_pop, corr, covar_samp, covar_pop, collect |
| `String` | count, count_distinct, count_distinct_approx, first, last, min, max, collect |
| `Boolean` | the `Integer` set |
| `Date` / `DateTime` / `DateTimeTz` | count, count_distinct, count_distinct_approx, first, last, min, max, collect |
| `Array` | count (non-NULL lists), count_distinct (distinct element sets) |
| `Time` / `Opaque` | none |

A boolean is its integer (true 1, false 0, NULL ignored) inside every numeric aggregation, so `sum` counts the true rows, `avg` is their share (formatted as a percentage), and `min` / `max` stay boolean.

`count_distinct_approx` is dialect-aware: it emits the database-native approximate-distinct function where one exists and falls back to an exact `COUNT(DISTINCT)` where it does not (Postgres / SQLite / MySQL). A dataset's sole primary-key field is always restricted to `count` / `count_distinct` / `count_distinct_approx` / `min` / `max` / `collect` regardless of type, and is skipped by sampling and profiling; composite primary-key members aggregate by their type. When `allowed_aggregations` is set, every entry must already be eligible under the type-default map (or be a custom aggregation defined on this dataset); violations are caught at dataset construction time, so query-time validation is a single membership check. On a list field it may name only `count` and `count_distinct`, and a custom aggregation over a list is rejected.

### Filtered fields

A field can carry a `filter` — a SQL condition applied as a `CASE WHEN` value mask wherever the field is read (NULL on non-matching rows, so an aggregation over it covers just the matching rows). This is how you express business metrics that apply to a row subset without a separate dataset:

```yaml
fields:
  - name: active_revenue
    expression: amount
    datatype: Float
    filter: "status = 'active'"
  - name: completed_count
    expression: id
    datatype: Integer
    filter: "status = 'completed'"
```

`sum(orders.active_revenue)` then generates `SUM(CASE WHEN status = 'active' THEN amount END)`. The filter is pure syntactic sugar for `CASE WHEN <filter> THEN <value> END`, a **value mask** that fires in *every* position: as a group-by dimension, rows where the filter is false fall into the `NULL` group; a `first`/`last` picks the masked value at the chosen row (`NULL` if it does not match). It never removes rows or changes which row is picked — a genuine row restriction belongs in a query `filter`.

A bare name in a filter reads the source column and a path (`orders.status`, `categories.type`) reads a field. Filtered and unfiltered fields coexist freely in the same query and combine cleanly in arithmetic expressions (e.g. `"sum(orders.active_revenue) / sum(orders.total_revenue)"`).

### Field expressions

A field's `expression` is Ossie's expression object, or a string standing for its portable `sql`:

```yaml
fields:
  - {name: amount_usd, expression: "amount / 100.0"}
  - name: amount_rounded
    expression:
      sql: "ROUND(amount, 2)"
      dialects:
        - {dialect: POSTGRES, expression: "round(amount::numeric, 2)"}
```

- **Which form runs**: the entry for the datasource's own dialect (its type's SQL dialect name, upper-cased: `POSTGRES`, `DUCKDB`, `SQLITE`, …), else the portable `sql`, else an `OSSIE_SQL_2026` entry. Entries for other dialects are kept and never run; a field with two entries for its datasource's dialect, or with no form that runs there, is refused at save.
- **Portable `sql`** is read by SLayer's Mode B grammar with [Ossie's meaning](expressions.md); a dialect entry reaches the database verbatim.
- **Names**: inside a field's `expression` or `filter`, and a dataset's `filters`, a bare name or `source.x` reads the dataset's **source column**, and a path from the dataset reads a **field** (`orders.gross`, `customers.region`). So a derived field may take a source column's name (`status: UPPER(status)` upper-cases the physical `status`).
- **Physical reader**: a field whose running form is one bare source column and which has no `filter`; schema drift and live typing read only these.
- The pre-2.0 `sql` converts to `expression`: one column becomes the shorthand, other text becomes the entry for the datasource's dialect with each sibling it read written as a path, so its results do not change.

### Derived fields referencing other fields

A field reads another field of its own dataset, or of a joined one, by path; the engine inlines the referenced definitions at query time, so chains stay DRY.

```yaml
# Dataset: stations
fields:
  - {name: foo_raw, datatype: Float}
  - {name: foo_normalized, expression: "foo_raw / 100.0", datatype: Float}   # foo_raw: the source column
# Dataset: telescopes — joined to stations
fields:
  - name: aoi_ratio
    expression: "aperture / stations.foo_normalized"
    datatype: Float
relationships:
  - to: stations
    from_columns: ["station_id"]
    to_columns: ["id"]
```

At query time, `aoi_ratio` expands to `telescopes.aperture / (stations.foo_raw / 100.0)`. A same-dataset field is read as `stations.foo_normalized` from `stations`; a multi-hop path is dotted (`B.C.x_derived`).

A derived field whose definition (recursively) crosses a fanning (not provably to-one) hop fails closed with a typed error when used as an aggregate input or an `error`-mode dimension — declare the [relationship cardinality](#relationship-cardinality) or primary key to prove the hop; as a dimension it may instead be queried under the default (associating) handling. As a population filter it restricts by association in every mode. A derived field whose definition reaches a to-many (or undeclared) target cannot be aggregated as a field of its dataset — aggregate the target field directly (`sum(line_items.qty)`). Where the hop is *provably* fanning (declared `one_to_many`/`many_to_many`), the dataset is rejected at **save time**; an unproven hop saves with a warning and relies on the query-time check above. A definition whose relationship path **revisits** a dataset already on it (a cycle, e.g. `regions.customers.spend` declared on `customers`) is rejected at **save time** as a circular definition — reference the field on the revisited dataset directly, or aggregate on the dataset the hop leaves. A definition crossing an ambiguous hop (two or more edges between the pair and no edge name in the path) is also rejected at **save time** — spell that hop by an edge name.

Names inside a nested scope of a dialect entry (sub-query, `UNION` branch, CTE, `VALUES`) belong to the inner rowset and are kept as written.

Cycles in the reference graph (e.g., `c1: "orders.c2 + 1"` and `c2: "orders.c1 - 1"`) are rejected at `save_dataset` time and raise `DatasetFieldCycleError` (which subclasses both `SlayerError` and `ValueError`) with the cycle path in the message. Save-time validation stays within the dataset's `data_source`; unresolved cross-datasource refs are silently skipped. The same expansion is applied to filters and to aggregated metrics, so `"sum(B.foo_normalized)"` produces `SUM(B.foo_raw / 100.0)`.

### Window functions in a field's expression

A field whose dialect entry contains a window function (`row_number() over (...)`, …) is computed over its dataset's filtered rows before any join, query filter or aggregation, then read as a column in every position — so a dataset filter reading its own dataset's windowed field is a filter cycle, and a window in a field's portable `sql` is refused at save.

### SQLite JSON extraction

`json_extract(col, '$.path')` in a field's `SQLITE` entry is preserved verbatim — SLayer does **not** rewrite it to `col -> '$.path'`. The `->` operator in SQLite returns the JSON-*quoted* form (`'"Owned"'`, with literal quotes), which silently breaks equality and `CASE WHEN` matches against bare-string literals. The function form returns the unquoted scalar.

```yaml
fields:
  - name: tier
    datatype: String
    expression: {dialects: [{dialect: SQLITE, expression: "json_extract(payload, '$.tier')"}]}
  - name: is_gold
    datatype: Integer
    expression:
      dialects:
        - {dialect: SQLITE, expression: "CASE LOWER(json_extract(payload, '$.tier')) WHEN 'gold' THEN 1 ELSE 0 END"}
```

If you specifically want SQLite's JSON-scalar operator, write `->>` (`exp.JSONExtractScalar`) directly — SLayer leaves it untouched.

## Metrics vs aggregations — the disambiguation

SLayer has two lists on a dataset that both relate to metrics, and the names don't make the difference obvious. The split is real and load-bearing:

- **`metrics`** is a library of named **expressions** — saved expressions like `aov = sum(revenue) / count(*)`. Queries reference them by qualified name (`orders.aov`) and the expression expands inline. Think *what to compute*, at the metric level.
- **`aggregations`** is a registry of custom **operators** — definitions like `trimmed_mean(p)` or `weighted_avg(weight=…)`. Once defined, they become usable inside any expression: `trimmed_mean(orders.revenue, p=0.1)`. Think *how to aggregate*, at the operator level.

A typical dataset uses zero or a few entries in each. They compose:

```yaml
aggregations:
  - name: trimmed_mean
    expression: "AVG(CASE WHEN {value} BETWEEN {low} AND {high} THEN {value} END)"

metrics:
  - name: clean_aov
    expression: "trimmed_mean(revenue, low=0, high=1e6) / count(*)"
```

## Metrics (named expressions)

A metric is a saved expression. Its shape is identical to an inline `SlayerQuery.metrics` entry; the only difference is scope — dataset-level metrics are reusable across queries.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `expression` | string | Yes | e.g. `"sum(revenue) / count(*)"`, `"cumsum(sum(revenue))"` |
| `name` | string | No | Queries reference this as `<dataset>.<name>` (auto-derived if omitted) |
| `label` | string | No | Display name |
| `description` | string | No | Explanatory text |
| `datatype` | DataType | No | Declares the expression's result type (drives outer CAST) |
| `meta` | dict | No | Arbitrary JSON metadata |

An inferred integer metric type describes the result without narrowing the
database's native integer range. For example, `"sum(amount)"` can return a total
larger than a 32-bit integer even when each source value fits in one. An explicit
metric `"datatype": "Integer"` still requests the database's INT cast and can reject
out-of-range results. Auto-ingested NUMERIC/DECIMAL fields likewise retain the
database's exact aggregate type instead of being coerced through floating point
(except on SQLite, whose numeric affinity has no exact decimal type to retain).
An explicit metric `"datatype": "Float"` still requests a floating-point cast.
Other type casts, including declared derived-field types, are unchanged.

Field and metric names share a namespace within a dataset — you can't have a field `aov` *and* a metric `aov`. A metric can use any other metric of its dataset by bare name, and a query by its qualified name, including inside transforms and arithmetic:

```json
"metrics": [
  "orders.aov",
  "cumsum(orders.aov)",
  {"metric": "orders.aov * 1.1", "alias": "aov_with_markup"}
]
```

A saved expression produces the same SQL as writing it out longhand. Cycles (`a → b → a`) are rejected during binding. Names shadowing built-in transforms are rejected at dataset construction.

`AGG(m)` evaluates a metric as one aggregate, with the same values as `m`: inside a metric's expression `AGG(aov)` or `AGG(customers.aov)`, in a query `AGG(orders.aov)`. Its argument must be a single metric; `AGG` of a field or any other expression is refused. Metrics of a [semantic model](semantic-models.md#model-scoped-metrics) as a whole, queried by bare name, are model-scoped metrics.

### Reusing another dataset's saved metric (`customers.aov`)

A dotted `"customers.aov"` reuses a saved metric from the joined `customers` dataset as if its expression were written inline, and a query-backed dataset cannot declare `metrics` directly. A saved metric whose expression crosses back to a dataset already on the path (e.g. `customers.order_total` defined as `sum(customer.amount)`, used from `orders`) evaluates over its own occurrence of that dataset rather than failing.

See [expressions.md](expressions.md) for the full expression grammar — operators, transforms, time-shifted forms, and the `Mode B` DSL rules.

## Aggregations

Aggregations are the operators that turn a field expression into a value: `sum`, `avg`, `percentile(p=…)`, and so on. They're applied at query time as a function call — `aggregation(field)` — and the same operator works on any compatible field.

### Built-in aggregations

| Aggregation | Syntax | SQL |
|-------------|--------------|-----|
| `count` | `count(orders.*)` | `COUNT(*)` — counts all rows |
| `count` | `count(col)` | `COUNT(col)` — counts non-null values |
| `count_distinct` | `count_distinct(col)` | `COUNT(DISTINCT col)` |
| `sum` | `sum(orders.revenue)` | `SUM(revenue)` |
| `avg` | `avg(orders.revenue)` | `AVG(revenue)` |
| `min` / `max` | `min(orders.revenue)` | `MIN(revenue)` / `MAX(revenue)` |
| `first` / `last` | `first(col, time_col)` | Earliest / latest record's value, time-ordered |
| `weighted_avg` | `weighted_avg(orders.price, weight=orders.quantity)` | `SUM(price * quantity) / SUM(quantity)` |
| `median` | `median(orders.revenue)` | Median value |
| `percentile` | `percentile(orders.revenue, p=0.95)` | 95th percentile |
| `stddev_samp` / `stddev_pop` | `stddev_samp(orders.latency)` | Sample / population standard deviation |
| `var_samp` / `var_pop` | `var_samp(orders.latency)` | Sample / population variance |
| `corr` | `corr(orders.price, other=orders.quantity)` | Pearson correlation between two fields |
| `covar_samp` / `covar_pop` | `covar_samp(orders.price, other=orders.quantity)` | Sample / population covariance |
| `collect` | `collect(customers.name)` | The values as a list, see [list fields](filters.md#list-fields) |

`count(orders.*)` is always available with no metric definition. `*` means "all rows" and is **only** valid with `count` — `sum(orders.*)` and friends are rejected. Detailed NULL / N=1 semantics for the statistical aggregations are documented in [database-support.md](../database-support.md).

### The `first` and `last` aggregations

`first` and `last` return the value from the **earliest or most recent record** within each grouped bucket — like `min`/`max`, but ordered by time instead of value. Useful for snapshot metrics like balances, inventory counts, or status fields where you want the latest state:

```yaml
fields:
  - {name: balance, datatype: Float}
```

`last(accounts.balance, accounts.updated_at)` gives the most recent balance per group; `first(accounts.balance, accounts.updated_at)` the earliest. A row whose ranking key is NULL is never picked, on every database; a group whose keys are all NULL returns NULL. When grouped by month, each month returns the latest (or earliest) record's balance in that month. If no time field is specified, ordering resolves via: the first DATE / TIMESTAMP row-level dimension in the query → the first time dimension's untruncated field or expression → the dataset's `default_time_dimension` (its only DATE / TIMESTAMP field when unset). Whichever field wins is an input of the aggregation: if it, or a derived definition it names, crosses a relationship hop that is not provably to-one from the aggregation's root, the query fails with the input-safety error exactly as an explicit ranking argument would. The value and the ranking key may each be a row-level expression (`first(orders.amount - customers.discount)`, `last(accounts.balance, coalesce(accounts.updated_at, accounts.created_at))`), and a computed dimension typed DATE / TIMESTAMP counts as a DATE / TIMESTAMP dimension in that order.

The ranking key is consumed: when it is itself one of the query's dimensions (or named in `partition_by=`), the aggregation returns one value per coarser cell — grouped by `accounts.customer_id` and the raw `accounts.updated_at`, `last(accounts.balance, accounts.updated_at)` is each customer's latest balance on every row. Only an exact match is consumed: a raw date key never drops its month bucket.

Over an aggregate, `first` / `last` rank that aggregate's cells — see [First and last over an aggregate](expressions.md#first-and-last-over-an-aggregate).

## Custom aggregations

Add your own operators via the `aggregations` list. Each entry has a name and a SQL expression template using `{value}` for the aggregated expression and named placeholders for kwargs:

```yaml
aggregations:
  - name: weighted_avg
    expression: "sum({value} * {weight}) / sum({weight})"
  - name: trimmed_mean
    expression: "avg(CASE WHEN {value} BETWEEN {low} AND {high} THEN {value} END)"
```

Use at query time: `weighted_avg(orders.price, weight=orders.quantity)`, `trimmed_mean(orders.revenue, low=10, high=1000)`. An aggregation entry can also override a built-in's default parameters without redefining the SQL, or give a built-in other than `first`/`last` an `expression`, which then replaces its SQL. Like fields and metrics, aggregations accept an optional `meta` dict for caller bookkeeping. An expression that does not parse as SQL is rejected when the dataset is saved, and so is a parameter whose default `sql` is empty, whose name is `value` (which always stands for the aggregated field) or `window` (the trailing-window argument), or that the expression never references; a query argument the aggregation does not accept is rejected, listing the accepted names. A built-in aggregation's name, in any case, resolves only to an aggregation named exactly that built-in's lowercase name.

An aggregation may take the name of a library function or function spelling (`reverse`, `left`, `strpos`, …): inside its own dataset every call over that dataset's fields — `reverse(a)`, `t.reverse(a)`, in any letter case — is the aggregation, the library function stays reachable everywhere else (another dataset's fields, a literal argument), and saving, loading and every query response that resolves through it carry a `library_name_shadow` warning; transform, date-part and granularity names stay reserved.

An aggregation rendered from an `expression` (a custom one, or a built-in it overrides) is typed by its optional `datatype` (any field type; a declared `Array` has `Opaque` elements), else by its expression's SQL with each placeholder typed by its argument in that call (a boolean in arithmetic or `SUM` reads as its integer), else, when that cannot be inferred (an unknown function), by its input's type: `COUNT(CASE WHEN {value} THEN 1 END)` is `Integer`, `AVG({value})` over an `Integer` is `Float`, and `MY_UDF({value})` over a `Date` is `Date`. The query plan, later stages and the SQL interface report the same type, and an invalid `datatype` is refused when the dataset is saved.

A placeholder inside a quoted string literal (`SUM({value}) * CAST('{n}' AS DOUBLE)`) splices its parameter's value and so needs a literal one, a number or a string; write a literal brace there as `{{` or `}}`.

A parameter default resolves from the dataset that declares the aggregation, and a qualifier naming a dataset already on the query's path to it reads that row rather than re-joining (`weight: customers.spend` on a `regions` aggregation queried as `weighted_avg(customers.regions.pop)` weights by that customer's own spend), while any other qualifier the declaring dataset cannot reach resolves from the query root.

A default behaves exactly like the same value passed explicitly, and the two spellings in one query are computed once. Default text that does not parse, or that contains an aggregate, a window function or a subquery, fails when a query uses the aggregation.

## Relationships

Datasets declare explicit LEFT JOIN relationships to other datasets:

```yaml
name: orders
sql_table: public.orders
relationships:
  - to: customers
    from_columns: ["customer_id"]
    to_columns: ["id"]
  - to: products
    from_columns: ["product_id"]
    to_columns: ["id"]
```

`from_columns` and `to_columns` are equal-length, non-empty lists: the n-th entries form one key pair, and a composite key lists several in pair order, kept as written in storage, dumps, exports and the generated `ON` condition. Each entry names a field declared on its side by its `name`, without a `filter`, whose expression reads only its own dataset's source columns: a physical reader (an `expression` unset or one source column), whose column is applied when the join is emitted, or a computed field (`CAST(cust_fk AS INT)`), whose expression is. The pre-2.0 `join_pairs: [["customer_id", "id"]]` still converts, with a deprecation; giving it beside either list is refused.

A relationship's `to` spells its dataset exactly: a save whose target matches a dataset only by case (`customers` for `Customers`), or whose name a stored relationship reaches only by case, fails naming the declared spelling.

Relationships enable **cross-dataset metrics** — querying a metric from a joined dataset alongside the main dataset's data. See [Cross-Dataset Metrics](cross-dataset-queries.md#cross-dataset-metrics). During [auto-ingestion](ingestion.md), relationships are generated automatically from foreign keys; multi-hop paths are resolved at query time by walking each intermediate dataset's own relationships. A relationship targeting the dataset itself (`employees.manager` from `manager_id` to its own `id`) saves and loads, as Ossie allows, but takes no part in paths: a reference spelling a hop over it (`employees.manager.name`) fails. To query the second role, define it as a separate dataset over the same table (or a view) and declare the relationship to that.

### Bidirectional traversal

A declared relationship is a **symmetric edge**: it is traversable from either endpoint, so with only `orders → customers` declared, a query rooted at `customers` can still reference `orders.status` — traversal swaps the join pairs and inverts the cardinality label (`many_to_one` ↔ `one_to_many`). Which dataset stores the declaration never changes any query answer, so declare each relationship **once**; declaring the exact inverse on the counterpart dataset is rejected at save time (stored mirror pairs from older versions are deduplicated automatically on load), while a **named** reverse declaration is not an exact inverse — it saves as a disambiguating parallel edge and dedup keeps both halves. The join type is root-relative: a LEFT edge keeps the querying root's rows whole from either side, an INNER edge restricts to matched pairs from either side, and a RIGHT join is never emitted.

When two or more edges connect the same pair of datasets (e.g. billing and shipping FKs onto `customers`), a bare dataset-name hop is ambiguous and **fails closed** in both directions with an error naming the candidate edges. Give each edge a `name` and use it as the path segment instead — names follow dataset-name rules, work from either endpoint, and keep the path as typed in result keys:

```yaml
relationships:
  - to: customers
    from_columns: ["billing_customer_id"]
    to_columns: ["id"]
    cardinality: many_to_one
    name: billing_customer
  - to: customers
    from_columns: ["shipping_customer_id"]
    to_columns: ["id"]
    cardinality: many_to_one
    name: shipping_customer
```

Now `orders.billing_customer.name` selects the billing customer's name (result key `orders.billing_customer.name`), and from a `customers`-rooted query `sum(customers.billing_customer.amount)` traverses the same edge in reverse. Validation rejects an edge name that repeats another edge name on either endpoint, and warns when unnamed parallel edges are left undisambiguatable. An edge may be named like a dataset; a path segment that names both an edge of the dataset and the target of a different edge of it is ambiguous and fails, naming both, so spell that hop by the other edge's name.

Revisits are judged per spelling: a derived field or saved metric is resolved from its own dataset, so from `orders`, `orders.customer.last_amount` (with `last_amount` defined on `customers` as `last_order.amount`) joins a second occurrence of `orders`, while typing `orders.customer.last_order.amount` directly is refused as circular.

### Relationship cardinality

A relationship optionally records its **arity**, read source→target:

```yaml
relationships:
  - to: customers
    from_columns: ["customer_id"]
    to_columns: ["id"]
    cardinality: many_to_one   # many orders → one customer
```

`cardinality` is one of `one_to_one`, `one_to_many`, `many_to_one`, `many_to_many` (omit it when undetermined). It is **orthogonal to the join type** — joins stay LEFT regardless — but it is load-bearing for [cross-dataset metrics](cross-dataset-queries.md#cross-dataset-metrics): a dimension reached over a hop that is provably to-one (a primary key on the far side, or a declared `one_to_one`/`many_to_one`) gets the **exact** per-group value, while an unproven hop makes the metric **associate** across that dimension (each cell aggregating the distinct rows reaching it, cells possibly overlapping) with a response warning. Declaring cardinality (or primary keys) is how you make such dimensions partition the rows. Proof is **per orientation**: a declared `customers → orders (one_to_many)` edge traversed in reverse is a provable `many_to_one` hop, while inverting a to-one hop yields a fan-out orientation that associates.

Auto-ingestion fills it structurally from key constraints: an FK relationship defaults to `many_to_one`, upgrading to `one_to_one` when the source key is itself unique. To infer it from the actual data instead, run:

```bash
slayer validate-datasets --datasource mydb --cardinality                        # report only
slayer validate-datasets --datasource mydb --cardinality --persist-cardinality  # write it back
```

Detection full-scans each side of the relationship and reports the observed arity, a `verdict` (whether it confirms, refines, or hard-contradicts the stored value), and any field declared `unique` that the data shows has duplicates. It is a strong guess, not a guarantee — a duplicate disproves uniqueness with certainty, but the absence of duplicates only suggests it.

A side with no non-null key rows reports `no_evidence` and detects nothing: an empty scan would trivially look unique, and that is not weak evidence — it is none. Re-run once the table has data. A relationship whose scan fails outright reports `scan_failed` and does not stop the rest of the report. Full verdict table: [CLI reference](../reference/cli.md#slayer-validate-datasets).

### Trusted relationships

Declared cardinality and keys decide which paths exist and what a dataset determines; they never change a value. If the data violates a declaration (say a `customers` row appears twice), each `orders` row still counts once in each group it matches, as Ossie defines. SLayer de-duplicates across the hop by reading the target through a `SELECT DISTINCT` of the keys and the values the query groups by. An aggregate input read across the hop (`wsum(orders.amount, weight=customers.weight)`) gets one of the matching values; `to_many_handling: "error"` refuses it.

A hop is **trusted**, and joined directly without de-duplication, when any of these holds:

- its relationship sets `trusted: true`;
- its target key is listed in the target's `enforced_keys`, the declared keys the database enforces;
- its target key is unique by construction: a stage's grain, the time spine, or a list element's own parent row.

```yaml
relationships:
  - to: customers
    from_columns: ["customer_id"]
    to_columns: ["id"]
    cardinality: many_to_one
    trusted: true             # the data never repeats a customer; skip de-duplication
```

On data that satisfies the declarations, trusted and untrusted hops return the same values. On data that violates a trusted relationship, the result is undefined. `trusted` covers every orientation of the relationship that is provably to-one, and has no effect on a relationship with none. Ingestion records `enforced_keys` from primary and unique constraints on databases that enforce them (Postgres, MySQL/MariaDB, SQLite, SQL Server, Oracle, DuckDB), never on Snowflake, BigQuery, Redshift, Databricks, ClickHouse or Trino. An enforced key must name the dataset's primary key or a unique field of a table-backed dataset. Saving a dataset with a different source drops its enforced keys. Models stored before 2.0 upgrade with `trusted: true` on every relationship, so their results do not change.

`validate-datasets` also prints a **Join safety** section with one finding per declared edge, reporting the provability of **both orientations**; an edge neither declared `many_to_one`/`one_to_one` nor structurally proven (via a primary/unique key on the target side) is flagged as a warning — dimensions across such a relationship are [associated](cross-dataset-queries.md#cross-dataset-metrics) rather than partitioning the rows, and aggregation inputs across it are refused, so each finding names the remedy.

### Path-based table aliases

Joined tables use `__`-delimited path aliases in generated SQL so **diamond joins** stay unambiguous — when the same table is reachable via multiple paths. For example, if `orders` has relationships to both `customers` and `warehouses`, each referencing `regions`:

- `customers.regions.name` → table alias `customers__regions`
- `warehouses.regions.name` → table alias `warehouses__regions`

You write dotted relationship paths everywhere — in both queries and dataset-internal SQL (`customers.regions.name`). The `__`-delimited alias is purely an emitted-SQL detail SLayer generates for you. See [Diamond relationships](ingestion.md#diamond-relationships) for details.

## Dataset filters

Dataset filters are SQL conditions always-applied to the underlying table:

```yaml
name: active_orders
sql_table: public.orders
filters:
  - "deleted_at IS NULL"
  - "status <> 'test'"
```

They apply wherever the dataset's rows are read — as a query's source, a relationship target, a cross-dataset aggregate or inside another dataset's filter — before any join, so a LEFT join with no passing match reads as NULL and an INNER join drops the row. As a query's population they decide only its rows: a metric of another dataset is narrowed only where that dataset reads the filtered one along its own paths.

These are SQL-mode expressions (Mode A): any valid SQL the underlying dialect accepts — function calls (`json_extract`, `coalesce`, …), `CASE WHEN`; a bare name reads a source column and a path reads a field (`orders.status`, `customers.regions.name`). Aggregations and SLayer transforms are rejected here — those are DSL constructs (Mode B) and belong in query-level filters or `DatasetMetric.expression`. See [references.md](references.md) for the full Mode A / Mode B table. Dotted paths stay dotted (no auto-conversion); the legacy `__`-delimited split-alias form (`customers__regions.name`) is a hard error.

## Query-backed datasets

A query-backed dataset is a queryable relation whose rows are the final-stage result of one or more saved `SlayerQuery` stages. You can save any query as a dataset, then run it directly by name or use it as `population` in another query — exactly like any table-backed dataset.

### Creating datasets from queries

```python
await engine.create_dataset_from_query(
    query={
        "population": "orders",
        "metrics": ["sum(orders.amount)"],
        "dimensions": ["orders.region"],
        "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}],
    },
    name="monthly_revenue",
    description="Monthly revenue by region",
    variables={"region": "US"},  # default placeholder values, optional
)
```

This saves the query structure in `dataset.source_queries`, saves any defaults in `dataset.query_variables`, runs save-time validation (rendered exactly as execution with no runtime variables, so a `{var}` without a default refuses the save), and caches the resulting `fields` on the dataset for fast inspection; a `dry_run` query on the dataset returns its SQL.

`create_dataset_from_query` accepts a single `SlayerQuery` or a list of stages; for multi-stage queries, every non-final stage must have a `name` so it can be referenced. Stages form a DAG: any stage may use a *prior* named sibling as `population` or as `relationships.to`. Forward and self references are rejected.

### Three ways to use a saved query

Run the backing query directly by name — returns the final-stage result:

```python
await engine.execute("monthly_revenue", variables={"region": "US"})
```

Run it by name with extra clauses merged into its final stage (see [Refining a saved query](saved-and-multistage-queries.md#refining-a-saved-query)):

```python
await engine.execute("monthly_revenue", refine={"dimensions": ["orders.region"]})
```

Or use the saved result as a dataset in another query:

```json
{
  "population": "monthly_revenue",
  "metrics": ["avg(monthly_revenue.amount_sum)"],
  "dimensions": ["monthly_revenue.region"]
}
```

A consumed query-backed dataset is spliced into the consuming query as stages whose names stay private to it, and one that references itself, directly or through other query-backed datasets, is rejected.

Relationships declared on a saved query-backed dataset, keyed on its output fields, stay usable wherever the dataset is consumed. Its final stage also inherits, like any stage, every relationship whose key fields it outputs unchanged (see [Stages inherit edges on pass-through keys](saved-and-multistage-queries.md#query-lists)); a declared relationship wins over an inherited one, and `inspect` lists inherited edges marked `inherited`.

### Variables in dataset SQL

`{variable}` substitution is not limited to query-level filters — the same mechanism injects caller-supplied values into a dataset's **raw-SQL (Mode A) surfaces**. This is the primitive for parameterizing hand-written dataset SQL (for example, representing a Cube `FILTER_PARAMS` cube as an `sql`-mode dataset whose filter bounds arrive as variables).

The four substituted surfaces are:

| Surface | What it holds |
| -- | -- |
| `SlayerDataset.sql` | the raw SQL body of an `sql`-mode dataset (WHERE inside CTEs, projected scalars, anywhere) |
| `SlayerDataset.filters` | dataset-level always-applied WHERE predicates |
| a field's dialect entry | a field's row-level SQL in its datasource's dialect |
| `DatasetField.filter` | a field's CASE-WHEN conditional (e.g. a windowed TY/LY sum with scalar bounds) |

```json
{
  "name": "floored_orders",
  "data_source": "shop",
  "sql": "SELECT id, region, amount FROM orders WHERE amount >= {floor}",
  "query_variables": {"floor": 0},
  "fields": [
    {"name": "id", "datatype": "Integer", "primary_key": true},
    {"name": "region", "datatype": "String"},
    {"name": "amount", "datatype": "Float"}
  ]
}
```

Querying this dataset with `variables={"floor": 100}` binds `100` as the literal of `WHERE amount >= 100`. Every placeholder binds as a literal node of the parsed SQL, never as spliced text: inside a string literal (`'{region}'`), in a bare literal slot (a number, numeric string or boolean), or as the whole list of `IN ({regions})`. A placeholder in any other position — an identifier, a quoted identifier, a function or type name, a comment, part of an `IN` list — is rejected with the error stating that variables supply literal values, when the dataset is saved and again when a query binds it, and so is a value that is no literal in its slot (`"0 AND EXISTS (...)"` for `{floor}`).

Rules:

- **Same precedence** as everywhere else (runtime kwarg > stage > outer query > `dataset.query_variables`), and the same `{{`/`}}` literal-brace escaping.
- **Raise on missing — once any variable is in play.** As soon as at least one variable is supplied (a `query_variables` default, or a caller/stage/runtime value), every `{var}` placeholder must resolve or execution raises `Undefined variable` — a parameterized dataset is meant to fail without its value, not silently render a neutral predicate.
- **Variable-free executions treat braces as literals.** When there is *no* variable in play at all (no `query_variables` and no caller variables), the four surfaces are left untouched: a raw brace literal (e.g. a Postgres array `'{1,2,3}'`) survives verbatim, and a placeholder-shaped token like `'{status}'` is emitted as-is rather than raising. This is the deliberate contract that lets brace-bearing SQL coexist with the feature. If a dataset *does* use variables, escape any literal braces as `{{`/`}}`. Two kinds of dataset are exempt and always run substitution — one carrying an optional `{? ... ?}` block (so the block collapses), and a **generated** dataset that declares its variables in `meta.cube_variables` (an importer wrote the template, so there is no brace-literal ambiguity to protect and a missing variable raises normally).
- **String escaping is dialect-aware.** Write the surrounding quotes yourself (`WHERE region = '{region}'`); the value is spelled into that literal in the datasource dialect's own escaping, so a value like `a\'b` stays exactly that text. An unquoted placeholder takes a number, a numeric string or a boolean; non-finite floats are rejected.
- **List values render an `IN`-list.** A `list` (or `tuple`) variable renders a comma-separated, injection-safe `IN`-list body. Write the parentheses yourself and **omit** per-element quotes — each string element is auto-quoted for you (the opposite of the scalar-string rule above, because a single placeholder can't carry per-element quotes):

    ```json
    { "filters": ["region IN ({regions})"] }
    ```

    with `variables={"regions": ["US", "CA"]}` renders `region IN ('US', 'CA')`. Elements must be strings or numbers; an **empty list raises** (`IN ()` is invalid SQL — for "no filter" semantics use a sentinel default rather than an empty list). A scalar there binds as a one-element list.
- **Optional blocks `{? ... ?}` — a filter that disappears when its variable is absent.** Wrap a predicate in `{? ... ?}` (Mode-A surfaces only): when every `{var}` inside is supplied, the block renders parenthesised; when any is missing, the whole block collapses to the neutral `(1=1)`. This is the SLayer form of a Cube `FILTER_PARAMS` optional pushdown. Put the `AND` outside the block so the collapse leaves valid SQL (open your `WHERE` with `1=1`):

    ```json
    { "sql": "SELECT * FROM orders WHERE 1=1 AND {? region IN ({regions}) ?}" }
    ```

    With `variables={"regions": ["US","CA"]}` this renders `... WHERE 1=1 AND (region IN ('US', 'CA'))`; with no `regions` supplied it renders `... WHERE 1=1 AND (1=1)`. A block must contain at least one `{var}`; blocks do not nest; a block collapses even on a **zero-variable** call (unlike bare placeholders, which are left literal when no variable is in play at all). Optional blocks are rejected in every Mode-B expression.
- **`inspect` shows the literal template** (`{floor}`), not a rendered value. A `Variables:` line lists the dataset's placeholders classified **required** (bare, no default — omitting it raises) vs **optional** (inside a block, or carrying a `query_variables` default). The classification is derived from the SQL, not stored, so it can never drift from the template.

**Scope:** every dataset a query reads (its population, a joined dataset, an aggregate's own dataset, each stage of a query-backed source) binds its `{var}` with its own `query_variables` defaults, overridden by the query's variables and runtime variables; another dataset's defaults never reach it.

**Saved metric expressions** (`DatasetMetric.expression`) on those same datasets take variables too, bound as literals like any Mode-B expression and with no optional blocks, and inspection lists their placeholders as required unless defaulted.

**Literals only.** No value a query, a stage or a `query_variables` default supplies can change the structure of the SQL it binds into. The backslash-dialect escaping assumes the server's **default** string mode — a MySQL server running with `sql_mode=NO_BACKSLASH_ESCAPES` treats backslash as an ordinary char, which the whole sqlglot dialect layer (not just this feature) assumes is off.

### Variable precedence

When a query-backed dataset references `{var}` placeholders, values flow in this order (highest first):

1. **Runtime kwarg** — `variables=` on `engine.execute(...)` (also via REST `/query`, MCP `query` / `create_dataset`, CLI `--variables` / `--variables-json`). Wins at every nesting level.
2. **Stage `.variables`** — set on an individual `SlayerQuery` stage.
3. **Outer query `.variables`** — when a query-backed dataset is used as `population` in another query.
4. **Dataset defaults** — `dataset.query_variables`; through nested query-backed datasets each enclosing dataset's defaults override those of the dataset it wraps, with the innermost stage's population lowest.

Unresolved placeholders raise a clear error at execute time, naming the dataset and the missing variable. Runtime-kwarg variables that don't appear anywhere are silently ignored.

### What gets cached

For a query-backed dataset the engine caches `dataset.fields` (final-stage output columns — a discoverability snapshot); each cached field produced by a time dimension records its `granularity`, so a finer time dimension over the dataset is the same typed error as over a stage column. The cache is populated **only** on save through `engine.save_dataset` (REST `POST`/`PUT /datasets`, MCP `create_dataset`/`edit_dataset`). **Read operations never write storage** — `engine.execute`, `inspect`, `get_result_column_types`, MCP `query`, and REST `/query` will never modify the persisted cache. Writing a query-backed dataset directly to storage outside the engine leaves the cache stale until the next engine save.

You **cannot** supply `fields` yourself when creating a query-backed dataset — they are engine-managed, and any user-supplied value is rejected with a clear error.

### Field naming in query-derived datasets

A query result is a self-contained table — it no longer has the relationships its source dataset may have had. Dimensions and metrics that came from joined datasets use `__` to encode the original relationship path in their name:

| Inner query field | Virtual dataset field name |
|----------------------|--------------------------|
| `stores.name` | `stores__name` |
| `customers.regions.name` | `customers__regions__name` |
| `orders.customer_id` | `customer_id` |
| `count(orders.*)` (metric) | `_count` |
| `sum(orders.revenue)` (metric) | `revenue_sum` |
| `{"metric": "sum(orders.revenue)", "alias": "rev"}` | `rev` |

This uses the same `__` convention as the SQL-level path aliases. When referencing these fields in an outer query, qualify the `__` name by the dataset or stage name (e.g., `"monthly_revenue.stores__name"`) — a further dotted path would imply a relationship to a dataset that doesn't exist on the virtual table.

An explicit `name` on a metric spec **overrides** the canonical naming above, for both arithmetic/transform expressions and simple aggregations. This is what lets multi-stage `source_queries` rename inner-stage outputs cleanly:

```json
{
  "source_queries": [
    {
      "name": "raw",
      "population": "orders",
      "dimensions": ["orders.region"],
      "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}]
    },
    {
      "population": "raw",
      "metrics": ["sum(raw.rev)"]
    }
  ]
}
```

The inner stage emits a column named `rev` (not `amount_sum`), and the outer stage references it by that chosen name. See the [multistage queries example](../examples/06_multistage_queries/multistage_queries.md) for working examples.

## Result column format

Query results key each reference by its path as written, never by the population. Aggregations map to keys: `sum(orders.revenue)` becomes `orders.revenue_sum`; `count(orders.*)` becomes `orders._count` (leading underscore so the alias never collides with a user field literally named `count`). Multi-hop joined dimensions keep their full path:

```json
{"orders.status": "completed", "orders._count": 42, "orders.revenue_sum": 1500}
{"customers.regions.name": "US", "orders._count": 3}
```

## Schema versioning

Every persisted SLayer entity (`SlayerDataset`, `SlayerQuery`, `DatasourceConfig`, `Memory`) carries a `version: int` field — currently `19` for `SlayerDataset`, `11` for `SlayerQuery`, `3` for `DatasourceConfig`, `8` for `Memory`. Behaviour:

- **On save**, SLayer always writes the current schema version.
- **On load**, an older `version` triggers a chain of pure dict→dict converters before Pydantic validation. Hand-edited and older files keep working as the schema evolves.
- **Round-tripping** an older file (load → save) upgrades it on disk to the current schema.
- **Forward tolerance.** A file with a higher `version` than this SLayer knows about loads on a best-effort basis. A stored dataset drops unknown keys with an `unknown_stored_key` warning, and `DatasourceConfig` ignores them. A stored query refuses an unknown key on itself or any entry with an `UnknownKeyError` (that query fails to load; the rest of the store loads), so a future schema's new query fields will not load on an older SLayer.

The v2→v3 converter drops the legacy `dry_run` / `explain` fields from `SlayerQuery` — they were execution-mode flags that had no business being persisted; pass them as kwargs to `engine.execute(query, dry_run=..., explain=...)` instead. Each migrated query emits one `logger.warning` and one `DeprecationWarning` on first load.

Stored Mode-B text written before the SQL grammar is upgraded on load: double-quoted strings become `'…'`, backslash escapes are re-encoded, adjacent literals are joined, `|` becomes `||`, an object-form time dimension becomes its dotted `dataset.name` text, and an aggregation over a bare field named `distinct` (`sum(distinct)`) reads it as `"distinct"`.

Text the old grammar could not have held (`::`, `||`, `CAST(`, `FILTER (WHERE`, `BETWEEN`) is left unchanged with a warning even under an old `version`, but old-version text using only `''`, `"…"` or backslash escapes is still upgraded, so hand-edit such text only together with its `version`.

A stored datasource granularity whose name the library has since reserved is renamed on load to `<name>_granularity` (`<name>_granularity_<n>` if taken), recording its former name as `renamed_from`, with a logged warning, so update any client that names it. References to the former name are renamed only in that datasource's datasets, saved queries and memories written before the library reserved it; a newer document reads the name as the library's.

Persisted documents omit unset (`None`) fields, so a store written by this release still reads on the previous one, except for text already in the new grammar and `ARRAY` fields.

A [semantic model](semantic-models.md)'s `version` is Ossie's spec version, never SLayer's; SLayer versions only its `SLAYER` entries, and loading a document never rewrites it.

Migrations are defined in `slayer/storage/migrations.py` and apply at the Pydantic-validation layer, so every storage backend (YAML, SQLite, third-party backends registered via `register_storage`, plus HTTP API, MCP server, and dbt importer) gets them automatically.

## Keeping datasets in sync with the live schema

When the live database schema changes — a column drops, a type bucket flips, a table goes away — persisted datasets stop being valid. SLayer surfaces this as a first-class concept: `slayer validate-datasets` returns a structured diff (including semantic-model fields that declare no type), `slayer ingest` is idempotent (additive only), and query-time DBAPI errors get attributed to the right dataset with a `SchemaDriftError`. See [Schema Drift](schema-drift.md) for the full diff / cascade contract and the `--force-clean` apply path.
