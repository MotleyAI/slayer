# Datasets

A dataset is SLayer's view of a database table or an underlying SQL query. It declares the fields, named metric expressions, relationships, and always-applied filters that queries can build on. Datasets are defined as YAML (one file per dataset under `models/<data_source>/`) or created via API/MCP — the two paths produce the same persisted object.

A tiny example to anchor what follows:

```yaml
name: orders
sql_table: public.orders
data_source: my_postgres
fields:
  - {name: id, datatype: Integer, primary_key: true}
  - {name: status, datatype: String}
  - {name: revenue, sql: amount, datatype: Decimal}
metrics:
  - {name: aov, expression: "sum(revenue) / count(*)"}
```

A query then asks for `sum(revenue)` (aggregate the `revenue` field), `aov` (the saved expression), or `status` (group by it). Same dataset, different roles per query.

## Fields at a glance

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique dataset name. In the default YAML storage, names differing only by letter case (`Orders` vs `orders`) are rejected at save time (`IdCollisionError`) — they would collide as filenames on macOS / Windows |
| `sql_table` | string | One of | A physical database table or view (e.g. `public.orders`) |
| `sql` | string | these | A SQL subquery to use as the source |
| `source_queries` | list[SlayerQuery] | three | Saved query stages — makes the dataset **query-backed** |
| `source_kind` | string | No | What kind of object `sql_table` names: `table`, `view`, or `materialized_view`. Set by auto-ingestion; `null` means unknown |
| `data_source` | string | Yes | Datasource name |
| `fields` | list[DatasetField] | No | Field definitions. For query-backed datasets this is an engine-managed cache |
| `metrics` | list[DatasetMetric] | No | Named expression library — referenced by bare name in queries |
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

A field is the unit of structure on the dataset. The same field entry can serve as a group-by key in one query and as input to an aggregation in another — the role is decided per query, not declared up front. A [list field](queries.md#list-fields) is the exception. What the field *carries* is its identity (name), how to compute it from the source (`sql`), what data type to expect (`datatype`), and a handful of policy attributes (which aggregations are allowed, whether it's a primary key, whether it's hidden).

### Attributes

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique within the dataset. Must not contain `.` or `:`; the `__slayer_` prefix is reserved. `__` is otherwise allowed |
| `description` | string | No | — | Clarifies meaning for agents and users |
| `label` | string | No | — | Display name; propagates into query result metadata |
| `sql` | string | No | (bare field name) | SQL expression — defaults to the field's name |
| `datatype` | string | No | `String` | One of the [data types](#data-types), matched by exact case |
| `element_type` | string | With `Array` on a table-backed dataset | `Opaque` on an `sql`-mode dataset's `Array` | A list's element type (any data type but `Array`; `Opaque` for nested lists and records, which support only NULL tests and `count`); rejected on any other type |
| `db_type` | string | No | — | The raw database type, recorded by ingestion where `datatype` loses information: `Opaque`, exact `NUMERIC` / `DECIMAL`, JSON, zoned timestamps and lists |
| `primary_key` | bool | No | `false` | A dataset's sole primary key is an identifier, restricted to counting, `min` / `max` and `collect`; members of a composite key are ordinary fields |
| `unique` | bool | No | `false` | Single-field uniqueness (non-PK). A sole `primary_key` implies unique; a composite-key member does not. Auto-set from `UNIQUE` constraints / unique indexes; used to infer one-to-one relationships |
| `hidden` | bool | No | `false` | Hide from listings |
| `format` | dict | No | — | `NumberFormat` used by response metadata |
| `allowed_aggregations` | list[str] | No | — | Whitelist (must be a subset of the type-default eligibility set, or a custom aggregation defined on this dataset) |
| `filter` | string | No | — | SQL condition wrapping the field value in `CASE WHEN` — a value mask that fires in every position. See [Filtered fields](#filtered-fields) |
| `granularity` | string | No | — | Time bucket a temporal field is already truncated to (`month`, `year`, …); a finer or non-nesting time dimension over it is a typed error. Query-backed datasets stamp it automatically; set it by hand only when the values are truly bucketed at that grain |
| `meta` | dict | No | — | Arbitrary JSON metadata |
| `sampled` | string | No | — | Cached sample-value text snapshot (top-20 by frequency joined, or `top20 ... (50+ distinct)` on overflow, or `min .. max` for numeric/temporal); populated lazily on the first `inspect` of the field (or via `slayer datasources refresh-samples`), not at ingest time |
| `sampled_values` | list[str] | No | — | Structured top-50-by-frequency list (categorical values only); the unambiguous counterpart to `sampled` for consumers that need to compare predicate literals against stored values. `None` for numeric/temporal fields |
| `distinct_count` | int | No | — | Exact distinct count when ≤ 50 (categorical only). `None` on overflow (> 50 distinct — one scan only, no secondary `count_distinct` query) and for numeric/temporal fields |

A list field is profiled over its non-NULL elements: `sampled_values` and `distinct_count` for `String` / `Boolean` elements, a `min .. max` range for numeric and temporal ones; a whole list is never sampled, and a list with `Opaque` elements is never profiled.

A JSON function reads a field over a native JSON column (Postgres `json` / `jsonb`, `JSON`, Snowflake `VARIANT`) in its native form only when its `db_type` is recorded; a field without one is read as text.

A field's `sql` is rendered into the executed statement, which reaches the database verbatim — `:name` is never read as a bind parameter, nor `%` as a format directive — so regex literals (`(?:…)`) and date formats (`%Y-%m`) pass through unchanged.

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
| `DateTimeTz` | Date and time with a time zone; behave as `DateTime` | TIMESTAMPTZ, TIMESTAMP WITH TIME ZONE |
| `Time` | Time of day; opaque, like `Opaque` | TIME |
| `Opaque` | Stored and displayed, never grouped, aggregated or cast | Postgres `point`, `json`, range types |
| `Array` | A list of `element_type` values, see [list fields](queries.md#list-fields); re-ingest re-types a `String` field whose live type is a list, keeping its other attributes | `TEXT[]`, BigQuery `REPEATED STRING`, `Array(String)` |

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
    sql: amount
    datatype: Float
    filter: "status = 'active'"
  - name: completed_count
    sql: id
    datatype: Integer
    filter: "status = 'completed'"
```

`sum(active_revenue)` then generates `SUM(CASE WHEN status = 'active' THEN amount END)`. The filter is pure syntactic sugar for `CASE WHEN <filter> THEN <value> END`, a **value mask** that fires in *every* position: as a group-by dimension, rows where the filter is false fall into the `NULL` group; a `first`/`last` picks the masked value at the chosen row (`NULL` if it does not match). It never removes rows or changes which row is picked — a genuine row restriction belongs in a query `filter`.

Filters can reference joined fields via dot syntax (`categories.type = 'electronics'`). Filtered and unfiltered fields coexist freely in the same query and combine cleanly in arithmetic expressions (e.g. `"sum(active_revenue) / sum(total_revenue)"`).

### Derived Fields Referencing Other Derived Fields

A `DatasetField.sql` may reference any other field on the same dataset or on a joined dataset — including fields that are themselves *derived* (have their own `sql` expression rather than being a bare base-table column). The engine recursively inlines those references at query time, so chains stay DRY.

```yaml
# Dataset: stations
fields:
  - name: foo_raw                 # base field
    sql: "foo_raw"
    datatype: Float
  - name: foo_normalized          # derived on stations
    sql: "foo_raw / 100.0"
    datatype: Float
# Dataset: telescopes — joined to stations
fields:
  - name: aoi_ratio               # derived on telescopes, references the
                                  # *derived* stations.foo_normalized
    sql: "telescopes.aperture / stations.foo_normalized"
    datatype: Float
relationships:
  - to: stations
    from_columns: ["station_id"]
    to_columns: ["id"]
```

At query time, `aoi_ratio` expands to `telescopes.aperture / (stations.foo_raw / 100.0)`. The same applies to same-dataset chains (a field referencing another derived field on the same dataset) and to multi-hop relationship paths (use the dotted form, e.g., `B.C.x_derived`, when crossing more than one relationship).

A derived field whose definition (recursively) crosses a fanning (not provably to-one) hop fails closed with a typed error when used as an aggregate input, a population filter, or an `error`-mode dimension — declare the [relationship cardinality](#relationship-cardinality) or primary key to prove the hop, or query it under the default (associating) or `broadcast` handling. A derived field whose definition reaches a to-many (or undeclared) target cannot be aggregated as a field of its dataset — aggregate the target field directly (`sum(line_items.qty)`). Where the hop is *provably* fanning (declared `one_to_many`/`many_to_many`), the dataset is rejected at **save time**; an unproven hop saves with a warning and relies on the query-time check above. A definition whose relationship path **revisits** a dataset already on it (a cycle, e.g. `regions.customers.spend` declared on `customers`) is rejected at **save time** as a circular definition — reference the field on the revisited dataset directly, or aggregate on the dataset the hop leaves. A definition crossing an ambiguous hop (two or more edges between the pair and no edge name in the path) is also rejected at **save time** — spell that hop by an edge name.

Same-dataset references may be written **bare** (just the field name) or qualified with the host alias — both forms expand the same way. So given `bucket.sql = "raw_a / 10"`, a sibling `rn.sql = "ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY id)"` correctly expands `bucket` to the inlined body. Bare references inside a nested scope (sub-query, `UNION` branch, CTE, `VALUES`) are NOT inlined — those identifiers belong to the inner rowset, not the host dataset — so `DatasetField.sql = "(SELECT MAX(score) FROM other) + score"` inlines the outer `score` but leaves the inner one alone.

Cycles in the reference graph (e.g., `c1.sql = "c2 + 1"` and `c2.sql = "c1 - 1"`) are rejected at `save_dataset` time and raise `DatasetFieldCycleError` (which subclasses both `SlayerError` and `ValueError`) with the cycle path in the message — so a broken chain never reaches a query. The compile-time guard remains as defence in depth. Save-time validation stays within the dataset's `data_source`; unresolved cross-datasource refs are silently skipped. The same expansion is applied to filters and to aggregated metrics, so `"sum(B.foo_normalized)"` produces `SUM(B.foo_raw / 100.0)`.

### Window functions in `DatasetField.sql`

A field's `sql` may contain a window function (`row_number() over (...)`, `dense_rank() over (...)`, etc.). The field behaves like any other field when used in `dimensions` / SELECT.

> **Filtering on a windowed field is rejected.** A query filter naming a `DatasetField` whose `sql` contains a window function (e.g. `{"filter": ["rn <= 3"]}` against a field whose `sql` is `row_number() over (...)`) raises with a clear message. Use `{"filter": ["rank(<metric>, direction='desc') <= 3"]}` (see [formulas.md](formulas.md#rank-family-transforms)) — the rank-family transforms cover the top-N case in pure DSL — or factor the field into a multi-stage `source_queries` dataset.

### SQL expression conventions

Bare field names auto-qualify against the dataset's own table — single references (`"amount"`) and arithmetic alike (`"amount * quantity"`). Explicit `dataset.field` works too but adds nothing. For joined fields inside a dataset's own `sql`, use the dotted relationship path (`customers.regions.name`) — see [Relationships](#relationships).

### SQLite JSON extraction

`json_extract(col, '$.path')` in a `DatasetField.sql` is preserved verbatim on SQLite — SLayer does **not** rewrite it to `col -> '$.path'`. The `->` operator in SQLite returns the JSON-*quoted* form (`'"Owned"'`, with literal quotes), which silently breaks equality and `CASE WHEN` matches against bare-string literals. The function form returns the unquoted scalar.

```yaml
fields:
  - name: tier
    datatype: String
    sql: "json_extract(payload, '$.tier')"
  - name: is_gold
    datatype: Integer
    sql: "CASE LOWER(json_extract(payload, '$.tier')) WHEN 'gold' THEN 1 ELSE 0 END"
```

If you specifically want SQLite's JSON-scalar operator, write `->>` (`exp.JSONExtractScalar`) directly — SLayer leaves it untouched.

## Metrics vs aggregations — the disambiguation

SLayer has two lists on a dataset that both relate to metrics, and the names don't make the difference obvious. The split is real and load-bearing:

- **`metrics`** is a library of named **expressions** — saved expressions like `aov = sum(revenue) / count(*)`. Queries reference them by bare name and the expression expands inline. Think *what to compute*, at the metric level.
- **`aggregations`** is a registry of custom **operators** — definitions like `trimmed_mean(p)` or `weighted_avg(weight=…)`. Once defined, they become usable inside any expression: `trimmed_mean(revenue, p=0.1)`. Think *how to aggregate*, at the operator level.

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
| `name` | string | No | Queries reference this by bare name (auto-derived if omitted) |
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

Field and metric names share a namespace within a dataset — you can't have a field `aov` *and* a metric `aov`. A metric can use any other metric by bare name, including inside transforms and arithmetic:

```json
"metrics": [
  "aov",
  "cumsum(aov)",
  {"metric": "aov * 1.1", "alias": "aov_with_markup"}
]
```

A saved expression produces the same SQL as writing it out longhand. Cycles (`a → b → a`) are rejected during binding. Names shadowing built-in transforms are rejected at dataset construction.

### Reusing another dataset's saved metric (`customers.aov`)

A dotted `"customers.aov"` reuses a saved metric from the joined `customers` dataset as if its expression were written inline, and a query-backed dataset cannot declare `metrics` directly. A saved metric whose expression crosses back to a dataset already on the path (e.g. `customers.order_total` defined as `sum(customer.amount)`, used from `orders`) evaluates over its own occurrence of that dataset rather than failing.

See [formulas.md](formulas.md) for the full expression grammar — operators, transforms, time-shifted forms, and the `Mode B` DSL rules.

## Aggregations

Aggregations are the operators that turn a field expression into a value: `sum`, `avg`, `percentile(p=…)`, and so on. They're applied at query time as a function call — `aggregation(field)` — and the same operator works on any compatible field.

### Built-in aggregations

| Aggregation | Syntax | SQL |
|-------------|--------------|-----|
| `count` | `count(*)` | `COUNT(*)` — counts all rows |
| `count` | `count(col)` | `COUNT(col)` — counts non-null values |
| `count_distinct` | `count_distinct(col)` | `COUNT(DISTINCT col)` |
| `sum` | `sum(revenue)` | `SUM(revenue)` |
| `avg` | `avg(revenue)` | `AVG(revenue)` |
| `min` / `max` | `min(revenue)` | `MIN(revenue)` / `MAX(revenue)` |
| `first` / `last` | `first(col, time_col)` | Earliest / latest record's value, time-ordered |
| `weighted_avg` | `weighted_avg(price, weight=quantity)` | `SUM(price * quantity) / SUM(quantity)` |
| `median` | `median(revenue)` | Median value |
| `percentile` | `percentile(revenue, p=0.95)` | 95th percentile |
| `stddev_samp` / `stddev_pop` | `stddev_samp(latency)` | Sample / population standard deviation |
| `var_samp` / `var_pop` | `var_samp(latency)` | Sample / population variance |
| `corr` | `corr(price, other=quantity)` | Pearson correlation between two fields |
| `covar_samp` / `covar_pop` | `covar_samp(price, other=quantity)` | Sample / population covariance |
| `collect` | `collect(name)` | The values as a list, see [list fields](queries.md#list-fields) |

`count(*)` is always available with no metric definition. `*` means "all rows" and is **only** valid with `count` — `sum(*)` and friends are rejected. Detailed NULL / N=1 semantics for the statistical aggregations are documented in [database-support.md](../database-support.md).

### The `first` and `last` aggregations

`first` and `last` return the value from the **earliest or most recent record** within each grouped bucket — like `min`/`max`, but ordered by time instead of value. Useful for snapshot metrics like balances, inventory counts, or status fields where you want the latest state:

```yaml
fields:
  - {name: balance, sql: balance, datatype: Float}
```

`last(balance, updated_at)` gives the most recent balance per group; `first(balance, updated_at)` the earliest. A row whose ranking key is NULL is never picked, on every database; a group whose keys are all NULL returns NULL. When grouped by month, each month returns the latest (or earliest) record's balance in that month. If no time field is specified, ordering resolves via: the first DATE / TIMESTAMP row-level dimension in the query → the first time dimension's untruncated field or expression → the dataset's `default_time_dimension` (its only DATE / TIMESTAMP field when unset). Whichever field wins is an input of the aggregation: if it, or a derived definition it names, crosses a relationship hop that is not provably to-one from the aggregation's root, the query fails with the input-safety error exactly as an explicit ranking argument would. The value and the ranking key may each be a row-level expression (`first(amount - customers.discount)`, `last(balance, coalesce(updated_at, created_at))`), and a computed dimension typed DATE / TIMESTAMP counts as a DATE / TIMESTAMP dimension in that order.

Not to be confused with the [`last()` expression function](formulas.md#first-and-last-functions) — a window-function transform that broadcasts a value across all rows. Same name, different layer.

## Custom aggregations

Add your own operators via the `aggregations` list. Each entry has a name and a SQL expression template using `{value}` for the aggregated expression and named placeholders for kwargs:

```yaml
aggregations:
  - name: weighted_avg
    expression: "sum({value} * {weight}) / sum({weight})"
  - name: trimmed_mean
    expression: "avg(CASE WHEN {value} BETWEEN {low} AND {high} THEN {value} END)"
```

Use at query time: `weighted_avg(price, weight=quantity)`, `trimmed_mean(revenue, low=10, high=1000)`. An aggregation entry can also override a built-in's default parameters without redefining the SQL, or give a built-in other than `first`/`last` an `expression`, which then replaces its SQL. Like fields and metrics, aggregations accept an optional `meta` dict for caller bookkeeping. An expression that does not parse as SQL is rejected when the dataset is saved, and so is a parameter whose default `sql` is empty, whose name is `value` (which always stands for the aggregated field) or `window` (the trailing-window argument), or that the expression never references; a query argument the aggregation does not accept is rejected, listing the accepted names. A built-in aggregation's name, in any case, resolves only to an aggregation named exactly that built-in's lowercase name.

An aggregation may take the name of a library function or function spelling (`reverse`, `left`, `strpos`, …): inside its own dataset every call over that dataset's fields — `reverse(a)`, `t.reverse(a)`, in any letter case — is the aggregation, the library function stays reachable everywhere else (another dataset's fields, a literal argument), and saving, loading and every query response that resolves through it carry a `library_name_shadow` warning; transform, date-part and granularity names stay reserved.

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

`from_columns` and `to_columns` are equal-length, non-empty lists: the n-th entries form one key pair, and a composite key lists several in pair order, kept as written in storage, dumps, exports and the generated `ON` condition. Each entry names a base field (`sql` unset or a single column, no `filter`) declared on its side by the field's `name`, and the field's `sql` rename is applied when the join is emitted. The pre-2.0 `join_pairs: [["customer_id", "id"]]` still converts, with a deprecation; giving it beside either list is refused.

A relationship's `to` spells its dataset exactly: a save whose target matches a dataset only by case (`customers` for `Customers`), or whose name a stored relationship reaches only by case, fails naming the declared spelling.

Relationships enable **cross-dataset metrics** — querying a metric from a joined dataset alongside the main dataset's data. See [Cross-Dataset Metrics](queries.md#cross-dataset-metrics). During [auto-ingestion](ingestion.md), relationships are generated automatically from foreign keys; multi-hop paths are resolved at query time by walking each intermediate dataset's own relationships. A relationship targeting the dataset itself is rejected at validation — relationships are addressed by dataset name, so define the second role as a separate dataset over the same table (or a view) and declare the relationship to that.

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

Now `billing_customer.name` selects the billing customer's name (result key `orders.billing_customer.name`), and from a `customers`-rooted query `sum(billing_customer.amount)` traverses the same edge in reverse. Validation rejects an edge name that collides with a dataset name or another edge name on either endpoint, and warns when unnamed parallel edges are left undisambiguatable.

Revisits are judged per spelling: a derived field or saved metric is resolved from its own dataset, so from `orders`, `customer.last_amount` (with `last_amount` defined on `customers` as `last_order.amount`) joins a second occurrence of `orders`, while typing `customer.last_order.amount` directly is refused as circular.

### Relationship cardinality

A relationship optionally records its **arity**, read source→target:

```yaml
relationships:
  - to: customers
    from_columns: ["customer_id"]
    to_columns: ["id"]
    cardinality: many_to_one   # many orders → one customer
```

`cardinality` is one of `one_to_one`, `one_to_many`, `many_to_one`, `many_to_many` (omit it when undetermined). It is **orthogonal to the join type** — joins stay LEFT regardless — but it is load-bearing for [cross-dataset metrics](queries.md#cross-dataset-metrics): a dimension reached over a hop that is provably to-one (a primary key on the far side, or a declared `one_to_one`/`many_to_one`) gets the **exact** per-group value, while an unproven hop makes the metric **associate** across that dimension (each cell aggregating the distinct rows reaching it, cells possibly overlapping) with a response warning. Declaring cardinality (or primary keys) is how you make such dimensions partition the rows. Proof is **per orientation**: a declared `customers → orders (one_to_many)` edge traversed in reverse is a provable `many_to_one` hop, while inverting a to-one hop yields a fan-out orientation that associates.

Auto-ingestion fills it structurally from key constraints: an FK relationship defaults to `many_to_one`, upgrading to `one_to_one` when the source key is itself unique. To infer it from the actual data instead, run:

```bash
slayer validate-datasets --datasource mydb --cardinality                        # report only
slayer validate-datasets --datasource mydb --cardinality --persist-cardinality  # write it back
```

Detection full-scans each side of the relationship and reports the observed arity, a `verdict` (whether it confirms, refines, or hard-contradicts the stored value), and any field declared `unique` that the data shows has duplicates. It is a strong guess, not a guarantee — a duplicate disproves uniqueness with certainty, but the absence of duplicates only suggests it.

A side with no non-null key rows reports `no_evidence` and detects nothing: an empty scan would trivially look unique, and that is not weak evidence — it is none. Re-run once the table has data. A relationship whose scan fails outright reports `scan_failed` and does not stop the rest of the report. Full verdict table: [CLI reference](../reference/cli.md#slayer-validate-datasets).

`validate-datasets` also prints a **Join safety** section with one finding per declared edge, reporting the provability of **both orientations**; an edge neither declared `many_to_one`/`one_to_one` nor structurally proven (via a primary/unique key on the target side) is flagged as a warning — dimensions across such a relationship are [associated](queries.md#cross-dataset-metrics) rather than partitioning the rows, and aggregation inputs across it are refused, so each finding names the remedy.

### Path-based table aliases

Joined tables use `__`-delimited path aliases in generated SQL so **diamond joins** stay unambiguous — when the same table is reachable via multiple paths. For example, if `orders` has relationships to both `customers` and `warehouses`, each referencing `regions`:

- `customers.regions.name` → table alias `customers__regions`
- `warehouses.regions.name` → table alias `warehouses__regions`

You write dotted relationship paths everywhere — in both queries and dataset-internal SQL (`customers.regions.name`). The `__`-delimited alias is purely an emitted-SQL detail SLayer generates for you. See [Diamond Joins](ingestion.md#diamond-relationships) for details.

## Dataset filters

Dataset filters are SQL conditions always-applied to the underlying table:

```yaml
name: active_orders
sql_table: public.orders
filters:
  - "deleted_at IS NULL"
  - "status <> 'test'"
```

They apply wherever the dataset's rows are read — as a query's source, a relationship target, a cross-dataset aggregate or inside another dataset's filter — before any join, so a LEFT join with no passing match reads as NULL and an INNER join drops the row.

These are SQL-mode expressions (Mode A): any valid SQL the underlying dialect accepts — function calls (`json_extract`, `coalesce`, …), `CASE WHEN`, joined-field references via dotted relationship paths (`customers.regions.name`). Aggregations and SLayer transforms are rejected here — those are DSL constructs (Mode B) and belong in query-level filters or `DatasetMetric.expression`. See [references.md](references.md) for the full Mode A / Mode B table. Dotted paths stay dotted (no auto-conversion); the legacy `__`-delimited split-alias form (`customers__regions.name`) is a hard error.

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

Run it by name with extra clauses merged into its final stage (see [Refining a saved query](queries.md#refining-a-saved-query)):

```python
await engine.execute("monthly_revenue", refine={"dimensions": ["region"]})
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

Relationships declared on a saved query-backed dataset, keyed on its output columns, stay usable wherever the dataset is consumed; the relationships of the dataset its backing query reads are not inherited.

### Variables in dataset SQL

`{variable}` substitution is not limited to query-level filters — the same mechanism injects caller-supplied values into a dataset's **raw-SQL (Mode A) surfaces**. This is the primitive for parameterizing hand-written dataset SQL (for example, representing a Cube `FILTER_PARAMS` cube as an `sql`-mode dataset whose filter bounds arrive as variables).

The four substituted surfaces are:

| Surface | What it holds |
| -- | -- |
| `SlayerDataset.sql` | the raw SQL body of an `sql`-mode dataset (WHERE inside CTEs, projected scalars, anywhere) |
| `SlayerDataset.filters` | dataset-level always-applied WHERE predicates |
| `DatasetField.sql` | a field's row-level SQL expression |
| `DatasetField.filter` | a field's CASE-WHEN conditional (e.g. a windowed TY/LY sum with scalar bounds) |

```json
{
  "name": "floored_orders",
  "data_source": "shop",
  "sql": "SELECT id, region, amount FROM orders WHERE amount >= {floor}",
  "query_variables": {"floor": 0},
  "fields": [
    {"name": "id", "sql": "id", "datatype": "Integer", "primary_key": true},
    {"name": "region", "sql": "region", "datatype": "String"},
    {"name": "amount", "sql": "amount", "datatype": "Float"}
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

**Scope:** substitution applies to the query's **direct population**, each stage's own population, and every stage of a query-backed source (spliced into the statement); relationship-target and cross-dataset-target datasets are not yet substituted, so a `{var}` there is left untouched (and surfaces as an error on the stray placeholder).

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
| `customer_id` | `customer_id` |
| `count(*)` (metric) | `count` |
| `sum(revenue)` (metric) | `revenue_sum` |
| `{"metric": "sum(revenue)", "alias": "rev"}` | `rev` |

This uses the same `__` convention as the SQL-level path aliases. When referencing these fields in an outer query, use the `__` name directly (e.g., `"stores__name"`), not dot syntax — dots would imply a relationship to a dataset that doesn't exist on the virtual table.

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

Query results use `dataset_name.field_name` keys. Aggregations map to keys: `sum(revenue)` becomes `orders.revenue_sum`; `count(*)` becomes `orders._count` (leading underscore so the alias never collides with a user field literally named `count`). Multi-hop joined dimensions keep their full path:

```json
{"orders.status": "completed", "orders._count": 42, "orders.revenue_sum": 1500}
{"orders.customers.regions.name": "US", "orders._count": 3}
```

## Schema versioning

Every persisted SLayer entity (`SlayerDataset`, `SlayerQuery`, `DatasourceConfig`, `Memory`) carries a `version: int` field — currently `19` for `SlayerDataset`, `11` for `SlayerQuery`, `3` for `DatasourceConfig`, `8` for `Memory`. Behaviour:

- **On save**, SLayer always writes the current schema version.
- **On load**, an older `version` triggers a chain of pure dict→dict converters before Pydantic validation. Hand-edited and older files keep working as the schema evolves.
- **Round-tripping** an older file (load → save) upgrades it on disk to the current schema.
- **Forward tolerance.** A file with a higher `version` than this SLayer knows about loads on a best-effort basis. For `SlayerDataset` and `DatasourceConfig`, unknown fields are ignored. `SlayerQuery` v3 sets `extra="forbid"`, so any unknown field on a future-version query raises a `ValidationError` rather than being silently dropped — this catches typos but means a future schema's new fields will not load on an older SLayer.

The v2→v3 converter drops the legacy `dry_run` / `explain` fields from `SlayerQuery` — they were execution-mode flags that had no business being persisted; pass them as kwargs to `engine.execute(query, dry_run=..., explain=...)` instead. Each migrated query emits one `logger.warning` and one `DeprecationWarning` on first load.

Stored Mode-B text written before the SQL grammar is upgraded on load: double-quoted strings become `'…'`, backslash escapes are re-encoded, adjacent literals are joined, `|` becomes `||`, an object-form time dimension becomes its dotted `dataset.name` text, and an aggregation over a bare field named `distinct` (`sum(distinct)`) reads it as `"distinct"`.

Text the old grammar could not have held (`::`, `||`, `CAST(`, `FILTER (WHERE`, `BETWEEN`) is left unchanged with a warning even under an old `version`, but old-version text using only `''`, `"…"` or backslash escapes is still upgraded, so hand-edit such text only together with its `version`.

A stored datasource granularity whose name the library has since reserved is renamed on load to `<name>_granularity` (`<name>_granularity_<n>` if taken) together with every reference in that datasource's datasets, saved queries and memories, with a logged warning, so update any client that names it.

Persisted documents omit unset (`None`) fields, so a store written by this release still reads on the previous one, except for text already in the new grammar and `ARRAY` fields.

Migrations are defined in `slayer/storage/migrations.py` and apply at the Pydantic-validation layer, so every storage backend (YAML, SQLite, third-party backends registered via `register_storage`, plus HTTP API, MCP server, and dbt importer) gets them automatically.

## Keeping datasets in sync with the live schema

When the live database schema changes — a column drops, a type bucket flips, a table goes away — persisted datasets stop being valid. SLayer surfaces this as a first-class concept: `slayer validate-datasets` returns a structured diff, `slayer ingest` is idempotent (additive only), and query-time DBAPI errors get attributed to the right dataset with a `SchemaDriftError`. See [Schema Drift](schema-drift.md) for the full diff / cascade contract and the `--force-clean` apply path.
