# Reference semantics

SLayer has two distinct expression layers and the rules for what each one accepts are deliberately different. Every field belongs to exactly one of the two modes below; mixing them is rejected at construction time with an actionable error.

## The two-mode table

| Mode | Fields | Parser | Accepts | Rejects |
|---|---|---|---|---|
| **A — SQL** | `Column.sql`, `Column.filter`, each entry of `SlayerModel.filters` | sqlglot | Any valid SQL expression for the underlying dialect — function calls (`json_extract`, `coalesce`, `nullif`, `lower`, `length`, …), arithmetic, `CASE WHEN`, string literals, comparison and boolean operators in SQL spelling (`=`, `<>`, `IS NULL`, `AND`, `OR`, `NOT`, `IN`, `LIKE`). Bare names and `__`-delimited join paths. | Aggregation colon syntax (`revenue:sum`); SLayer transform calls (`cumsum`, `change`, `rank`, …); references to `ModelMeasure` formulas; raw `OVER (...)` window functions inside `Column.filter` / `SlayerModel.filters` (allowed only in `Column.sql`). |
| **B — DSL** | `ModelMeasure.formula`, `SlayerQuery.measures`, `SlayerQuery.filters`, `SlayerQuery.dimensions`, `SlayerQuery.time_dimensions`, `SlayerQuery.order`, `SlayerQuery.main_time_dimension` | Python AST formula parser | Bare names that resolve to a `Column` or `ModelMeasure` on the model; single-dot dotted paths through joins (`customers.regions.name`, `customers.revenue:sum`); aggregation colon syntax (`<col>:<agg>`, `*:count`, parametric forms); transform calls (`cumsum(revenue:sum)`, `rank(revenue:sum, partition_by=region)`); arithmetic / boolean / comparison operators; the SQL `\|\|` concat operator (folded into `concat(...)`); pattern matching via the `like(value, pattern)` scalar (emits the SQL `LIKE` operator — wrap in `not (...)` for `NOT LIKE`); a closed allowlist of scalar functions (matched case-insensitively) — null handling (`nullif`, `coalesce`, `ifnull`), math (`ln`, `log10`, `log2`, `log`, `exp`, `sqrt`, `pow`, `power`, `abs`, `floor`, `ceil`, `ceiling`, `round`, `sign`, `trunc`, `mod`), scalar min/max (`greatest`, `least`), string hygiene (`lower`, `upper`, `trim`, `ltrim`, `rtrim`, `replace`, `substr`, `substring`, `instr`, `length`, `concat`) and `like`, each with a declared argument count that is validated (`coalesce`, `concat`, `greatest` and `least` are variadic; `trunc` takes exactly one argument); `{variable}` placeholders (filters only). | `__`-delimited tokens in user input; raw SQL function calls outside that allowlist (`json_extract`, `date_trunc`, …), and any allowlisted call with the wrong number of arguments; raw `OVER (...)`; bare names that don't resolve to a Column / ModelMeasure / custom aggregation / query alias; `NULL` inside an `in` / `not in` list (use `is null` / `is not null` instead — see below). |

## Identifier resolution

### SQL mode (`Column.sql`, `Column.filter`, `SlayerModel.filters`)

* A bare identifier `col` resolves to the column named `col` on the underlying table or SQL of this model.
* A path `a__b__c.col` resolves through the join graph: `a__b__c` is the SQL table alias produced by walking `model → a → b → c`, and `.col` is the leaf column on the final model. **`__` separates join hops only**; the leaf column always follows a single dot. The flattened form `a__b__c__col` does **not** exist in SQL mode — it appears only inside virtual-model column names produced by the query-backed model wrap (see below).
* Single-dot `t.col` is a literal `<table>.<column>` SQL reference (sqlglot's normal behavior).
* User-supplied multi-dot input (`a.b.c`) is auto-rewritten to `a__b.c` at validation time with a warning.
* Other derived columns of the same model (or of a joined model via `__`) are recursively expanded so chains like `A.ratio = "A.bar / B.foo_normalized"` (where `B.foo_normalized` is itself derived) work.
* A subquery in a Mode-A surface (`col IN (SELECT … FROM other)`, a scalar `= (SELECT … LIMIT 1)`) must be **self-contained**: its own columns bind against the subquery's own `FROM`, never against this model — reference resolution does not reach in to re-qualify them. Correlating back to the outer model is **not** supported (an outer reference inside the subquery is a scope leak, flagged by the scope-closure check SLayer runs over generated SQL under `SLAYER_VALIDATE_SCOPES`).
* `ModelMeasure` names are not visible from SQL mode — saved measures are DSL-only.
* `{variable}` placeholders are substituted into these Mode-A surfaces from the merged variable set (raise-on-missing once any variable is in play; a fully variable-free execution leaves braces as literals; Mode-A- and dialect-aware string escaping, so quoted values round-trip on backslash-escaping backends like MySQL/ClickHouse — DEV-1727). See [Variables in model SQL](models.md#variables-in-model-sql).

### DSL mode (queries + `ModelMeasure.formula`)

* A bare name must resolve to a `Column`, a `ModelMeasure`, or a custom `Aggregation` defined on the model. Filters additionally accept `{variable}` placeholders, query-level measure / transform / expression aliases, and synthesised canonical agg names like `revenue_sum`.
* A single-dot dotted path walks the join graph: `customers.regions.name` traverses `model → customers → regions` and resolves `name` on the regions model. Multi-hop is supported.
* Aggregation colon syntax: `<col>:<agg>` (e.g. `revenue:sum`), `*:count`, `<col>:<agg>(<args>)` (e.g. `price:weighted_avg(weight=quantity)`), and `<dotted.path>:<agg>` for cross-model aggregations.
* Transform calls wrap aggregated refs: `cumsum(revenue:sum)`, `rank(revenue:sum, partition_by=region)`, `change(customers.revenue:sum)`, etc.
* `__`-delimited tokens are rejected in user input — they're reserved for internal join-path aliases. Use single-dot DSL paths instead.

## The internal `__` carve-out

The `Column._validate_name` validator allows `__` inside `Column.name`. This is required by the query-backed model wrap (`_expand_query_backed_model`, via `flat_name` in `slayer/sql/naming.py`), which flattens joined-model columns into virtual-model column names like `stores__name` or `customers__regions__name` — the entire dotted path becomes one SQL identifier on the synthetic table.

`__` is **not** rejected at SlayerQuery / ModelMeasure construction. A user-authored DSL formula or filter that references such a virtual column by name (e.g. a downstream stage filtering on `kpis__total_amount_sum`) needs to remain constructible. Instead, **strict resolution at binding time** catches the cases that are actually wrong: any bare name in a query measure / filter / dimension that doesn't resolve to a `Column` / `ModelMeasure` / custom aggregation / canonical agg alias / query-level alias on the source model raises `ReferenceError`. Typos like `customers__region` (against a model that has `customers` joined to `region`, but no virtual column with that flattened name) are surfaced at execution time, not at construction.

`reject_user_dunder` in `slayer/core/refs.py` is retained as a helper for narrow contexts where `__` is unambiguously wrong (e.g. `SlayerQuery.name`, where `__` would clash with the SQL alias namespace) — it is not applied to free-form formula / filter strings.

## Reference-resolution rules at a glance

1. **Model-side filters** (`Column.filter`, `SlayerModel.filters`) use a sqlglot-based SQL-mode parser, so they accept arbitrary SQL function calls (`json_extract`, `coalesce`, `CASE WHEN`, …) — matching the spec that "models are the boundary that lifts raw SQL tables into the SLayer DSL".

2. **Query-side filters** strict-resolve at enrichment time: any bare name that isn't a `Column` / `ModelMeasure` / custom aggregation / query alias / canonical-agg synthesis raises a clear error.

3. **No predicate promotion.** A query filter that names a windowed `Column` raises with a suggestion to use a rank-family transform (`rank` / `percent_rank` / `dense_rank` / `ntile`) or a multi-stage `source_queries` model. The rank-family transforms cover top-N filtering in pure DSL.

4. **Single reference-resolution surface.** Identifier handling lives in `slayer/core/refs.py`; join walks live in the binder (`slayer/engine/binding.py`), which resolves each hop against the resolved source bundle.

## Examples — accepted and rejected

### `Column.filter` (SQL mode)

Accepted at `Column` construction:

```json
{"name": "active_amount", "sql": "amount", "filter": "json_extract(metadata, '$.active') = 1", "type": "DOUBLE"}
{"name": "amt", "sql": "amount", "filter": "CASE WHEN status = 'active' THEN 1 ELSE 0 END = 1", "type": "DOUBLE"}
{"name": "amt", "sql": "amount", "filter": "customers__regions.name = 'US'", "type": "DOUBLE"}
```

Rejected at `Column` construction:

```json
{"name": "x", "sql": "amount", "filter": "revenue:sum > 100"}        // DSL agg colon syntax
{"name": "x", "sql": "amount", "filter": "cumsum(amount) > 0"}       // DSL transform call
{"name": "x", "sql": "amount", "filter": "row_number() over (...)"}  // raw OVER
```

### `SlayerQuery.filters` (DSL mode)

Accepted at `SlayerQuery` construction:

```json
{"source_model": "orders", "filters": ["revenue:sum > 100"]}
{"source_model": "orders", "filters": ["change(revenue:sum) > 0"]}
{"source_model": "orders", "filters": ["customers.region == 'EU'"]}
{"source_model": "orders", "filters": ["status = '{val}'"], "variables": {"val": "active"}}
```

Rejected at `SlayerQuery` construction:

```json
{"source_model": "orders", "filters": ["row_number() over (...)"]}    // raw OVER
```

Rejected at enrichment:

```json
{"source_model": "orders", "dimensions": ["id"], "filters": ["json_extract(data, '$.x') > 5"]}
// ↑ ReferenceError: raw SQL function calls in DSL mode

{"source_model": "orders", "dimensions": ["id"], "filters": ["unknown_col > 0"]}
// ↑ ReferenceError: 'unknown_col' is not a Column / ModelMeasure on 'orders'

{"source_model": "orders", "dimensions": ["id"], "filters": ["customers__region = 'EU'"]}
// ↑ ReferenceError: 'customers__region' doesn't resolve to any virtual-model column
//   (use single-dot DSL: 'customers.region')
```

### `ModelMeasure.formula` (DSL mode)

Accepted at construction:

```json
{"name": "aov", "formula": "revenue:sum / *:count"}
{"name": "cust_rev", "formula": "customers.revenue:sum"}     // cross-model dotted path
{"name": "growth", "formula": "change(revenue:sum)"}         // transform on agg ref
```

Rejected at enrichment (when the formula is evaluated against a model):

```json
{"name": "bad", "formula": "json_extract(data, '$.x')"}      // raw SQL fn
```

## Scalar functions and dialect semantics

The allowlisted scalars are rendered as typed SQL and then translated to each
backend's own spelling, so one formula stays correct across dialects rather
than being passed through verbatim. `length(x)` emits `LEN(x)` on SQL Server,
`substr(x, 1, 5)` emits `SUBSTRING(x FROM 1 FOR 5)` on Postgres, and
`ifnull(x, 0)` emits `COALESCE(x, 0)` on backends without `IFNULL`.

Five consequences worth knowing:

* **`concat` follows SQL string-concatenation semantics.** On dialects whose
  natural spelling is the `||` operator (Postgres, DuckDB, SQLite), `concat(a, b)`
  emits `a || b`, which yields `NULL` if either operand is `NULL`. That differs
  from those backends' own `CONCAT()` function, which treats `NULL` as an empty
  string. Wrap operands in `ifnull(...)` when you want the NULL-tolerant
  behaviour:

    ```json
    {"filters": ["concat(ifnull(first_name, ''), ifnull(last_name, '')) = 'AdaLovelace'"]}
    ```

* **`greatest` / `least` NULL handling is backend-specific.** They pass through
  to each backend's native form, and those forms disagree on `NULL`: Postgres,
  DuckDB and SQL Server *ignore* `NULL` arguments (returning `NULL` only when
  every argument is `NULL`), while SQLite (scalar `MAX`/`MIN`), MySQL,
  ClickHouse, BigQuery and Snowflake *propagate* `NULL` (any `NULL` argument
  makes the result `NULL`; Snowflake's `GREATEST`/`LEAST` need
  `GREATEST_IGNORE_NULLS`/`LEAST_IGNORE_NULLS` to skip them). SLayer does not normalise this — wrap arguments in
  `ifnull(...)` / `coalesce(...)` if you need one behaviour on every backend.
  On SQL Server, `GREATEST` / `LEAST` require SQL Server 2022 or newer; earlier
  versions reject the generated SQL.

* **`log10` / `log2` keep their single-argument form** on the backends that
  provide one, rather than becoming the generic two-argument `LOG(base, x)`.

* **Argument counts are validated.** Each allowlisted scalar has a fixed arity
  (`round` takes 1 or 2, `substr` / `substring` 2 or 3, `replace` exactly 3;
  `coalesce` and `concat` are variadic). A call with the wrong number is
  rejected with a message naming the function, rather than being silently
  truncated or passed through to fail at the database.

  The bounds are deliberately tight where a wider call would translate into
  something else entirely: `ceiling(x, y)` would emit `CEIL(x, y)` and
  `ceiling(x, y, z)` DuckDB's unrelated `CEIL(x TO z)` rounding form, so
  `ceil` / `ceiling` take exactly one argument. `trunc` takes exactly one
  argument too — a second "decimal places" argument is silently dropped on
  SQLite, a wrong answer rather than an error. `ltrim` / `rtrim` likewise take
  the string only — the "strip these characters" second argument is not
  accepted, because its meaning (a character set vs. an exact substring)
  differs across backends.

* **Aliases render identically to their canonical spelling.** `ceiling` is
  `ceil` and `substring` is `substr`; both spellings are accepted so a formula
  written against either SQL convention binds, and both emit the target's own
  form.

## `NULL` inside an `in` list

`NULL` is rejected inside an `in` / `not in` list. SQL compares it by
three-valued logic, so `status in ('a', None)` never matches on the null, and
`status not in ('a', None)` matches **no rows at all** — the filter silently
returns an empty result instead of "everything except 'a'".

Test for null separately:

```json
{"filters": ["status not in ('new', 'old')", "status is not null"]}
```

## See also

* [Models](models.md) — `Column.sql`, `Column.filter`, model-level filters
* [Queries](queries.md) — `SlayerQuery` field semantics
* [Formulas](formulas.md) — DSL grammar and transforms
* [Joins](models.md#joins) — `__` alias convention for join-path SQL
