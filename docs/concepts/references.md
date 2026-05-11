# Reference semantics

SLayer has two distinct expression layers and the rules for what each one accepts are deliberately different. Every field belongs to exactly one of the two modes below; mixing them is rejected at construction time with an actionable error.

## The two-mode table

| Mode | Fields | Parser | Accepts | Rejects |
|---|---|---|---|---|
| **A — SQL** | `Column.sql`, `Column.filter`, each entry of `SlayerModel.filters` | sqlglot | Any valid SQL expression for the underlying dialect — function calls (`json_extract`, `coalesce`, `nullif`, `lower`, `length`, …), arithmetic, `CASE WHEN`, string literals, comparison and boolean operators in SQL spelling (`=`, `<>`, `IS NULL`, `AND`, `OR`, `NOT`, `IN`, `LIKE`). Bare names and `__`-delimited join paths. | Aggregation colon syntax (`revenue:sum`); SLayer transform calls (`cumsum`, `change`, `rank`, …); references to `ModelMeasure` formulas; raw `OVER (...)` window functions inside `Column.filter` / `SlayerModel.filters` (allowed only in `Column.sql`). |
| **B — DSL** | `ModelMeasure.formula`, `SlayerQuery.measures`, `SlayerQuery.filters`, `SlayerQuery.dimensions`, `SlayerQuery.time_dimensions`, `SlayerQuery.order`, `SlayerQuery.main_time_dimension` | Python AST formula parser | Bare names that resolve to a `Column` or `ModelMeasure` on the model; single-dot dotted paths through joins (`customers.regions.name`, `customers.revenue:sum`); aggregation colon syntax (`<col>:<agg>`, `*:count`, parametric forms); transform calls (`cumsum(revenue:sum)`, `rank(revenue:sum, partition_by=region)`); arithmetic / boolean / comparison operators; `LIKE` / `NOT LIKE`; `{variable}` placeholders (filters only). | `__`-delimited tokens in user input; raw SQL function calls (`json_extract`, `coalesce`, …); raw `OVER (...)`; bare names that don't resolve to a Column / ModelMeasure / custom aggregation / query alias. |

## Identifier resolution

### SQL mode (`Column.sql`, `Column.filter`, `SlayerModel.filters`)

* A bare identifier `col` resolves to the column named `col` on the underlying table or SQL of this model.
* A path `a__b__c.col` resolves through the join graph: `a__b__c` is the SQL table alias produced by walking `model → a → b → c`, and `.col` is the leaf column on the final model. **`__` separates join hops only**; the leaf column always follows a single dot. The flattened form `a__b__c__col` does **not** exist in SQL mode — it appears only inside virtual-model column names produced by `_query_as_model` (see below).
* Single-dot `t.col` is a literal `<table>.<column>` SQL reference (sqlglot's normal behavior).
* User-supplied multi-dot input (`a.b.c`) is auto-rewritten to `a__b.c` at validation time with a warning.
* Other derived columns of the same model (or of a joined model via `__`) are recursively expanded so chains like `A.ratio = "A.bar / B.foo_normalized"` (where `B.foo_normalized` is itself derived) work.
* `ModelMeasure` names are not visible from SQL mode — saved measures are DSL-only.

### DSL mode (queries + `ModelMeasure.formula`)

* A bare name must resolve to a `Column`, a `ModelMeasure`, or a custom `Aggregation` defined on the model. Filters additionally accept `{variable}` placeholders, query-level measure / transform / expression aliases, and synthesised canonical agg names like `revenue_sum`.
* A single-dot dotted path walks the join graph: `customers.regions.name` traverses `model → customers → regions` and resolves `name` on the regions model. Multi-hop is supported.
* Aggregation colon syntax: `<col>:<agg>` (e.g. `revenue:sum`), `*:count`, `<col>:<agg>(<args>)` (e.g. `price:weighted_avg(weight=quantity)`), and `<dotted.path>:<agg>` for cross-model aggregations.
* Transform calls wrap aggregated refs: `cumsum(revenue:sum)`, `rank(revenue:sum, partition_by=region)`, `change(customers.revenue:sum)`, etc.
* `__`-delimited tokens are rejected in user input — they're reserved for internal join-path aliases. Use single-dot DSL paths instead.

## The internal `__` carve-out

The `Column._validate_name` validator allows `__` inside `Column.name`. This is required by `_query_as_model`, which flattens joined-model columns into virtual-model column names like `stores__name` or `customers__regions__name` — the entire dotted path becomes one SQL identifier on the synthetic table.

`__` is **not** rejected at SlayerQuery / ModelMeasure construction. A user-authored DSL formula or filter that references such a virtual column by name (e.g. a downstream stage filtering on `kpis__total_amount_sum`) needs to remain constructible. Instead, **strict resolution at enrichment time** catches the cases that are actually wrong: any bare name in a query measure / filter / dimension that doesn't resolve to a `Column` / `ModelMeasure` / custom aggregation / canonical agg alias / query-level alias on the source model raises `ReferenceError`. Typos like `customers__region` (against a model that has `customers` joined to `region`, but no virtual column with that flattened name) are surfaced at execution time, not at construction.

`reject_user_dunder` in `slayer/core/refs.py` is retained as a helper for narrow contexts where `__` is unambiguously wrong (e.g. `SlayerQuery.name`, where `__` would clash with the SQL alias namespace) — it is not applied to free-form formula / filter strings.

## Reference-resolution rules at a glance

1. **Model-side filters** (`Column.filter`, `SlayerModel.filters`) use a sqlglot-based SQL-mode parser, so they accept arbitrary SQL function calls (`json_extract`, `coalesce`, `CASE WHEN`, …) — matching the spec that "models are the boundary that lifts raw SQL tables into the SLayer DSL".

2. **Query-side filters** strict-resolve at enrichment time: any bare name that isn't a `Column` / `ModelMeasure` / custom aggregation / query alias / canonical-agg synthesis raises a clear error.

3. **No predicate promotion.** A query filter that names a windowed `Column` raises with a suggestion to use a rank-family transform (`rank` / `percent_rank` / `dense_rank` / `ntile`) or a multi-stage `source_queries` model. The rank-family transforms cover top-N filtering in pure DSL.

4. **Single reference-resolution surface.** Identifier handling lives in `slayer/core/refs.py`; join walks live in `_walk_join_chain` in the engine.

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

## See also

* [Models](models.md) — `Column.sql`, `Column.filter`, model-level filters
* [Queries](queries.md) — `SlayerQuery` field semantics
* [Formulas](formulas.md) — DSL grammar and transforms
* [Joins](models.md#joins) — `__` alias convention for join-path SQL
