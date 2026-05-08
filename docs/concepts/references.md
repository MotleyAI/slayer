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

The `Column._validate_name` validator allows `__` inside `Column.name`. This is required by `_query_as_model`, which flattens joined-model columns into virtual-model column names like `stores__name` or `customers__regions__name`. This is the **only** place in SLayer where `__` connects all the way through to the leaf — because in a virtual model, the entire dotted path becomes one SQL identifier on the synthetic table.

User-authored input (queries, ModelMeasure formulas) cannot use `__`. The user-input validator (`reject_user_dunder` in `slayer/core/refs.py`) fires at SlayerQuery / ModelMeasure construction time. Virtual-model construction in `_query_as_model` bypasses the user-input check by routing through the internal factory.

## What changed in DEV-1369

The implementation drift fixed by DEV-1369:

1. **Model-side filters** (`Column.filter`, `SlayerModel.filters`) used to share the DSL parser with query-side filters. They now use a sqlglot-based SQL-mode parser so they accept arbitrary SQL function calls (`json_extract`, `coalesce`, `CASE WHEN`, …) — matching the spec that "models are the boundary that lifts raw SQL tables into the SLayer DSL".

2. **Query-side filters** used to silently pass through unknown bare names — a filter `unknown_col > 0` would emit raw SQL referencing `unknown_col` on the underlying table. They now strict-resolve at enrichment time: any bare name that isn't a `Column` / `ModelMeasure` / custom aggregation / query alias / canonical-agg synthesis raises a clear error.

3. **Predicate promotion** — the DEV-1336 escape hatch where a query filter naming a `Column` whose `sql` contained a window function auto-promoted to a post-aggregation outer `WHERE` — is removed. The rank-family transforms (`rank` / `percent_rank` / `dense_rank` / `ntile`, DEV-1353) cover top-N filtering in pure DSL, so the escape hatch is redundant. A query filter that names a windowed `Column` now raises with a suggestion to use a rank transform or a multi-stage `source_queries` model.

4. **Reference resolution consolidation** — the four scattered identifier regexes (in `formula.py`, `dbt/converter.py`, `engine/enrichment.py`, `memories/resolver.py`) and the two near-duplicate join walkers in `query_engine.py` (`_resolve_dimension_with_terminal`, `_resolve_cross_model_measure`) collapse into `slayer/core/refs.py` and `_walk_join_chain`.

A sibling issue DEV-1370 will further unify the DSL formula parser and filter parser behind a single `FieldSpec`-producing entry point. That cleanup is deferred to keep this PR's surface tight.

## Examples — accepted and rejected

### `Column.filter` (SQL mode)

```python
# Accepted
Column(name="active_amount", sql="amount", filter="json_extract(metadata, '$.active') = 1")
Column(name="amt", sql="amount", filter="CASE WHEN status = 'active' THEN 1 ELSE 0 END = 1")
Column(name="amt", sql="amount", filter="customers__regions.name = 'US'")

# Rejected at Column construction
Column(name="x", sql="amount", filter="revenue:sum > 100")        # DSL agg colon syntax
Column(name="x", sql="amount", filter="cumsum(amount) > 0")        # DSL transform call
Column(name="x", sql="amount", filter="row_number() over (...)")   # raw OVER
```

### `SlayerQuery.filters` (DSL mode)

```python
# Accepted
SlayerQuery(source_model="orders", filters=["revenue:sum > 100"])
SlayerQuery(source_model="orders", filters=["change(revenue:sum) > 0"])
SlayerQuery(source_model="orders", filters=["customers.region == 'EU'"])
SlayerQuery(source_model="orders", filters=["status = '{val}'"], variables={"val": "active"})

# Rejected at SlayerQuery construction
SlayerQuery(source_model="orders", filters=["json_extract(data, '$.x') > 5"])  # raw SQL fn (caught at enrichment)
SlayerQuery(source_model="orders", filters=["customers__region == 'EU'"])       # __ in user input
SlayerQuery(source_model="orders", filters=["row_number() over (...)"])         # raw OVER

# Rejected at enrichment
query = SlayerQuery(source_model="orders", dimensions=["id"], filters=["unknown_col > 0"])
await engine.execute(query)  # ReferenceError: 'unknown_col' is not a Column or ModelMeasure on 'orders'
```

### `ModelMeasure.formula` (DSL mode)

```python
# Accepted
ModelMeasure(name="aov", formula="revenue:sum / *:count")
ModelMeasure(name="cust_rev", formula="customers.revenue:sum")        # cross-model dotted path
ModelMeasure(name="growth", formula="change(revenue:sum)")             # transform on agg ref

# Rejected at construction
ModelMeasure(name="bad", formula="customers__revenue:sum")             # __ in user input
ModelMeasure(name="bad", formula="json_extract(data, '$.x')")          # raw SQL fn (caught at enrichment)
```

## See also

* [Models](models.md) — `Column.sql`, `Column.filter`, model-level filters
* [Queries](queries.md) — `SlayerQuery` field semantics
* [Formulas](formulas.md) — DSL grammar and transforms
* [Joins](models.md#joins) — `__` alias convention for join-path SQL
