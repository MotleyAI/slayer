# Variable Substitution

A semantic layer earns its keep when one definition serves many questions. Often
the only thing that changes between two questions is a *value* — a region, a date
window, a price threshold. SLayer lets callers supply those values as
`{variable}` placeholders, so a single model or query is reused across them
without any string-building on the client side.

Substitution spans SLayer's [two expression layers](../../concepts/references.md):

- **Mode B — the DSL.** A query's `filters` reference dimension and measure
  names. A `{var}` here is filled from the query's `variables` and applied as a
  post-aggregation predicate on named entities.
- **Mode A — raw SQL.** A model's `sql` and `filters`, and every `Column`'s `sql`
  and `filter`, are raw SQL over the underlying table. A `{var}` here is injected
  directly into that SQL, so it takes effect **before** aggregation — inside a
  `WHERE`, a `CASE WHEN`, or a projected expression.

That "before aggregation, inside the SQL" placement is the whole point of Mode-A
substitution: it expresses things a query-time filter can't. A return-rate
decomposition that scans a this-year window and a year-shifted last-year window
in the same model, for instance, needs the date bounds applied *inside* the raw
SQL — not as a filter on the result.

## The four raw-SQL surfaces

| Surface | What lands here |
| -- | -- |
| `SlayerModel.filters` | a model-level always-applied `WHERE` predicate |
| `SlayerModel.sql` | the raw SQL body of an `sql`-mode model (WHERE, projected scalars, anywhere) |
| `Column.sql` | a column's row-level SQL expression |
| `Column.filter` | a column's `CASE WHEN` conditional (a per-measure windowed sum with scalar bounds) |

```json
{
  "source_model": {
    "name": "orders_over_floor",
    "sql_table": "orders",
    "data_source": "jaffle_shop",
    "filters": ["order_total >= {floor}"]
  },
  "measures": ["*:count"],
  "variables": {"floor": 50}
}
```

The `{floor}` is substituted into the model's `WHERE` before the query runs.
Write the surrounding quotes yourself for string values (`region = '{region}'`);
SLayer escapes the value so an embedded quote can't break out of the literal.

## Optional blocks — a filter that vanishes when its value is absent

The surfaces above **require** their variables. But a Cube `FILTER_PARAMS`
pushdown is *optional*: it filters when the caller supplies a value and becomes a
no-op when they don't. SLayer expresses that with an **optional block**
`{? ... ?}` on a Mode-A surface — it renders (parenthesised) when every `{var}`
inside is supplied, and collapses to the neutral `(1=1)` otherwise. A **list**
value renders an injection-safe `IN`-list (write the parens; per-element quotes
are added for you). Open the `WHERE` with `1=1` so the collapse leaves valid SQL.

```json
{
  "source_model": {
    "name": "orders_by_store",
    "data_source": "jaffle_shop",
    "sql": "SELECT o.id, s.name AS store_name FROM orders o LEFT JOIN stores s ON o.store_id = s.id WHERE 1=1 AND {? s.name IN ({stores}) ?}"
  },
  "measures": ["*:count"],
  "variables": {"stores": ["Brooklyn", "Philadelphia"]}
}
```

With `stores` supplied this renders `... AND (s.name IN ('Brooklyn', 'Philadelphia'))`;
omit `stores` and the whole block becomes `... AND (1=1)`, counting every order.
This is exactly how `slayer import-cube` represents an optional Cube
`FILTER_PARAMS` pushdown — see [Importing Cube definitions](../../cube/cube_import.md#filter_params-pushdowns).

## Precedence

The same variable may be set in several places. Highest priority wins:

**runtime `variables=` kwarg > query `variables` > model `query_variables`**

`query_variables` on a model are the lowest-priority *defaults*, so a
parameterized model can ship with sensible values and still be overridden per
query or per call.

## Contract

- **Raise on missing — once any variable is in play.** As soon as one variable is
  supplied, every `{var}` must resolve or execution raises. A parameterized model
  is meant to fail loudly without its value, not silently match nothing.
- **Fully variable-free executions treat braces as literals**, so a raw brace
  literal such as a Postgres array `'{1,2,3}'` survives untouched. Use `{{` / `}}`
  for literal braces in a model that *does* use variables.
- **Trusted input.** Values are treated as trusted, not attacker-controlled. The
  Mode-A escaping is not dialect-aware, so avoid untrusted values on
  backslash-escaping backends like MySQL.

Substitution currently applies to a query's **direct source model**; nested
`source_queries` stages, join targets, and cross-model targets are a tracked
follow-up.

See [Variables in model SQL](../../concepts/models.md#variables-in-model-sql) and
[Filter Variables](../../concepts/queries.md#filter-variables) for the reference
details.

---

See the [companion notebook](variable_substitution_nb.ipynb) for runnable code
demonstrating Mode B, all four Mode-A surfaces, precedence, missing-variable
behavior, and escaping — executed end-to-end against the Jaffle Shop demo.
