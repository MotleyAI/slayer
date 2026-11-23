# Variable Substitution

A semantic layer earns its keep when one definition serves many questions. Often
the only thing that changes between two questions is a *value* — a region, a date
window, a price threshold. SLayer lets callers supply those values as
`{variable}` placeholders, so a single dataset or query is reused across them
without any string-building on the client side.

Substitution spans SLayer's [two expression layers](../../concepts/references.md):

- **Mode B — the DSL.** A `{var}` in a query's `filter` binds a literal value
  from the query's `variables` (`'{s}'` for text, a bare number or boolean, a list
  as a whole `IN` list), never a name. Metric expressions, computed
  dimensions, `order` expressions, `date_range` bounds and the population's saved
  metrics take placeholders too (`"metrics": ["sum(orders.order_total) * {rate} / 100"]`).
- **Mode A — raw SQL.** A stored dataset's `sql` and `filters`, and every `DatasetField`'s
  `sql` and `filter`, are raw SQL over the underlying table. A `{var}` here binds
  as a literal of that SQL, so it takes effect **before** aggregation — inside a
  `WHERE`, a `CASE WHEN`, or a projected expression.

That "before aggregation, inside the SQL" placement is the whole point of Mode-A
substitution: it expresses things a query-time filter can't. A return-rate
decomposition that scans a this-year window and a year-shifted last-year window
in the same dataset, for instance, needs the date bounds applied *inside* the raw
SQL — not as a filter on the result.

## The four raw-SQL surfaces

| Surface | What lands here |
| -- | -- |
| `SlayerDataset.filters` | a dataset-level always-applied `WHERE` predicate |
| `SlayerDataset.sql` | the raw SQL body of an `sql`-mode dataset (WHERE, projected scalars, anywhere) |
| a field's dialect entry | a field's row-level SQL in its datasource's dialect |
| `DatasetField.filter` | a field's `CASE WHEN` conditional (a per-metric windowed sum with scalar bounds) |

A stored dataset `orders_over_floor` (`sql_table: orders`) with the filter
`order_total >= {floor}` is queried by name:

!!! note "Illustrative schema"
    The companion notebook saves `orders_over_floor`; it is not part of the demo datasets.

```json
{
  "population": "orders_over_floor",
  "metrics": ["count(orders_over_floor.*)"],
  "variables": {"floor": 50}
}
```

The `{floor}` binds as the literal of the dataset's `WHERE` before the query runs.
Write the surrounding quotes yourself for string values (`region = '{region}'`);
the value is spelled into that literal, so an embedded quote can't break out of it.
The one place you *don't* write quotes is an `IN`-list — `region IN ({regions})`
takes a list and quotes each element for you; a scalar there is a one-element
list. A placeholder in any other position — a table or column name, a function
name, a comment — is rejected when the dataset is saved.

## Optional blocks — a filter that vanishes when its value is absent

The surfaces above **require** their variables. But a Cube `FILTER_PARAMS`
pushdown is *optional*: it filters when the caller supplies a value and becomes a
no-op when they don't. SLayer expresses that with an **optional block**
`{? ... ?}` on a Mode-A surface — it renders (parenthesised) when every `{var}`
inside is supplied, and collapses to the neutral `(1=1)` otherwise. A **list**
value renders an injection-safe `IN`-list (write the parens; *string* elements
are auto-quoted, numbers stay unquoted). Open the `WHERE` with `1=1` so the
collapse leaves valid SQL.

A stored `sql`-mode dataset `orders_by_store` with the body
`SELECT o.id, s.name AS store_name FROM orders o LEFT JOIN stores s ON o.store_id = s.id WHERE 1=1 AND {? s.name IN ({stores}) ?}`
is queried by name:

!!! note "Illustrative schema"
    The companion notebook saves `orders_by_store`; it is not part of the demo datasets.

```json
{
  "population": "orders_by_store",
  "metrics": ["count(orders_by_store.*)"],
  "variables": {"stores": ["Brooklyn", "Philadelphia"]}
}
```

With `stores` supplied this renders `... AND (s.name IN ('Brooklyn', 'Philadelphia'))`;
omit `stores` and the whole block becomes `... AND (1=1)`, counting every order.
This is exactly how `slayer import-cube` represents an optional Cube
`FILTER_PARAMS` pushdown — see [Importing Cube definitions](../../cube/cube_import.md#filter_params-pushdowns).

## Precedence

The same variable may be set in several places. Highest priority wins:

**runtime `variables=` kwarg > stage `variables` > outer query `variables` (when a query-backed dataset is the `population`) > dataset `query_variables`**

`query_variables` on a dataset are the lowest-priority *defaults*, so a
parameterized dataset can ship with sensible values and still be overridden per
query or per call.

## Contract

- **Raise on missing — once any variable is in play.** As soon as one variable is
  supplied, every *required* `{var}` must resolve or execution raises. A
  parameterized dataset is meant to fail loudly without its value, not silently
  match nothing. (The exception is a `{var}` **inside a `{? ... ?}` block** —
  when it's absent the whole block collapses to `(1=1)` instead of raising.)
- **Fully variable-free executions treat braces as literals**, so a raw brace
  literal such as a Postgres array `'{1,2,3}'` survives untouched. Use `{{` / `}}`
  for literal braces in a dataset that *does* use variables.
- **Literals only.** Every value — from a query, a stage or a dataset default —
  binds as a literal, spelled in the datasource dialect's own escaping. A value
  that is no literal in its slot (`"0 OR 1=1"` for a bare `{floor}`) is
  rejected before any SQL runs.

Substitution applies to every dataset a query reads, each with its own defaults
under the query's variables.

See [Variables in dataset SQL](../../concepts/datasets.md#variables-in-dataset-sql) and
[Variables](../../concepts/queries.md#variables) for the reference
details.

---

See the [companion notebook](variable_substitution_nb.ipynb) for runnable code
demonstrating Mode B, all four Mode-A surfaces, precedence, missing-variable
behavior, and escaping — executed end-to-end against the Jaffle Shop demo.
