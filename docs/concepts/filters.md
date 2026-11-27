# Filters

Filter expressions define conditions for the query. `filter` is one condition string, or a list of them, all of which must hold; it is serialized in the shape written, and omitted, `null` and `[]` all mean no filter:

```json
{"filter": "orders.status = 'active'"}
{"filter": ["orders.status = 'active'", "orders.amount > 100"]}
```

## Comparison Operators

| Operator | Example |
|----------|---------|
| `=` | `"orders.status = 'active'"` |
| `<>` | `"orders.status <> 'cancelled'"` |
| `>` | `"orders.amount > 100"` |
| `>=` | `"orders.amount >= 100"` |
| `<` | `"orders.amount < 1000"` |
| `<=` | `"orders.amount <= 1000"` |
| `in` | `"orders.status in ('active', 'pending')"` |
| `not in` | `"orders.status not in ('cancelled', 'expired')"` |
| `IS NULL` | `"orders.discount IS NULL"` |
| `IS NOT NULL` | `"orders.discount IS NOT NULL"` |
| `like` | `"customers.name like '%acme%'"` |
| `not like` | `"customers.name not like '%test%'"` |
| `ilike` | `"customers.name ilike '%acme%'"` |
| `between` | `"orders.amount between 100 and 1000"` |
| `is distinct from` | `"orders.discount is distinct from 0"` (NULL-safe `<>`) |
| `is not distinct from` | `"orders.discount is not distinct from 0"` (NULL-safe `=`) |
| `IS TRUE` / `IS FALSE` | `"orders.is_paid IS TRUE"` (NULL is neither) |
| `IS NOT TRUE` / `IS NOT FALSE` | `"orders.is_paid IS NOT TRUE"` (keeps false and NULL) |

The right-hand side of `in` / `not in` must be a non-empty tuple of literal
values (strings, numbers, or booleans) — references and expressions on the
RHS are not supported. Both `(...)` and `[...]` syntax are accepted.

A string compared with a DATE / TIMESTAMP expression is a [time point](time.md#time-points):
`"orders.created_at >= 'last month'"`, `"orders.created_at in '2025-Q1'"` and `"date_trunc('month', orders.created_at) = '2025-03'"`
all work; a date-only string is its midnight (`orders.created_at in '2025-03-01'` is the whole day).
A DATE expression compared with a TIMESTAMP one reads as its midnight instant (`orders.order_date = orders.created_at` holds only at midnight); see [time buckets](time.md#time-buckets).

## Boolean Logic

Use `and`, `or`, `not` within a single filter string:

```json
"filter": [
    "orders.status = 'completed' or orders.status = 'pending'",
    "orders.amount > 100 and orders.amount < 1000"
]
```

Multiple entries in the `filter` list are combined with AND.

## Field vs metric filters

Each filter conjunct (a top-level `and` splits) types as either a **field**
(aggregate-free — masks rows before aggregation, so every metric sees only
passing rows) or a **metric** (any expression legal as a metric in the same
query, including partitioned, windowed, and cross-dataset aggregates — evaluated
at query grain, it prunes result rows without changing surviving values). An
expression valid as both (a plain dimension) is a field; one valid as neither
— e.g. an `or` mixing an aggregate with a row field that isn't a query
dimension — fails with an error naming both failed typings. The same typing
applies to `order` targets, so anything you can compute as a metric you can filter or sort
by.

## Scalar Functions in Filters

Filters in `SlayerQuery.filter` accept the
[portable function library](references.md#the-portable-function-library) — strings
(`lower`, `left`, `split_part`, `concat_ws`, …), math (`round`, `div`, `atan2`, …), null
handling, `greatest` / `least`, regex (`regexp_like`, `x ~ 'p'`), JSON (`json_value`,
`orders.payload ->> 'key'`), the [list functions](#list-fields) `contains`, `contains_any`,
`contains_all` and `length`, and [date functions](references.md#date-and-time-functions) — each with
one meaning on every database that translates it. The SQL `||` concat operator is rewritten to `concat(...)`
automatically.

```json
"filter": [
  "lower(orders.status) = 'active'",
  "trim(customers.name) = 'Smith'",
  "replace(orders.category, ',', '') = 'books'",
  "substr(orders.s, 1, instr(orders.s, ',') - 1) = 'first_token'",
  "coalesce(customers.nickname, customers.name) = 'Ada'",
  "customers.first || ' ' || customers.last = 'jane doe'",
  "regexp_like(orders.sku, '^[A-Z]{3}-[0-9]+$')",
  "json_value(orders.payload, '$.plan') = 'pro'"
]
```

Names are matched case-insensitively — `LOWER(...)` and `lower(...)` both bind — and raw SQL
functions outside the library (`initcap`, `to_date`, …) belong in a field's dialect entry /
`DatasetField.filter` / `SlayerDataset.filters` (Mode A SQL).

## Filtering on Computed Metrics

Filters can reference names of computed metrics — transforms and arithmetic expressions defined in `metrics`. These are applied as post-filters on the outer query, after all transforms are computed. In a query with any transform, a plain aggregate filter such as `sum(orders.amount) > 200` is applied there too, so it never changes the series a transform reads.

When a query metric is renamed via `{"metric": "agg(col)", "alias": "alias"}`, the filter in the same node may reference EITHER form — the raw aggregation expression `agg(col)` OR the user alias `alias`. Both resolve to the user alias, and an aggregation filter is classified as HAVING on the underlying aggregate. Renaming never changes the legal filter form. Two enrichment-time validations apply: (1) a query metric `alias` that collides with a source field on the population is rejected (alias-form filters would otherwise silently bind to the source field); (2) a rename whose canonical alias literally shadows a source field on the same dataset is also rejected (the aggregation filter would otherwise be ambiguous).

Renaming also works for *cross-dataset* aggregated metrics (`{"metric": "sum(customers.revenue)", "alias": "cust_rev"}`). The result-column key is the bare alias (`cust_rev`), however many hops the metric crosses. In any *downstream* stage of a multi-stage query list, the column is exposed under the stage name and the bare user name — type `max(s1.cust_rev)` (or `max(s1.region_pop)`) in stage 2 to consume the value, not the dotted hop-path form. Filters referencing a renamed cross-dataset metric in the SAME stage resolve in both forms — the bare user alias (`filter=["cust_rev > 100"]`) and the raw aggregation form (`"sum(customers.revenue) > 100"`) — and restrict the result rows on the attached value, as does ORDER BY via the bare user alias (`order=[{"field": "cust_rev"}]`).

```json
{
  "metrics": [
    "sum(orders.revenue)",
    {"metric": "change(sum(orders.revenue))", "alias": "rev_change"}
  ],
  "filter": ["rev_change < 0"]
}
```

Transform expressions can also be used **directly in filters** without defining them as fields first:

```json
{
  "filter": ["last(change(sum(orders.revenue))) < 0"]
}
```

Post-filters can be combined with regular filters — base filters (on dimensions/metrics) are applied in the inner query, post-filters on the outer wrapper:

```json
{
  "filter": ["orders.status = 'completed'", "change(sum(orders.revenue)) > 0"]
}
```

## Filters and Auto-Joins

Filters can reference fields from joined datasets, and the planner adds the implied joins automatically — no need to also list the field in `dimensions`. This works for three reference shapes:

- Dotted refs: `"customers.region = 'EU'"` — direct relationship target.
- Multi-hop dotted refs: `"customers.regions.name = 'US'"` — every prefix on the path is added.
- Local derived fields whose own SQL crosses a relationship: e.g. a field `{"name": "is_eu", "expression": "CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END"}` on `orders` referenced as `"filter": ["orders.is_eu = 1"]`. The planner walks the field's expression (recursively, through any local derived-field chain) to find the cross-table aliases and adds the corresponding joins.

The same auto-join logic applies to dataset-level `filters` (always-applied WHERE) and to field-level `filter=` attributes (a `CASE WHEN` value mask that fires in every position).

A query filter that reaches the population root only across a fanning (not provably to-one) hop restricts the population *by association* — a correlated `EXISTS` that counts each population row once, surfaced as a `semi_join_pushed` warning rather than multiplying rows through the join.

A negated condition on a joined dataset does not mean "has none": `not orders.status = 'bad'` keeps a customer with at least one non-bad order, while a customer with no orders fails it (the comparison is NULL) — include those with `or orders.id is null`.

## List fields

A list field `skills` (datatype `ARRAY`, see [Data types](datasets.md#data-types)) is also a relationship hop to its elements: one row per non-NULL element, with the single field `skills.value`. So `contractors.skills.value = 'go'` keeps the contractors having a `go` element, `contractors.skills.value` groups by element with `count(contractors.skills.*)` counting occurrences, and `count(contractors.*)` by `contractors.skills.value` is the number of contractors per skill (no key needed). Every `contractors.skills.value` condition in one query reads the same element, so require several with `contains_all(contractors.skills, ['go', 'sql'])`, not `AND`; absence is `NOT contains(contractors.skills, 'go')`, since `NOT (contractors.skills.value = 'go')` keeps rows with some other element.

```json
{"population": "contractors", "dimensions": ["contractors.skills.value"], "metrics": ["count(contractors.*)"]}
```

A parent aggregate sliced by `contractors.skills.value` and given `partition_by=[]` repeats its total on every element instead.

A whole list groups (a dimension or `partition_by=`), compares (`=`, `<>`, `IS [NOT] DISTINCT FROM`, `contractors.skills IN (['go'], ['sql'])`) and counts distinctly (`count_distinct(contractors.skills)`) as its element set, its distinct non-NULL elements in ascending order (`['go', 'sql', 'go']` and `['sql', 'go']` are one group, shown as `['go', 'sql']`, and `[NULL]` equals `[]`), with a `whole_list` warning naming `skills.value` for grouping and distinct counting, while a raw-rows query shows each list as stored.

What a list can't do, each a typed error naming the remedy:

- be compared with a scalar (use `contains` / `contains_any`, or compare `contractors.skills.value`), or ordered (`<`, `>`, `BETWEEN`, an `order` key); `IS NULL` / `IS NOT NULL` work;
- take an aggregation that [Aggregation eligibility](datasets.md#aggregation-eligibility) does not allow a list, `collect` and custom aggregations included;
- hold a NULL element in a list literal (`['go', NULL]`);
- have `Opaque` elements and still be grouped, compared, counted distinctly, measured by `length` or read element by element: such a list supports only NULL tests and `count`.

A text value against a numeric list is converted like any mixed-type comparison, so `contains(contractors.scores, '5')` tests for the element `5`.

Element access composes like any relationship hop: through a to-one relationship (`contractors.skills.value` from a dataset joined to `contractors`), on a derived list field, and on a list column of a query stage, including a stage's `collect` result. One query may aggregate the parent's rows (`count(contractors.*)`) and the elements (`count(contractors.skills.value)`), each over its own rows. Saving a dataset whose list field has the name of a hop reachable from it (an edge or joined-dataset name) fails.

`inspect`, `search` and `datasets_summary` show a list field's datatype as `Array<String>` with its operations, and its element samples once profiled.

`collect(x)` goes the other way: the distinct non-NULL values of `x` in each cell, sorted, and `[]` for a cell with none. `x` is a field, a list's elements or an expression, of the query's dataset or a joined one (`collect(orders.status)` by customer is each customer's order statuses), and a collected list stays a list in a later stage, with the same functions and elements.

The [list functions](references.md#lists) test a whole list, and [Database support](../database-support.md) names the databases that run lists.

## Window functions in filters

A filter may hold a window function. A row-level one (no aggregate among its operands) is a column of its dataset,
computed before the query's filters, so `ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at) = 1`
keeps each region's first order whatever else the query filters. A measure-level one (`RANK() OVER (ORDER BY
SUM(orders.amount) DESC) = 1`) filters cells, like a transform. See
[SQL window functions](expressions.md#sql-window-functions-over). Under `conformance="ossie-l3"` a window in the
filter is Ossie's `E_WINDOW_IN_WHERE`.

## Variables

Filters support `{variable_name}` placeholders, substituted from the query's `variables` dict. This keeps filter templates reusable and avoids string concatenation in client code.

The same placeholders work in metric expressions, computed- and time-dimension expressions, `order` expressions and `date_range` bounds, where a placeholder is written unquoted (`"date_range": ["{start}", "{end}"]`; `"metrics": ["sum(orders.amount) * {k} / 100"]`), and an unnamed entry is named from its template (`orders.amount_sum_k_100`), so result keys never depend on the values; `{? ... ?}` blocks and a placeholder naming a time dimension's granularity or standing for its whole dimension is rejected.

```json
{
  "population": "orders",
  "metrics": ["count(orders.*)"],
  "filter": ["orders.status = '{status}' AND orders.amount > {min_amount}"],
  "variables": {"status": "completed", "min_amount": 100}
}
```

This produces the filter `orders.status = 'completed' AND orders.amount > 100`.

- Variable names must be alphanumeric + underscore (`[a-zA-Z_][a-zA-Z0-9_]*`)
- Values must be strings, numbers, booleans or **lists**, and every value binds as a literal: **you write the surrounding quotes** for a string (`orders.status = '{status}'`, an embedded `'` is doubled), an unquoted placeholder takes a number (a numeric string included) or a boolean, and a placeholder in a name position (`sum({col})`, `customers.{c}`, `{f}(x)`, `partition_by={x}`) is rejected, so no value can change the expression's structure; non-finite floats (`nan`/`inf`) are rejected.
- **List values render an `IN`-list.** A list variable powers an `in` / `not in` filter — write the parentheses and omit per-element quotes (each string element is auto-quoted):

    ```json
    {
      "population": "orders",
      "metrics": ["count(orders.*)"],
      "filter": ["orders.region in ({regions})"],
      "variables": {"regions": ["US", "CA"]}
    }
    ```

    An empty list there raises, and a literal `x IN []` is rejected; an empty list binds only as a list-function argument (`contains_all(contractors.skills, {wanted})`).
- `{{` and `}}` produce literal `{` and `}`; a brace pair holding only digits, commas and spaces (`'{1,2}'`, `'{}'`, `'[0-9]{3}'`) is literal text in any string
- Undefined variables raise an error
- The same `{variable}` mechanism also works in the **raw-SQL (Mode A) surfaces** of a dataset — see [Variables in dataset SQL](datasets.md#variables-in-dataset-sql).

!!! note "Literals only"
    A query carries no raw SQL: every value it supplies, in query fields and in a dataset's raw-SQL (Mode A) surfaces alike, binds as a literal, and a value that would change the SQL's structure is rejected.

### Variables passed as a runtime kwarg

Every execution entry point also accepts a `variables=` runtime kwarg that **always wins**, even over a stage's own `variables` dict and a query-backed dataset's `query_variables` defaults:

```python
# Python
await engine.execute(slayer_query, variables={"region": "EU"})
await engine.execute("monthly_revenue", variables={"region": "EU"})  # run-by-name
```

```bash
# CLI
slayer query @query.json --variables region=EU --variables threshold=100
slayer query monthly_revenue --variables region=EU
```

```json
// REST POST /query
{"population": "orders", "metrics": ["count(orders.*)"], "variables": {"region": "EU"}}
{"name": "monthly_revenue", "variables": {"region": "EU"}}  // run-by-name
```

Precedence (highest first):

1. Runtime kwarg (`variables=`)
2. Stage `SlayerQuery.variables`
3. Outer-query `.variables` (when a query-backed dataset is used as `population`)
4. Dataset defaults (`dataset.query_variables`)

Unknown kwarg variables (not referenced in any filter) are silently ignored.
