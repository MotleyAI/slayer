# Reference semantics

SLayer has two distinct expression layers and the rules for what each one accepts are deliberately different. Every field belongs to exactly one of the two modes below; mixing them is rejected at construction time with an actionable error.

## The two-mode table

| Mode | Fields | Parser | Accepts | Rejects |
|---|---|---|---|---|
| **A — SQL** | a field `expression`'s entry for the datasource's dialect, `DatasetField.filter`, each entry of `SlayerDataset.filters` | sqlglot | Any valid SQL expression for the underlying dialect — function calls (`json_extract`, `coalesce`, `nullif`, `lower`, `length`, …), arithmetic, `CASE WHEN`, string literals, comparison and boolean operators in SQL spelling (`=`, `<>`, `IS NULL`, `AND`, `OR`, `NOT`, `IN`, `LIKE`). A bare name is a source column; a path from the dataset is a field (`orders.status`, `customers.regions.name`). | Aggregations (`sum(revenue)`); SLayer transform calls (`cumsum`, `change`, `rank`, …); references to `DatasetMetric` expressions; a window function in `SlayerDataset.filters` (it reads the rows the filters define: a filter cycle); the legacy `__`-delimited split-alias qualifier (`customers__regions.name`) — now a hard error (`LegacyDunderAliasError`). |
| **B — DSL** | a field `expression`'s portable `sql` (a bare name there is a source column, a path a field), `DatasetMetric.expression`, `SlayerQuery.metrics`, `SlayerQuery.filter`, `SlayerQuery.dimensions`, `SlayerQuery.time_dimensions`, `SlayerQuery.order`, `SlayerQuery.main_time_dimension` | SQL-compatible grammar | Bare names that resolve to a `DatasetField` or `DatasetMetric` on the dataset; single-dot dotted paths through relationships (`customers.regions.name`, `sum(customers.revenue)`); aggregations (`sum(revenue)`, `count(*)`, `percentile(price, p=0.9)`, parametric forms), including row-level expression sources over the dataset and its to-one relationships (`sum(amount - customers.discount)`); transform calls (`cumsum(sum(revenue))`, `rank(sum(revenue), partition_by=region, direction='desc')`); arithmetic / boolean / comparison operators; the SQL `\|\|` concat operator (folded into `concat(...)`); SQL forms — `CASE`, `CAST` / `TRY_CAST` / `::`, `BETWEEN`, `IS [NOT] DISTINCT FROM`, `IS [NOT] TRUE` / `IS [NOT] FALSE` (NULL is neither), `[NOT] LIKE` / `ILIKE`, the regex operators `~` / `~*` / `!~` / `!~*`, the JSON arrows `->` / `->>`, `EXTRACT`, `POSITION`, `SUBSTRING … FROM … FOR`, `INTERVAL` / `DATE` / `TIMESTAMP` / `TIMESTAMP_NTZ` literals, `CURRENT_DATE`, `FILTER (WHERE …)`, [window functions](expressions.md#sql-window-functions-over) (`… OVER (…)`), plus the `like(value, pattern)` / `ilike(value, pattern)` scalars; the [portable function library](#the-portable-function-library) (matched case-insensitively) — null handling (`nullif`, `coalesce`, `ifnull`, `zeroifnull`, `nullifzero`), the conditional `iif(cond, then, else)`, math (`ln`, `log10`, `log2`, `log`, `exp`, `sqrt`, `pow`, `power`, `abs`, `floor`, `ceil`, `ceiling`, `round`, `sign`, `trunc`, `mod`, `div`, `pi`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `cbrt`, `radians`, `degrees`), scalar min/max (`greatest`, `least`), strings (`lower`, `upper`, `trim`, `ltrim`, `rtrim`, `replace`, `substr`, `substring`, `instr` / `strpos`, `length`, `concat`, `left`, `right`, `starts_with`, `ends_with`, `contains`, `lpad`, `rpad`, `reverse`, `repeat`, `split_part`, `concat_ws`, `chr`, `ascii`), regex (`regexp_like`, `regexp_extract`, `regexp_replace`, `regexp_count`), [Ossie's spellings](#ossie-spellings), JSON (`json_value` and its aliases `json_extract`, `json_extract_scalar`, `json_extract_path_text`, `jsonb_extract_path_text`), [lists](#lists) (`contains` and `length` over a list, `contains_any`, `contains_all`, the array literal `['a', 'b']`), `like`, and [date functions](#date-and-time-functions) (`date_part`, `date_diff`, `date_add`, `date_trunc`, `current_date`, `now`), each with a declared argument count that is validated (`coalesce`, `concat`, `concat_ws`, `greatest` and `least` are variadic); `{variable}` placeholders supplying literal values. | identifiers using the reserved `__slayer_` prefix; raw SQL function calls outside that library (`initcap`, `to_date(s, format)`, `strftime`, JSON builders, …), and any library call with the wrong number of arguments; bare names that don't resolve to a DatasetField / DatasetMetric / custom aggregation / query alias; a function under `OVER` other than Ossie's ranking, offset and aggregation functions; `NULL` inside an `in` / `not in` list (use `is null` / `is not null` instead — see below); `\|` (use `\|\|`), `//` (use `div(a, b)` or `floor(a / b)`), `TRIM(BOTH \| LEADING \| TRAILING …)` (use `trim` / `ltrim` / `rtrim`), two-argument scalar `min` / `max` (use `least` / `greatest`), `SIMILAR TO`, `REGEXP` / `RLIKE` (use `~`), a bare `->` (end the chain with `->>`), `TIME '…'` literals. |

## Identifier resolution

### Row definitions (a field's `expression` and `filter`, `SlayerDataset.filters`)

* A bare identifier `col`, or `source.col`, resolves to the column named `col` on the underlying table or SQL of this dataset, whatever fields the dataset declares (`source.col` reads a field when the dataset declares a field named `source`).
* A path from the dataset reads a field: `orders.status` on dataset `orders`, `customers.region` through a relationship.
* A relationship path is written with **dots**: `a.b.c.col` walks `dataset → a → b → c` and resolves the leaf field `col` on the final dataset. Dots separate relationship hops; the leaf field follows the final dot. The generated SQL emits this walk with a `__`-delimited internal table alias (`LEFT JOIN … AS a__b__c`), but that spelling is an emitted-SQL detail you never write. A `__` token in a qualifier is treated as part of **one exact name**, never split into a hop walk — `customers__region.label` means "the directly-joined dataset literally named `customers__region`, field `label`". The legacy `__`-delimited split-alias input form (`customers__regions.name` meant as a `customers → regions` walk) is a hard error (`LegacyDunderAliasError`), not auto-converted. The flattened form `a__b__c__col` appears only inside virtual-dataset column names produced by the query-backed dataset wrap (see below).
* A path segment resolves as an incident **edge name first, then a neighbour dataset name**, and traverses the edge in **either direction** (any declared relationship is reachable in reverse — see [Bidirectional traversal](datasets.md#bidirectional-traversal)); a hop spanned by two or more edges fails closed unless an edge-name segment disambiguates it. A named edge's hop is canonically spelled by its edge name, so result keys use it whichever spelling is typed.
* Single-dot `t.col` is a literal `<table>.<column>` SQL reference (sqlglot's normal behavior).
* Multi-dot input (`a.b.c`) stays dotted through to resolution — there is no `__` rewrite (the old dotted→`__` normalization is retired).
* Fields read by path are recursively expanded, so chains like `A.ratio = "A.bar / B.foo_normalized"` (where `A.bar` and `B.foo_normalized` are themselves derived) work.
* A subquery in a Mode-A surface (`col IN (SELECT … FROM other)`, a scalar `= (SELECT … LIMIT 1)`) must be **self-contained**: its own columns bind against the subquery's own `FROM`, never against this dataset — reference resolution does not reach in to re-qualify them. Correlating back to the outer dataset is **not** supported (an outer reference inside the subquery is a scope leak, flagged by the scope-closure check SLayer runs over generated SQL under `SLAYER_VALIDATE_SCOPES`).
* `DatasetMetric` names are not visible from SQL mode — saved metrics are DSL-only.
* `{variable}` placeholders are substituted into these Mode-A surfaces from the merged variable set (raise-on-missing once any variable is in play; a fully variable-free execution leaves braces as literals; Mode-A- and dialect-aware string escaping, so quoted values round-trip on backslash-escaping backends like MySQL/ClickHouse). See [Variables in dataset SQL](datasets.md#variables-in-dataset-sql).

### DSL mode (queries + `DatasetMetric.expression`)

* **A query reference is anchored at its first segment**, which names a dataset of the population's datasource, a stage of the same query list (a stage precedes a same-named dataset) or `time_spine`; the rest is an explicit route from it, and `source` is an ordinary first segment. `orders.amount`, `orders.revenue` (a saved metric), `count(orders.*)` and `customers.regions.name` all start at a dataset. A relationship name never starts a reference: a role is a route from its dataset (`orders.buyer.name`).
* An **aggregation is read from its own anchor**, never through the population: `sum(orders.amount)` reads `orders` whatever the population. When its source's references start at different datasets, it is read from the smallest dataset determining each of them along to-one paths (`sum(customers.discount * products.price)` reads `order_lines`); two such datasets tie and are refused, naming each one's spelling.
* The **reading dataset** reaches the anchor by its **only to-one path**, else its **only route**: an aggregation's anchor for its source, its home for its grain dimensions and the filters restricting it, the population for every other reference. Each spelling is routed on its own, so `customers.regions.managers.name` and `managers.name` are two references even when the population reaches one column. A reference anchored where the aggregation's source is anchored goes back along the source's own path (`orders.status` beside `sum(orders.billing_customer.spend)`). Two or more to-one paths, or none and several routes, fail with `UnresolvableDimensionRelationshipError` naming each explicit spelling, even when one of them is a direct relationship. With no route the anchor is an [unrelated dataset](cross-dataset-queries.md#cross-dataset-metrics).
* Each hop of the explicit route resolves as an incident **edge name first, then a neighbour dataset name**, in **either direction** ([bidirectional traversal](datasets.md#bidirectional-traversal)); an ambiguous hop (parallel edges, no edge-name segment) fails closed naming the candidates, and a broken route is never silently repaired.
* **A bare name in a query is a metric**: an alias declared by an entry of the same query, referenceable in filters, order and later stages, else a [model-scoped metric](semantic-models.md#model-scoped-metrics) of the datasource. Any other bare name fails with `UnknownMetricError` suggesting each qualified field and metric of that name; `count(*)` without a population fails with `UnqualifiedRowCountError`. A pre-2.0 root-relative spelling in a query that names its population still converts, with a deprecation (see [Upgrading](../upgrading/2.0.md#root-relative-references)).
* **A stored definition reads its own dataset**: a `DatasetMetric.expression` and a custom aggregation's defaults resolve bare names against their dataset and dotted paths from it; in a metric's `expression`, `source.col` reads a source column unless the dataset declares a field named `source`.
* `{variable}` placeholders bind literal values in every Mode-B expression — metrics, filters, computed and time dimensions, `date_range` bounds, order and saved metric expressions.
* Aggregations: `<agg>(<dataset>.<col>)` (e.g. `sum(orders.revenue)`), `count(<dataset>.*)`, `<agg>(<col>, <args>)` (e.g. `weighted_avg(orders.price, weight=orders.quantity)`) — see [Aggregation syntax](#aggregation-syntax).
* Transform calls wrap aggregated refs: `cumsum(sum(orders.revenue))`, `rank(sum(orders.revenue), partition_by=customers.region, direction='desc')`, `change(sum(customers.revenue))`, etc.
* **Result keys** are the reference as written, its route spelled canonically, never the population: `sum(orders.amount)` keys `orders.amount_sum` under every population; an alias keys as itself; an unaliased expression keys by its references' common dataset (`orders.amount_sum_count`), or by its sanitized text when they share none. Two entries starting at different datasets are two columns even when they read one field.
* A `__` token in a name is matched by **exact name**, not split into a relationship walk — write a single-dot DSL path (`customers.region`) for a relationship. Only the reserved `__slayer_` prefix is rejected.
* Strings are single-quoted with `''` for an embedded quote and a backslash is an ordinary character (`'O''Brien'`, `'C:\temp'`), while a double-quoted name is an identifier (`sum("order total")`, `customers."Region"`) with `""` for an embedded double quote; an unresolved double-quoted name gets a hint to single-quote a string.
* A bare name matches in any case; when two declared names differ only in case, the exact spelling wins and any other spelling is an error naming both (`"Revenue"` / `"revenue"` pick one).
* A double-quoted name matches the declared name spelled exactly, else the one whose Ossie normalized form it is (a name writable bare upper-cased, any other as written: `"AMOUNT"` reaches `amount`, `"ORDER TOTAL"` never reaches `order total`), and several names of that form are an error naming each. Keywords and function, aggregation and transform names match in any case (`SUM(amount)` is `sum(amount)`), and `agg` is reserved: it is never a function or an aggregation name, though a field or metric may be named `agg`.
* Inside an aggregation or transform call, `name = value` is a keyword argument when `name` is one of the call's parameters (`weighted_avg(amount, weight = qty)`, `time_shift(x, periods = -1)`) and a comparison otherwise (`sum(status = 'paid')` counts the paid rows); parenthesise or double-quote the left side to compare a field named like a parameter (`sum((weight = 1))`), and a scalar function's arguments are always values (`iif(status = 'paid', 1, 0)`).
* A syntax error reports its column (`syntax error at column 12`), and a malformed `CASE` names the missing keyword (`THEN`, `END`).
* `CAST(x AS type)` and `x::type` render the type you write and mean the same on every database: a number cast to `BOOLEAN` is true when it is not zero, a fraction cast to an integer or `DECIMAL(p, s)` type rounds half away from zero (`CAST(2.5 AS INT)` is `3`; a floating-point tie as the database rounds it), a bare `DECIMAL` / `NUMERIC` is `DECIMAL(38, 0)` and `DECIMAL(p)` is `DECIMAL(p, 0)`, and `VARCHAR(n)` keeps the first `n` characters; invalid input (text that is no literal of the type, a value out of range) is the database's own, except that SQLite gives `NULL` for a `BOOLEAN` or `DECIMAL` cast it cannot convert. `TRY_CAST(x AS type)` is that cast where it converts and NULL otherwise, except that Postgres older than 16 (or of unknown version) gets a plain `CAST` and MariaDB and a MySQL server reporting a version before 8.0 refuse `TRY_CAST` to a number at plan time.
* A cast's type follows its target's name — integer names INT, `DECIMAL` / `NUMERIC` / float names DOUBLE, character names TEXT, `BOOLEAN`, `DATE`, timestamp names TIMESTAMP, `TIME` a [time of day](#time-of-day), anything else UNKNOWN — and type parameters and chained casts are kept as written (`CAST(x AS DECIMAL(10,2))`, `x::INT::TEXT`), while a type the database cannot express (`MONEY` on DuckDB) is a plan-time `CastTypeError`.
* A type's parameters may sit between its words and an array is written `T[]` (`TIMESTAMP(6) WITH TIME ZONE`, `CAST(codes AS INT[])`, a list of INT usable with the list functions where the database has arrays), while a type the database would render losing a part (`DECIMAL(10,2) UNSIGNED` on Postgres), `T[][]` and `TRY_CAST` to an array are plan-time `CastTypeError`s.
* `--` starts a comment to the end of the line and `/* … */` is a (nestable) comment, as in SQL, so `a --b` means `a`.
* `TRY_CAST` to an integer type accepts only optionally signed digits with surrounding spaces, or a number rounded half away from zero, in the type's range (`INT` is 32-bit on every database), so `TRY_CAST('1.5' AS INT)` is `NULL` and `TRY_CAST(1.5 AS INT)` is `2` wherever `TRY_CAST` yields NULL (above); a JSON number converts as a number (`json_value(doc, '$.v' RETURNING INT)` over `{"v": 1.5}` is `2`), a boolean reads as `1` / `0` in `TRY_CAST` to a number, and a value past a `DECIMAL(p, s)`'s precision is `NULL`.
* On SQLite and MySQL, `TRY_CAST` to a non-integer number accepts decimal or exponent text with surrounding spaces, to `BOOLEAN` `true` / `t` / `yes` / `1` and `false` / `f` / `no` / `0` in any case, and to `DATE` / `TIMESTAMP` an ISO date or instant on SQLite (MySQL's own date cast); anything else is `NULL`.
* `INTERVAL '7 days'`, `INTERVAL '7' DAY` and `INTERVAL 7 DAY` (unit singular or plural) mean `interval(7, 'day')`, while a multi-unit literal (`INTERVAL '1 day 2 hours'`) is rejected — chain one interval per unit; Ossie's portable SQL has no `INTERVAL`, so a saved field or metric stores `x + INTERVAL '7 days'` as `DATEADD(day, 7, x)` (a subtraction as a negative count), with the same values, while queries keep accepting `INTERVAL`; `DATE '…'` / `TIMESTAMP '…'` are typed temporal literals (an impossible one such as `DATE '2025-02-30'` is a `TimeLiteralError`), and `CURRENT_DATE` / `CURRENT_TIMESTAMP` mean `current_date()` / `now()`.

## Aggregation syntax

An aggregation is written `agg(col)` or `agg(col, args)` — for example
`sum(orders.revenue)`, `percentile(orders.price, p=0.9)`, `sum(orders.revenue, window='90d')`,
`sum(orders.amount, partition_by=orders.city)`, `last(accounts.balance, accounts.updated_at)`,
`weighted_avg(orders.price, weight=orders.quantity)`, `my_custom_agg(orders.price)`. `*` counts
rows: `count(orders.*)`, `count(customers.*)`, and a field counts non-null
values: `count(customers.regions.name)`. This holds in **every** position that
accepts aggregations (query metrics, filters, order, dataset metrics, dataset
extensions, multi-stage expressions, computed-dimension
expressions, transform and arithmetic operands) and for **every** aggregation
— builtin, aliased (`countD(x)` = `count_distinct(x)`), and dataset-defined
custom aggregations. Declared parameters may be passed positionally in
declaration order — `percentile(orders.price, 0.9)` means `p=0.9` (`first`/`last`
keep their positional ranking key instead). An aggregation that declares no
parameters takes no positional value at all, and `first`/`last` at most one —
their ranking key, a field of the ranked dataset: over rows a row-level expression
(`last(accounts.balance, accounts.updated_at)`, `last(accounts.balance, CAST(accounts.updated_at AS DATE))`) or an
attached aggregate the rows determine, over an aggregate a field the operand's grain determines (a grain member,
or a field it reaches over an entity key or to-one hops) or an aggregate at a grain the operand determines (`last(sum(orders.revenue), sum(orders.units))`); never a literal.

Naming and matching read the parse tree, not the text: a metric declared
`{"metric": "sum(orders.revenue)", "alias": "rev"}` is matched by a filter written
`sum(orders.revenue) > 100`; its result key is `rev`, and `orders.revenue_sum` without the alias.

Disambiguation rules:

* `first` / `last` are always aggregations: a call whose first argument resolves to a row-level value ranks
  its home's rows (`last(accounts.balance)`), one whose argument resolves to aggregated values — including a saved
  metric — ranks that aggregate's cells (`last(sum(orders.revenue))`, `last(orders.rev)`).
* An unknown call name whose first argument is aggregatable defers to binding
  — custom aggregations resolve there, and genuinely unknown names get the
  "Unknown function" error.
* Scalar-allowlist names always stay scalar calls (`round(orders.price)` is the
  scalar); a custom aggregation may not shadow a scalar-function name
  (rejected at dataset validation).
* SQL's `DISTINCT` keyword is accepted only as `count(distinct orders.user_id)`, which
  means `count_distinct(orders.user_id)`.
* Mode-A surfaces are unchanged — `SUM(amount)` inside a field's dialect entry stays raw
  SQL.

An aggregation also accepts a row-level **expression** over its dataset and its to-one relationships as its value —
`sum(orders.amount - orders.cost)`, `sum(orders.amount - customers.discount)`. See [Expression aggregation](expressions.md#expression-aggregation).

## `__` in identifiers

`__` is a legal character in dataset / query / field / metric names, matched by **exact name**. A dataset can be named `customers__region`, and a Mode-A qualifier `customers__region.label` binds to that exact directly-joined dataset — it is never split into a `customers → region` walk. Dots are the only relationship-hop separator, so there is no ambiguity between a multi-token name and a relationship path.

The one reserved spelling is the **`__slayer_` prefix**: dataset / field / metric names and Mode-B tokens beginning with `__slayer_` are rejected, since that namespace is reserved for SLayer-internal identifiers.

The query-backed dataset wrap (`_expand_query_backed_dataset`, via `flat_name` in `slayer/sql/naming.py`) still flattens joined-dataset fields into virtual-dataset column names like `stores__name` or `customers__regions__name` — the entire dotted path becomes one SQL identifier on the synthetic table. A downstream stage references such a column by its stage name and that exact flattened name (`s1.customers__regions__name`); strict resolution at binding time raises `ReferenceError` for any bare name that doesn't resolve to a `DatasetField` / `DatasetMetric` / custom aggregation / canonical agg alias / query-level alias on the source dataset.

## Reference-resolution rules at a glance

1. **Dataset-side filters** (`DatasetField.filter`, `SlayerDataset.filters`) use a sqlglot-based SQL-mode parser, so they accept arbitrary SQL function calls (`json_extract`, `coalesce`, `CASE WHEN`, …) — matching the spec that "datasets are the boundary that lifts raw SQL tables into the SLayer DSL".

2. **Query-side filters** strict-resolve at enrichment time: any bare name that isn't a `DatasetField` / `DatasetMetric` / custom aggregation / query alias / canonical-agg synthesis raises a clear error.

3. **A windowed field is a column.** A query filter naming a field whose definition holds a window function, or holding a row-level window itself, filters on the value computed over its dataset's rows (see [datasets.md](datasets.md#window-functions-in-a-fields-expression)).

4. **Single reference-resolution surface.** Identifier handling lives in `slayer/core/refs.py`; relationship walks live in the binder (`slayer/engine/binding.py`), which resolves each hop against the resolved source bundle.

## Examples — accepted and rejected

### `DatasetField.filter` (SQL mode)

Accepted at `DatasetField` construction:

```json
{"name": "active_amount", "expression": "amount", "filter": "json_extract(metadata, '$.active') = 1", "datatype": "Float"}
{"name": "amt", "expression": "amount", "filter": "CASE WHEN status = 'active' THEN 1 ELSE 0 END = 1", "datatype": "Float"}
{"name": "amt", "expression": "amount", "filter": "customers.regions.name = 'US'", "datatype": "Float"}
{"name": "first_amount", "expression": "amount", "filter": "row_number() over (partition by region order by ordered_at) = 1", "datatype": "Float"}
```

Rejected at `DatasetField` construction:

<!-- docs-guard: skip -->
```json
{"name": "x", "expression": "amount", "filter": "sum(revenue) > 100"}        // DSL aggregation
{"name": "x", "expression": "amount", "filter": "cumsum(amount) > 0"}       // DSL transform call
```

### `SlayerQuery.filter` (DSL mode)

Accepted at `SlayerQuery` construction:

```json
{"population": "orders", "filter": ["sum(orders.revenue) > 100"]}
{"population": "orders", "filter": ["change(sum(orders.revenue)) > 0"]}
{"population": "orders", "filter": ["customers.region = 'EU'"]}
{"population": "orders", "filter": ["orders.status = '{val}'"], "variables": {"val": "active"}}
{"population": "orders", "filter": ["ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at) = 1"]}
```

Rejected at enrichment:

```json
{"population": "orders", "dimensions": ["orders.id"], "filter": ["initcap(orders.name) = 'Ada'"]}
// ↑ UnknownFunctionError: raw SQL function calls outside the library in DSL mode

{"population": "orders", "dimensions": ["orders.id"], "filter": ["orders.unknown_col > 0"]}
// ↑ ReferenceError: 'unknown_col' is neither a field nor a saved metric on 'orders'

{"population": "orders", "dimensions": ["orders.id"], "filter": ["orders.customers__region = 'EU'"]}
// ↑ ReferenceError: no field named 'customers__region' on 'orders'
//   (`__` is matched by exact name, not split — for the relationship write dotted: 'customers.region')
```

### `DatasetMetric.expression` (DSL mode)

Accepted at construction:

```json
{"name": "aov", "expression": "sum(revenue) / count(*)"}
{"name": "cust_rev", "expression": "sum(customers.revenue)"}     // cross-dataset dotted path
{"name": "resale", "expression": "customers.aov"}               // another dataset's SAVED metric
{"name": "growth", "expression": "change(sum(revenue))"}         // transform on agg ref
```

Rejected at enrichment (when the expression is evaluated against a dataset):

```json
{"name": "bad", "expression": "initcap(name)"}                  // raw SQL fn
```

A bare `aov` or dotted `customers.aov` reuses a saved metric by expanding its expression inline, but only inside a metric expression or computed-dimension expression — see [Reusing another dataset's saved metric](datasets.md#reusing-another-datasets-saved-metric-customersaov).

## Scalar functions and dialect semantics

The allowlisted scalars are rendered as typed SQL and then translated to each
backend's own spelling, so one expression stays correct across dialects rather
than being passed through verbatim. `length(x)` counts trailing spaces on SQL Server too (`LEN(CAST(x AS NVARCHAR(MAX)) + 'x') - 1`),
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
    {"filter": ["concat(ifnull(customers.first_name, ''), ifnull(customers.last_name, '')) = 'AdaLovelace'"]}
    ```

* **`greatest` / `least` ignore `NULL` arguments on every backend** (NULL only when every argument is NULL); on SQL Server they need 2022 or later.

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
  `ceil` / `ceiling` take exactly one argument. `ltrim` / `rtrim` likewise take
  the string only — the "strip these characters" second argument is not
  accepted, because its meaning (a character set vs. an exact substring)
  differs across backends.

* **Aliases render identically to their canonical spelling.** `ceiling` is
  `ceil` and `substring` is `substr`; both spellings are accepted so an expression
  written against either SQL convention binds, and both emit the target's own
  form.

## The portable function library

Every library function, operator and pattern language means the same on SQLite, Postgres, DuckDB, MySQL, ClickHouse, SQL Server, Trino, Snowflake and BigQuery — Postgres's value where databases disagree — and is verified on each of them; any other database runs only the functions it translates and refuses the rest at plan time ([support table](../database-support.md#portable-library-support)).

* **Out-of-domain arguments yield `NULL`** (`asin(2)` over a field, `div(a, 0)`, a surrogate `chr`, `ln` / `log10` / `log2` / `log` of a value ≤ 0 or to a base ≤ 0 or 1, `sqrt` of a negative, `pow` / `power` of a negative base to a non-integer exponent or of 0 to a negative one), and a literal one is a plan-time error; an overflowing result (`exp(1000)`) is the database's own (infinity or an error).
* **Strings count characters, not bytes**; `left` / `right` with a negative count drop characters from the other end, `lpad` / `rpad` truncate, `split_part`'s index is a non-zero integer literal (negative from the end, `''` past it), `concat_ws` skips `NULL`s, `chr` / `ascii` speak Unicode code points; `div` truncates toward zero.
* **`starts_with`, `ends_with`, `contains`, `strpos`, `replace` and `split_part`'s delimiter match literally**, case- and accent-sensitively whatever the column's collation, unlike `=` and `LIKE` ([string matching](#string-matching)).
* **A version-gated function fails at plan time** when the server reports an older version: regex needs SQL Server 2025 and MySQL 8.0, `greatest` / `least` SQL Server 2022, `json_value` MySQL 8.0.21, numeric `TRY_CAST` MySQL 8.0; JSON reads and `TRY_CAST` return `NULL` on malformed input only on Postgres 16+.
* **`date_trunc('month', x)` is the one spelling of a time bucket**, in every position; `month(x)` is the month of year.
* **`round(x, d)` and `trunc(x, d)` take whole digits**: a literal (`2`, `-1`, `2.0`) or an integer value; `round(x, 1.5)` is a plan-time error. `trunc(x, d)` cuts toward zero (`trunc(-2.789, 1)` is `-2.7`, `trunc(1234.5, -1)` is `1230`) and has `round`'s result type.
* **`zeroifnull(x)` and `nullifzero(x)` read a number**: `x`, or 0 for `NULL`, and `x`, or `NULL` for 0; over TEXT they need a `TRY_CAST`, as every number function does, in a query and in a field's definition, which is refused when saved.
* **`radians(x)` and `degrees(x)`** convert degrees to radians and back, as DOUBLE.

### Ossie spellings

Ossie's names for library functions are spellings of them, with their value, result key and SQL, matched case-insensitively: `IF` and `IFF` are `iif`, `NVL` is `ifnull`, `NVL2(e, a, b)` is `iif(e IS NOT NULL, a, b)`, `STARTSWITH` / `ENDSWITH` are `starts_with` / `ends_with`, `CHARINDEX(sub, s)` is `instr(s, sub)` (no start position), `TRUNCATE` is `trunc`, `TO_DATE(s)` is `CAST(s AS DATE)`, `TO_TIMESTAMP(s)` is `CAST(s AS TIMESTAMP_NTZ)` (a format argument is refused), and `TIMESTAMP_NTZ '…'` is a `TIMESTAMP '…'` literal. A dataset's own aggregation of one of these names keeps its meaning there.

### Regular expressions

`regexp_like(x, p[, 'i'])` (also `x ~ p`, `~*`, `!~`, `!~*`), `regexp_extract(x, p[, group])`, `regexp_replace(x, p, replacement[, 'i'])` and `regexp_count(x, p[, 'i'])` take a pattern literal or `{variable}` in one portable subset — literals, `.`, `[…]` / `[^…]`, `* + ? {m,n}`, `^ $`, `|`, `( )`, `\d \w \s` and their negations, `\n \t \r`, a backslash before a metacharacter — where `.` matches newlines, `^` / `$` are the whole string's ends, `\d \w \s` are ASCII, matching is case-sensitive without `'i'`, and a replacement's `\1`–`\9` insert a group and `\\` a backslash (`$` and `&` are literal); anything else (lookaround, backreferences, lazy quantifiers, `(?…)`, `\b`, `\p{…}`, POSIX classes) is a plan-time error naming the construct, and a repetition such as `'[0-9]{3}'` needs no brace escaping.

`regexp_count` is the number of non-overlapping matches, scanning left to right (0 when none), and needs Postgres 15; MySQL has no translation, and only Postgres counts a pattern that can match the empty string (`'x*'`) — elsewhere it is a plan-time error.

`regexp_extract` is `NULL` when nothing matches and `''` when the requested group took no part in the match, its `group` is an integer literal from 0 (the whole match) to the pattern's group count, `'i'` is the only flag, and a replacement is a string literal or `{variable}` in which any backslash escape other than `\1`–`\9` and `\\` is an error.

### JSON

`json_value(x, '$.a[0]."b c"' [RETURNING INT | DOUBLE | BOOLEAN | DATE | TIMESTAMP | TEXT])` reads one scalar from JSON text or a native JSON column (`NULL` when missing, JSON null, an object or an array; `NULL` when the value cannot convert), the arrows `x -> 'a' ->> 'b'` and `x ->> '$.a.b'` spell the same read as TEXT, and a path is `$` followed by `.key`, `."quoted key"` and `[n]` steps only.

Read as TEXT, a JSON boolean is `'true'` / `'false'` and a JSON number is the database's own text form (`1.50` may read `'1.5'`), so use `RETURNING DOUBLE` to read it as a number; `RETURNING` converts JSON booleans and numbers directly.

### Text beside a number

A TEXT operand meeting a number in a comparison, `IN`, `BETWEEN`, `IS [NOT] DISTINCT FROM`, arithmetic or `greatest` / `least` reads as `TRY_CAST(text AS DOUBLE)` (a non-number is `NULL`, or an error on Postgres before 16 or of unknown version; a TEXT literal converts at plan time or is an error), so `orders.amount_txt > 100` compares numerically on every database with a numeric `TRY_CAST` (MariaDB and a MySQL server reporting a version before 8.0 refuse it at plan time) — integers beyond 2^53 lose precision, so compare those with `TRY_CAST(x AS BIGINT)`, and a number function or aggregation over TEXT (`sum(orders.amount_txt)`) still needs an explicit `TRY_CAST`.

Arithmetic on TEXT is typed DOUBLE, a `CAST` you write is never rewritten (it fails at the database on a non-number), a number beside text in `||` / `concat` uses the database's own number-to-text cast (`-3.0` on DuckDB), and a BOOLEAN operand of `||` / `concat` / `concat_ws` is rejected with a `CASE WHEN flag THEN 'true' ELSE 'false' END` remedy.

### String matching

`=`, `<`, `IN` and `LIKE` follow the column's collation (except SQLite's `LIKE`, which is case-sensitive), `ILIKE` and the regex `'i'` flag are the case-insensitive forms, and every `LIKE` / `ILIKE` pattern reads `%` / `_` as wildcards, `\` as escaping `%`, `_` and itself (literal before anything else) and every other character, `[` included, as itself.

### Lists

Over a [list field](filters.md#list-fields) `contains(contractors.skills, 'go')`, `contains_any(contractors.skills, ['go', 'rust'])`, `contains_all(contractors.skills, {wanted})` and `length(contractors.skills)` (the number of distinct elements) read only its non-NULL elements and are `NULL` only for a `NULL` argument, and an array literal's elements are literals or `{variable}`s of one type family (`[]` takes the type of its context). [Database support](../database-support.md) names the databases that run lists.

## Date and time functions

Six portable functions cover extraction, truncation, differences, arithmetic and the clock. Each renders
to the same result on every supported backend, and the part/unit is a string literal (matched
case-insensitively):

| Function | Returns | Part / unit |
|---|---|---|
| `date_part(part, ts)` | INT | `year`, `iso_year`, `quarter`, `month`, `week`, `day`, `day_of_week`, `day_of_year`, `hour`, `minute`, `second`, `millisecond` |
| `date_diff(unit, start, end)` | INT | `second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year` |
| `date_add(ts, n, unit)` | DATE or TIMESTAMP | same units as `date_diff` |
| `date_trunc(unit, ts)` | DATE for a day-or-coarser bucket of a DATE, else TIMESTAMP | any granularity, the datasource's custom ones included |
| `current_date()` / `now()` | DATE / TIMESTAMP | — |
| `current_time()` | [`Time`](#time-of-day) | — |

`extract(part from ts)` and `year(ts)`, `quarter(ts)`, `month(ts)`, `week(ts)` (the ISO week number), `day(ts)`, `hour(ts)`, `minute(ts)` and `second(ts)` mean `date_part('part', ts)`, and the aliases `isodow` / `day_of_week`, `doy` / `dayofyear` / `day_of_year`, `isoyear` / `iso_year`, `isoweek` / `iso_week` / `weekofyear` and `dayofmonth` / `day_of_month` work both as one-argument functions (`isodow(ts)`) and as parts. Ossie's `dayofweek` (`EXTRACT(DAYOFWEEK FROM ts)`) and `millisecond` are parts only: `dayofweek(ts)` and `millisecond(ts)` are rejected, as are `dow` and `weekday` in any position, because databases number weekdays differently. As in `extract`, `date_part`'s part may be written as a bare word: `date_part(MONTH, ts)` means `date_part('month', ts)`.

```json
{
  "population": "orders",
  "dimensions": [{"field": "date_part('day_of_week', orders.created_at)", "alias": "dow"}],
  "metrics": [{"metric": "avg(date_diff('day', orders.created_at, coalesce(orders.shipped_at, now())))", "alias": "age_days"}],
  "filter": ["orders.created_at >= date_add(current_date(), -30, 'day')"]
}
```

* **ISO weeks.** `day_of_week` is Monday = 1 … Sunday = 7; `week` is the ISO-8601 week and
  `iso_year` the year that week belongs to, so `2024-12-30` is week 1 of 2025. Pair `week`
  with `iso_year`, never with `year`. Sub-day parts of a DATE are 0. `millisecond` is the
  millisecond component, truncated (0 … 999; `10:15:42.001999` gives 1), never the whole seconds.
* **`date_diff` counts calendar boundaries crossed**, not elapsed units:
  `date_diff('month', '2024-01-31', '2024-02-01')` is 1 and `date_diff('day', '2024-03-01 23:59:00', '2024-03-02 00:01:00')`
  is 1. It is negative when `end` precedes `start`. `week` boundaries are Mondays, `week_sunday`
  Sundays. For whole elapsed units, compare a boundary count with `date_add`, e.g.
  `date_add(start, date_diff('month', start, end), 'month') > end` means the last month is incomplete.
* **`date_add` clamps at month-end**: `date_add('2024-01-31', 1, 'month')` is `2024-02-29`. A DATE
  stays a DATE for day-or-coarser units; the time of day is kept. A literal count must be an
  integer; a computed count (`date_add(orders.order_date, orders.sla_days, 'day')`) is truncated toward zero.
* **`interval` spelling.** `ts + interval(n, unit)`, `interval(n, unit) + ts` and
  `ts - interval(n, unit)` are exactly `date_add` (chains apply left to right), an `INTERVAL '…'`
  literal included; `interval` anywhere else is rejected.
* **Typed operands.** Date operands must be DATE / TIMESTAMP: a field declared with that
  `datatype`, `min`/`max`/`first`/`last` of one, a date function, the clock, an ISO literal, a
  `DATE '…'` / `TIMESTAMP '…'` literal, a `CAST(… AS DATE)` / `CAST(… AS TIMESTAMP)`, or
  `coalesce` / `ifnull` / `nullif` / `greatest` / `least` / `iif` / `CASE` over those. Anything else
  (a TEXT field, arithmetic, a number) is a type error — declare the field's `datatype`.
* **ISO literals.** In a date position `'2024-01-31'` is a DATE and an [instant](time.md#time-points)
  (`'2024-01-31 10:00'`, with a space or `T`) a TIMESTAMP; `{variable}` placeholders work there.
  Elsewhere a string compared with a temporal operand is a [time point](time.md#time-points).
* **Backend notes.** `now()` / `current_date()` read the database clock (UTC on SQLite, the
  session time zone elsewhere); relative time points such as `'last month'` read the SLayer
  host clock instead. On SQLite a stored date that is not ISO text yields `NULL`, and date arithmetic
  runs through the registered `slayer_date_add` function, so dry-run SQL needs a SLayer
  connection. Queries using `now()` / `current_date()` / `current_time()` are never served from, or stored in, the
  [result cache](query-cache.md).

### Time of day

`TIME '10:30:00'` (also `'HH:MM'`, and up to six fraction digits), `current_time()` (bare `CURRENT_TIME` too: the time of day of `now()`) and `CAST(x AS TIME)` of text, a timestamp or a time of day each yield a `Time` value, as a field typed `Time` does.

A time of day groups, orders, takes `count`, `min`, `max`, `first`, `last` and `collect`, keeps its type through `coalesce` / `ifnull` / `nullif` / `greatest` / `least` / `iif` / `CASE`, and compares with another time of day; a string beside one is its `TIME` literal (`shifts.starts_at >= '09:00'`).

`hour`, `minute` and `second` (in every `date_part` spelling) read its parts; arithmetic, every other date function, a comparison with a date or timestamp, a granularity, a `date_range` and a `default_time_dimension` are refused, and a query `time_zone` never converts it.

It casts to a character type as `HH:MM:SS` followed by its fraction when that is not zero, and to no other type; `TRY_CAST(x AS TIME)` is NULL for text that names no time of day inside the day.

A result cell of a `Time` column is always a time of day (`"10:30:00"` in JSON): a stored value outside the day, which MySQL and ClickHouse allow, fails the query naming the column and the value. Per-database notes are in [database support](../database-support.md#time-of-day).

## `NULL` inside an `in` list

`NULL` is rejected inside an `in` / `not in` list. SQL compares it by
three-valued logic, so `status in ('a', NULL)` never matches on the null, and
`status not in ('a', NULL)` matches **no rows at all** — the filter silently
returns an empty result instead of "everything except 'a'".

Test for null separately:

```json
{"filter": ["orders.status not in ('new', 'old')", "orders.status is not null"]}
```

## See also

* [Datasets](datasets.md) — field expressions, `DatasetField.filter`, dataset-level filters
* [Queries](queries.md) — `SlayerQuery` field semantics
* [Expressions](expressions.md) — DSL grammar and transforms
* [Relationships](datasets.md#relationships) — dotted relationship paths in dataset SQL (generated SQL uses `__` internal aliases)
