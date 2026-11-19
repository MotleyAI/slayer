# Queries

## Shapes

The `query` tool takes one of:

- a query object: `population`, `dimensions`, `time_dimensions`, `metrics`,
  `filter`, `order`, `limit`, ... (the fields' descriptions hold the syntax);
- a list of query objects (stages), the last one returned;
- the name of a saved query-backed dataset, with `refine` merging extra dimensions,
  metrics, filters, order or limit into its final stage:
  `query(query="monthly_revenue", refine={"dimensions": ["region"]})`.

## Stages

Every stage but the last carries a `name`; later stages name it as `population`
(or reach it through an inline extension's relationship). A stage name shadows a saved dataset of the
same name inside its list. Define before you reference: a stage sees only what its
source defines or an earlier stage returned.

An outer stage sees the inner result's columns. Dotted paths flatten to `__`
(`customers.regions.name` becomes `customers__regions__name`); metrics keep their names
(`orders.total` is `total`). Select the columns the outer stage needs, display names
included, in the inner stage.

## Rows

A query with dimensions only returns distinct combinations; set
`distinct_dimension_values: false` for raw rows (no metrics allowed). Without a
`limit`, the MCP response stops at 20 rows with a notice; for a longer list set a
`limit` that covers it. Use `limit` for top N, never to cut a list asked for in full.

## Functions

Conditions and expressions take one portable library with the same meaning on every
database that translates it (others refuse the function at plan time): strings (`left`, `split_part`, `concat_ws`, ...), math (`div`, `atan2`, ...),
regex (`regexp_like(x, 'p')`, `x ~ 'p'`, `regexp_extract`, `regexp_replace`), JSON
(`json_value(x, '$.a' RETURNING INT)`, `x ->> 'a'`), lists (`contains(tags, 'x')`,
`contains_any(tags, ['x', 'y'])`, `contains_all(tags, ['x', 'y'])`, `length(tags)`; the
aggregation `collect(x)` builds one per group); text compared with a number reads as
a number (`amount_txt > 100`), except where `TRY_CAST` to a number is refused (below). `starts_with`, `ends_with`, `contains`, `strpos`, `replace`
and the `split_part` delimiter match literally, case- and accent-sensitively, whatever the
collation. Casts: `CAST(x AS INT)` or `x::INT`; `TRY_CAST(x AS INT)` gives NULL where a value
does not convert (on Postgres before 16, or of unknown version, it fails like `CAST`; MariaDB and a MySQL server
reporting a version before 8.0 refuse it to a number).

## Variables

`{name}` placeholders in conditions, expressions and `date_range` bounds take values from
the tool's `variables` argument, a stage's own `variables`, the outer query's
`variables` (when a saved query-backed dataset is its `population`), or the saved
dataset's defaults, in that order of precedence. A placeholder binds a literal value only:
its text inside quotes (`'{s}'`), a number, numeric string or boolean in a bare slot, a
list as the whole right-hand side of `IN` (`status in ({statuses})`) or as a list-function
argument (`contains_all(tags, {wanted})`, the only place an empty list binds). A
placeholder where a name belongs is an error. A `date_range` bound takes the value's text
unquoted (`"date_range": ["{start}", "{end}"]`).

## Debugging

`show_sql=true` returns the SQL with the rows; `dry_run=true` only the SQL;
`explain=true` the database plan.
