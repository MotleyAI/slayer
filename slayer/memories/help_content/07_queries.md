# Queries

## Shapes

The `query` tool takes one of:

- a query object: `source_model`, `dimensions`, `time_dimensions`, `measures`,
  `filters`, `order`, `limit`, ... (the fields' descriptions hold the syntax);
- a list of query objects (stages), the last one returned;
- the name of a saved query-backed model, with `refine` merging extra dimensions,
  measures, filters, order or limit into its final stage:
  `query(query="monthly_revenue", refine={"dimensions": ["region"]})`.

## Stages

Every stage but the last carries a `name`; later stages name it as `source_model`
(or join it through an inline extension). A stage name shadows a saved model of the
same name inside its list. Define before you reference: a stage sees only what its
source defines or an earlier stage returned.

An outer stage sees the inner result's columns. Dotted paths flatten to `__`
(`customers.regions.name` becomes `customers__regions__name`); measures keep their names
(`orders.total` is `total`). Select the columns the outer stage needs, display names
included, in the inner stage.

## Rows

A query with dimensions only returns distinct combinations; set
`distinct_dimension_values: false` for raw rows (no measures allowed). Without a
`limit`, the MCP response stops at 20 rows with a notice; for a longer list set a
`limit` that covers it. Use `limit` for top N, never to cut a list asked for in full.

## Functions

Conditions and formulas take one portable library with the same meaning on every
database that translates it (others refuse the function at plan time): strings (`left`, `split_part`, `concat_ws`, ...), math (`div`, `atan2`, ...),
regex (`regexp_like(x, 'p')`, `x ~ 'p'`, `regexp_extract`, `regexp_replace`), JSON
(`json_value(x, '$.a' RETURNING INT)`, `x ->> 'a'`), lists (`contains(tags, 'x')`,
`contains_any(tags, ['x', 'y'])`, `contains_all(tags, ['x', 'y'])`, `length(tags)`; the
aggregation `collect(x)` builds one per group); text compared with a number reads as
a number (`amount_txt > 100`). `starts_with`, `ends_with`, `contains`, `strpos`, `replace`
and the `split_part` delimiter match literally, case- and accent-sensitively, whatever the
collation. Casts: `CAST(x AS INT)` or `x::INT`; `TRY_CAST(x AS INT)` gives NULL where a value
does not convert (on Postgres before 16, or of unknown version, it fails like `CAST`; MariaDB and a MySQL server
reporting a version before 8.0 refuse it to a number).

## Variables

`{name}` placeholders in conditions, formulas and `date_range` bounds take values from
the tool's `variables` argument, a stage's own `variables`, the outer query's
`variables` (when a saved query-backed model is its `source_model`), or the saved
model's defaults, in that order of precedence. A placeholder binds a literal value only:
its text inside quotes (`'{s}'`), a number, numeric string or boolean in a bare slot, a
list as the whole right-hand side of `IN` (`status in ({statuses})`) or as a list-function
argument (`contains_all(tags, {wanted})`, the only place an empty list binds). A
placeholder where a name belongs is an error.

## Debugging

`show_sql=true` returns the SQL with the rows; `dry_run=true` only the SQL;
`explain=true` the database plan.
