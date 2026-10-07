# Queries

## Shapes

The `query` tool takes one of:

- a query object: `source_model`, `dimensions`, `time_dimensions`, `measures`,
  `filters`, `order`, `limit`, ... (the fields' descriptions hold the syntax);
- a list of query objects (stages), the last one returned;
- the name of a saved query-backed model, with `refine` merging extra dimensions,
  measures, filters, order or limit into its final stage:
  `query(query="monthly_revenue", refine={"dimensions": ["region"]})`.

## One query first

Shares, grand totals, ranks, top N per group, period-over-period changes, running
totals, trailing windows, joined models' aggregates and aggregates of aggregates all
fit in one query object. Reach for stages only when a whole result must be re-queried.

## Stages

Every stage but the last carries a `name`; later stages name it as `source_model`
(or join it through an inline extension). A stage name shadows a saved model of the
same name inside its list. Define before you reference: a stage sees only what its
source defines or an earlier stage returned.

An outer stage sees the inner result's columns. Dotted paths flatten to `__`
(`customers.region_id` becomes `customers__region_id`); measures keep their names
(`orders.total` is `total`). Select the columns the outer stage needs, display names
included, in the inner stage.

Typical uses: filter after a rank or window computed over all rows; aggregate a whole
result again; line up two differently grouped results by a shared key.

## Rows

A query with dimensions only returns distinct combinations; set
`distinct_dimension_values: false` for raw rows (no measures allowed). Without a
`limit`, the MCP response stops at 20 rows with a notice; use `limit` for top N,
never to trim a list.

## Variables

`{name}` placeholders in conditions, formulas and `date_range` bounds take values from
the tool's `variables` argument, a stage's own `variables`, or the saved model's
defaults, in that order of precedence.

## Debugging

`show_sql=true` returns the SQL with the rows; `dry_run=true` only the SQL;
`explain=true` the database plan.
