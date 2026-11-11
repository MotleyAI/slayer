## MODIFIED Requirements

### Requirement: The method-call spelling is an aggregation spelling
`<path>.<name>(<source>[, <literal arguments>])` SHALL be accepted in every position that accepts an
aggregation as a deprecated exact equivalent of `<name>(<path>.<source>[, <literal arguments>])` — same
aggregation, result key, SQL and values, with the deprecation warnings and save-time rewrite of
`queries/expression-grammar` › Legacy spellings keep their meaning — where `<path>` is a dotted join path,
`<source>` is `*`, a bare column name, or an expression whose column leaves are all bare names (each leaf
read under `<path>`), and every further argument, positional or keyword, is a literal. `<name>` SHALL be
resolved exactly as in its functional twin: builtin aggregations, aggregation-name aliases, custom
aggregations validated at binding, and the argument-shape dispatch of `first` / `last`. Any other
method-call shape SHALL be rejected with an error showing the functional spelling: a dotted source leaf,
a non-literal further argument, a `partition_by=` argument, or a callee that is a scalar function, a
date-part alias (including `year` … `second`) or a transform-only name.

#### Scenario: Count of a joined model's rows
- **WHEN** a query rooted at `orders` measures `order_items.count(*)`
- **THEN** its columns, rows and SQL equal those of `count(order_items.*)`, and the response warns naming `count(order_items.*)`

#### Scenario: Expression source
- **WHEN** a measure is written `order_items.sum(price * qty)`
- **THEN** it equals `sum(order_items.price * order_items.qty)`

#### Scenario: Literal parameter
- **WHEN** a measure is written `order_items.percentile(price, 0.9)`
- **THEN** it equals `percentile(order_items.price, 0.9)`

#### Scenario: Granularity callee follows its twin
- **WHEN** a dimension expression is written `order_items.month(created_at)`
- **THEN** it is rejected with an error showing `month(order_items.created_at)`, because `month` is a date-part function, not an aggregation

#### Scenario: Ambiguous method-call shapes are rejected
- **WHEN** a measure is written `order_items.weighted_avg(price, qty)`, `order_items.sum(products.price)`, `order_items.sum(price, partition_by=region)` or `order_items.round(price)`
- **THEN** each is rejected with an error showing the functional spelling to write

## ADDED Requirements

### Requirement: first and last never pick a NULL ranking key
A ranked `first` / `last` aggregation (plain, partitioned, windowed or cross-model) SHALL pick only among the rows whose ranking key is non-NULL: `first` the row with the smallest key, `last` the row with the largest, on every dialect and independent of its native NULL ordering or session settings. A cell whose rows all have a NULL ranking key SHALL take the empty value, NULL. A NULL value under a non-NULL key SHALL be picked like any other value.

#### Scenario: NULL keys are skipped in both directions
- **WHEN** a customer's rows are `(updated_at 2024-01-01, status 'a')`, `(NULL, 'b')`, `(2024-03-01, 'c')` and a query selects `first(status, updated_at)` and `last(status, updated_at)`
- **THEN** they are `'a'` and `'c'` on SQLite, DuckDB, Postgres and every other Tier-1 dialect

#### Scenario: All keys NULL
- **WHEN** a customer's rows all have a NULL `updated_at`
- **THEN** `first(status, updated_at)` and `last(status, updated_at)` are NULL for that customer

#### Scenario: NULL value under a non-NULL key
- **WHEN** the latest-by-`updated_at` row's `status` is NULL
- **THEN** `last(status, updated_at)` is NULL, not an earlier row's status

#### Scenario: Partitioned and cross-model
- **WHEN** a query rooted at `orders` selects `last(customers.status, customers.updated_at, partition_by=region)` over customers some of whose `updated_at` are NULL
- **THEN** each region's value is the status of its latest non-NULL-keyed customer, by executed values on SQLite and DuckDB
