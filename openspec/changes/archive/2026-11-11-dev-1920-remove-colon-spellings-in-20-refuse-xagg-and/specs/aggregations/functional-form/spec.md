## ADDED Requirements

### Requirement: The colon spelling is refused

Fresh input using the colon aggregation spelling `<source>:<name>[(<args>)]` — a column, a join-path column, `*` or `<path>.*` source — or the colon granularity spelling `<column>:<granularity>` SHALL be rejected with a typed error naming the colon span and its twin, in every Mode-B position (query measures, filters, order keys, dimensions, time dimensions, `main_time_dimension`, computed-dimension expressions, model measure formulas, model-extension and inline-model measures, stage formulas), when a model or saved query is saved, and in entity references (`recommend_root_model` items, memory `linked_entities`). The twin SHALL be `date_trunc('<name>', <source>)` when `<name>` is a built-in granularity, and `<name>(<source>[, <args>])` otherwise, with the source and arguments as written. A colon inside a string literal, a quoted identifier or a `::` cast SHALL NOT be refused.

#### Scenario: Measure

- **WHEN** a query on `orders` has the measure `amount:sum`
- **THEN** it is rejected with the typed error naming `amount:sum` and `sum(amount)`

#### Scenario: Every builtin aggregation and alias

- **WHEN** each builtin aggregation and each aggregation-name alias is written in the colon spelling as a measure
- **THEN** each is rejected naming its functional twin (parametrized over the full builtin and alias sets)

#### Scenario: Star and cross-model star

- **WHEN** a query on `orders` has the measure `*:count`, and another `customers.*:count`
- **THEN** they are rejected naming `count(*)` and `count(customers.*)`

#### Scenario: Parameters

- **WHEN** a measure is `price:percentile(p=0.9)`, `balance:last(updated_at)` or `amount:sum(window='30d')`
- **THEN** each is rejected naming `percentile(price, p=0.9)`, `last(balance, updated_at)` and `sum(amount, window='30d')`

#### Scenario: Filter and order key

- **WHEN** a query filters `amount:sum > 100`, and another orders by `amount:sum` descending, as a string and as an object order item
- **THEN** each is rejected with the typed error naming `sum(amount)`

#### Scenario: Built-in granularity

- **WHEN** a query has the dimension `created_at:month`, the time dimension `created_at:month`, or the order key `customers.created_at:month`
- **THEN** each is rejected naming `date_trunc('month', created_at)` (`date_trunc('month', customers.created_at)` for the order key)

#### Scenario: Custom granularity

- **WHEN** a query on a datasource defining the granularity `fiscal_quarter` has the time dimension `created_at:fiscal_quarter`
- **THEN** it is rejected naming `fiscal_quarter(created_at)`

#### Scenario: Model and saved-query saves

- **WHEN** a model is saved with the measure formula `amount:sum`, or a query-backed model with a source query measuring `*:count`
- **THEN** the save is refused with the typed error naming the twin, and nothing is stored

#### Scenario: Entity references

- **WHEN** `recommend_root_model` is given `orders.amount:sum`, or a memory is saved with the linked entity `orders.amount:sum`
- **THEN** each is rejected with the typed error naming `sum(orders.amount)`

#### Scenario: Colons that are not the colon spelling

- **WHEN** a filter is `note = 'a:sum'`, a measure is `sum(amount::INT)`, and a measure is `sum(amount) FILTER (WHERE note = 'x:count')`
- **THEN** each is accepted

## MODIFIED Requirements

### Requirement: SLayer emits only the functional spelling
Every aggregation SLayer writes or shows to an agent, or passes to its own engine,
SHALL use the functional spelling `agg(col, args)`, whatever spelling its input
used: model measure formulas written by the dbt, Cube (including view facades) and
OSI importers, the SQL facade's catalog and translated queries, schema-probe queries,
memory entity resolution, root-model recommendation paths, error and warning
remedies, and tool descriptions and help. Result keys SHALL be unchanged by the
spelling: an unnamed measure keeps the key it had when the colon spelling was the
canonical text.

#### Scenario: Importer-written formulas are functional
- **WHEN** a dbt, Cube or OSI project is imported
- **THEN** every saved measure formula uses the functional spelling (for example
  `sum(amount, window='30d')`, `count(orders.*)`, `percentile(price, p=0.9)`,
  `sum(a) / count(*)`) and parses

#### Scenario: Recommendation paths are functional
- **WHEN** `recommend_root_model` is given `sum(orders.amount)` or
  `percentile(orders.amount, p=0.9)`, and recommends `orders`
- **THEN** the item paths are `sum(amount)` and `percentile(amount, p=0.9)`; a
  cross-model item keeps its join path inside the call (`sum(customers.regions.population)`)

#### Scenario: Error remedies are functional
- **WHEN** a model defines a derived column across a fanning hop
- **THEN** the save error's remedy reads `<aggregation>(orders.line_items.qty)`

#### Scenario: Tool descriptions teach the functional spelling
- **WHEN** the MCP tool descriptions and help are listed
- **THEN** none shows the colon spelling; a custom aggregation is shown used as
  `sum_sq(column)`

#### Scenario: The SQL facade translates to functional queries
- **WHEN** a SQL client runs `SELECT COUNT(*), SUM(revenue) FROM orders GROUP BY status HAVING SUM(revenue) > 1000`, and another selects a joined model's row count and `customers.region` count
- **THEN** the translated SLayer queries measure `count(*)`, `sum(revenue)`, `count(customers.*)` and `count(customers.region)` and filter `sum(revenue) > 1000`, and the result column names are unchanged

#### Scenario: Result keys are unchanged
- **WHEN** a query on `orders` has the unnamed measures `sum(amount)`, `count(*)`, `sum(amount) / count(*)`, `cumsum(sum(amount))` and `sum(amount) FILTER (WHERE status = 'paid')`, and an unnamed computed dimension `sum(amount, partition_by=region)`
- **THEN** their result keys are `orders.amount_sum`, `orders._count`, `orders.amount_sum_count`, `orders.cumsum_amount_sum`, `orders.amount_sum_filter_where_status_paid` and `orders.amount_sum_partition_by_region`, as before this change, including names long enough to carry a hash

## REMOVED Requirements

### Requirement: The legacy colon spelling is accepted as an exact equivalent
**Reason**: SLayer 2.0 removes the colon spelling; the functional spelling is the only aggregation spelling.
**Migration**: Write `agg(col, args)` (`count(*)` for `*:count`, `count(customers.*)` for `customers.*:count`, `date_trunc('month', col)` for `col:month`). Stored text is upgraded on load.
