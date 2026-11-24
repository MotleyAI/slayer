## ADDED Requirements

### Requirement: Declared cardinality never changes a value
Every aggregation SHALL count each home row once in each output group it matches after the
filters, whatever the data, unless every hop it is evaluated across is trusted
(`models/join-cardinality` › Trusted hops). Declared cardinality and declared keys decide
which paths exist and what a dataset determines, never how often a home row counts. An
untrusted provably to-one hop SHALL keep its typing: it determines, raises no associated-grain
warning, is accepted under `to_many_handling: "error"`, and admits `partition_by=` keys
across it. A dimension computed from fields beyond such a hop SHALL count a home row once
per distinct computed value, never once per matching row. With data satisfying every
declared cardinality, results SHALL be identical whether the hops are trusted or not.

#### Scenario: A duplicated dimension row counts once per group
- **WHEN** on the Appendix B seed with customer C1 appearing twice, both in `EAST`, and the
  `orders → customers` relationship untrusted, a query selects dimension
  `customers.region` and metric `orders.total_revenue`
- **THEN** EAST is 350 and WEST 300, on SQLite and DuckDB, with no warning

#### Scenario: A filter across a duplicated row counts once
- **WHEN** the same seed and query run with no dimensions and filter
  `customers.region = 'EAST'`
- **THEN** the result is 350

#### Scenario: The duplicated value is reachable only by trusting
- **WHEN** the same relationship declares `trusted: true`
- **THEN** EAST is 650

#### Scenario: A row in two groups counts in each
- **WHEN** customer C3 appears once in `SOUTH` and once in `WEST`, the relationship untrusted
- **THEN** `orders.total_revenue` is 300 in SOUTH and 300 in WEST

#### Scenario: Clean data does not depend on trust
- **WHEN** the unmodified Appendix B seed is queried by `customers.region` with the
  relationship trusted, and again untrusted
- **THEN** both runs return identical rows

#### Scenario: A computed dimension counts each distinct value once
- **WHEN** C1 appears once in `EAST` and once in `WEST`, untrusted, and the query groups by
  `CASE WHEN customers.region IN ('EAST', 'WEST') THEN 'coast' END`
- **THEN** each of C1's orders counts once in `coast`

#### Scenario: A filter and a dimension on one dataset bind the same row
- **WHEN** C1 appears once as (`EAST`, `gold`) and once as (`WEST`, `silver`), untrusted, and
  the query groups by `customers.region` with filter `customers.tier = 'gold'`
- **THEN** C1's orders count in EAST only

#### Scenario: Error mode accepts an untrusted declared dimension
- **WHEN** the duplicated-C1 query runs under `to_many_handling: "error"`
- **THEN** it returns EAST 350 instead of refusing

#### Scenario: Every aggregation family counts the home row once
- **WHEN** on the duplicated-C1 seed, untrusted, the query groups by `customers.region` and
  selects `count(*)`, `avg(orders.amount)`, `count_distinct(orders.order_id)`,
  `last(orders.amount)`, `collect(orders.order_id)`, a windowed and a ranked aggregate over
  `orders`, and `sum(orders.amount)` with an aggregate filter on `orders`
- **THEN** each value equals the same query over the de-duplicated seed

### Requirement: An aggregate input across an untrusted hop counts its home row once
An aggregation whose input, parameter, ranking key or aggregate-filter operand reads a
field across an untrusted provably to-one hop SHALL count each home row once, reading one
of the home row's matching values when the data holds several; which one is unspecified.
Under `to_many_handling: "error"` such an aggregation SHALL be refused with a typed error
naming the hop and the remedies: mark the relationship `trusted`, or record an enforced key.
Across trusted hops the input is read as today.

#### Scenario: A weighted sum reads one weight per order
- **WHEN** C1 appears twice with weights 1 and 3, untrusted, and the query selects
  `wsum(orders.amount, weight=customers.weight)` by `customers.region`
- **THEN** each of C1's orders contributes once, weighted by 1 or by 3

#### Scenario: Error mode refuses an input across an untrusted hop
- **WHEN** the same query runs under `to_many_handling: "error"`
- **THEN** it fails with the typed error naming `orders → customers` and both remedies

#### Scenario: A trusted hop reads the input as before
- **WHEN** the relationship declares `trusted: true` and the data holds no duplicates
- **THEN** the weighted sum equals today's value under every mode
