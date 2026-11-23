## MODIFIED Requirements

### Requirement: Pre-2.0 root-relative spellings convert when a population is named

In fresh input to a query or stage that names its population P, the following SHALL convert to the qualified spelling before anything else reads them, in every position:
- a bare name SHALL read what it read before 2.0, in this order: inside a metric, dimension or time-dimension entry, or the main time dimension, P's field, then P's metric, then an alias of the same query other than the entry's own; in a filter or order, P's field, then an alias of the same query, then P's metric; a name read from P becomes `P.<name>`, and an alias, or a name that is none of these, stays bare; when P is a stage or a query-backed dataset, its output columns are its fields; a double-quoted name SHALL match only its exact spelling;
- `count(*)` becomes `count(P.*)`;
- a dotted reference whose first segment is not a dataset, a stage or the time spine (an edge of P, a list field of P) gets P prepended.

A dotted reference whose first segment names a dataset or a stage SHALL be read as qualified and SHALL NOT convert. Each distinct converted form SHALL emit exactly one `FutureWarning` and one structured `deprecated_input` warning per input document, naming the old spelling, its qualified twin and SLayer 3.0. A converted query SHALL behave, key its results, and be saved exactly as its qualified twin. A refinement and a cached result SHALL treat an entry and its converted twin as the same entry. A query that names no population SHALL NOT convert anything.

#### Scenario: Legacy bare names convert
- **WHEN** a query with `population: "orders"` selects the dimension `status` and measures `count(*)` and `sum(amount)`
- **THEN** it runs as the query selecting `orders.status` with `count(orders.*)` and `sum(orders.amount)`, with the same keys, and warns once for each of the three forms

#### Scenario: An edge-first path converts
- **WHEN** `orders` has a relationship named `buyer` to `customers` and a query with `population: "orders"` selects `buyer.region`
- **THEN** it runs as `orders.buyer.region`, with a `deprecated_input` warning

#### Scenario: A dataset-first path never converts
- **WHEN** a query with `population: "orders"` selects `customers.region`
- **THEN** it emits no deprecation warning and its key is `customers.region`

#### Scenario: A saved query stores the qualified twin
- **WHEN** a query with `population: "orders"` measuring `sum(amount)` is saved
- **THEN** the stored query measures `sum(orders.amount)`

#### Scenario: A refinement matches a converted entry
- **WHEN** a saved query with `population: "orders"` measuring `sum(orders.amount)` is refined by restating the metric `sum(amount)`
- **THEN** the restatement matches the saved entry: the response has one such column, with the same SQL and columns as the unrefined query, and one deprecation warning

#### Scenario: A metric reads the population metric a sibling alias shares
- **WHEN** `orders` has the metric `revenue` (`sum(amount)`) and a query with `population: "orders"` measures `count(*)` aliased `revenue` and `revenue * 10` aliased `r10`
- **THEN** it runs as `count(orders.*)` aliased `revenue` and `orders.revenue * 10` aliased `r10`, and `r10` is ten times the sum of `amount`

#### Scenario: A filter reads a population field before an alias of the same name
- **WHEN** a query with `population: "orders"` measures `sum(amount)` aliased `amount` and filters `amount > 15`
- **THEN** it runs as `sum(orders.amount)` aliased `amount` with the filter `orders.amount > 15`

#### Scenario: A filter reads an alias the population does not declare
- **WHEN** a query with `population: "orders"` has the dimension `status`, measures `count(*)` aliased `n` and filters `n > 1`
- **THEN** the filter stays `n > 1`, reading the metric, and the response has each `status` with more than one order
