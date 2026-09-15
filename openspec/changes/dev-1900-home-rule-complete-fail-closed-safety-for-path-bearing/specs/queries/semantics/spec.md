## MODIFIED Requirements

### Requirement: Aggregation parameters are typed by the home dataset's grain
Every parameter of an aggregation — a keyword or positional parameter (`weight=`,
`other=`, a custom aggregation's declared parameters) and a parameter supplied by the
aggregation definition's default — SHALL be typed against the dataset the aggregation
runs over: with `D` that dataset and `G` its grain, a parameter `P` is legal iff `G`
determines `P` — `P` is a grain member, an aggregate whose `partition_by=` grain is a
subset of `G` (a cell value of the same dataset), or a column reached from a grain
member over provably to-one join hops (per Axiom 1, Determination). A parameter naming a
derived column is determined only when `G` determines every dependency of that column's
definition, recursively — a derived parameter whose definition crosses a hop `G` does
not pin is not determined, however its own path is reached. A legal parameter is evaluated once per
cell of `D` and the aggregation reads that value; the origin of `D`'s rows — a model's
rows deduplicated per entity, or another aggregate's cells — MUST NOT affect the rule.
A parameter `G` does not determine SHALL fail at plan time with a typed error naming
the parameter, the grain, and the remedy (aggregate the parameter to that grain, or add
its determining keys to the operand's `partition_by=`) — never invalid SQL or a silently
arbitrary value. The rule applies identically in every `to_many_handling` mode and in
every consumer position.

#### Scenario: Parameter determined by the entity key under association
- **WHEN** a query rooted at `orders` with `to_many_handling: "associate"` selects
  `customers.spend:weighted_avg(weight=customers.spend)` by the orders-level dimension
  `status`
- **THEN** each status cell equals the spend-weighted average over the distinct customers
  associated with it, by hand-computed executed values on SQLite and DuckDB — each
  customer weighted once, never the join-multiplied figure — with unchanged result grain

#### Scenario: Definition-default parameter follows the same rule
- **WHEN** a model declares a custom aggregation whose parameter defaults to a column of
  the aggregate's root (e.g. `wsum` with `weight` defaulting to `spend`) and an
  associate-mode query selects `customers.spend:wsum` by an unattributable dimension
- **THEN** it executes with the default applied once per distinct entity, by executed
  values, identical to spelling the parameter explicitly

#### Scenario: Parameter determined over a to-one chain from the entity key
- **WHEN** an associate-mode aggregate's parameter is a column reached from the
  aggregate's root over a provably to-one join (e.g. `weight=customers.regions.pop`)
- **THEN** the query executes with the parameter read once per associated entity, by
  executed values

#### Scenario: Parameter as a cell of the operand dataset
- **WHEN** a query over `[region]` selects
  `weighted_avg(sum(amount, partition_by=[city, region]), weight=count(id, partition_by=[city, region]))`
- **THEN** each region row carries its city totals averaged with each city's row count as
  the weight, by hand-computed executed values on SQLite and DuckDB, equal to the manual
  two-stage encoding of the same computation

#### Scenario: Parameter determined over a to-one chain from the operand grain key
- **WHEN** the operand grain is an entity key (e.g. `sum(amount, partition_by=customer_id)`
  rooted at `corders`) and the parameter is a column reached from it over a provably
  to-one join (`weight=customers.region_id`)
- **THEN** the query executes with the parameter read once per cell, by executed values

#### Scenario: Parameter not determined by the grain fails closed
- **WHEN** the outer aggregation's parameter is a population-row column against a coarser
  cell grain (`weighted_avg(sum(amount, partition_by=[city, region]), weight=id)`), a
  definition default naming such a column, or an aggregate grained outside the operand
  grain (`weight=sum(amount, partition_by=product)` over a `[city, region]` operand)
- **THEN** the query fails at plan time with a typed error naming the parameter, the
  grain, and the remedy, containing no issue reference

#### Scenario: Derived re-aggregation parameter over an unpinned fanning hop fails closed
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(sum(amount, partition_by=customers.regions.id), weight=customers.regions.bad_pop)`,
  where `regions.bad_pop` is defined as `pop + region_events.value` over the one-to-many
  `regions → region_events` hop the operand grain does not pin
- **THEN** the query fails at plan time with the typed parameter error naming `weight`,
  the grain, and the `partition_by=` remedy — never the multiplying join

#### Scenario: Derived re-aggregation parameter with local-only dependencies executes
- **WHEN** the same query's weight is `customers.regions.derived_pop`, a derived column
  defined as `pop * 2` on `regions`, and the operand grain pins `regions` by its entity key
- **THEN** the query executes with each region cell weighted by twice its population, by
  hand-computed executed values on SQLite and DuckDB

#### Scenario: NULL parameter values follow SQL aggregate semantics
- **WHEN** a legal parameter is NULL for some cells (e.g. a to-one lookup with no match)
- **THEN** those cells contribute exactly as the underlying SQL aggregate treats NULL
  inputs (a NULL weight contributes nothing to `weighted_avg`), by executed values on
  SQLite and DuckDB

### Requirement: Filters restrict by association or fail loudly
A row-level filter conjunct that reaches an aggregate's root only across
non-determining paths SHALL either restrict the aggregate's population by association
— the aggregate computes over exactly the root rows related to at least one surviving
row combination, each counted once — or be loudly excluded (dropped-filter warning,
an error under `to_many_handling: "error"`) per the pushdown-scope rules in
`queries/cross-model-aggregates`. A stated restriction SHALL never be silently
ignored and SHALL never fan out an aggregation's inputs. Whether a conjunct crosses a
non-determining path is judged on its dependency closure — a reference to a derived
column whose definition crosses such a path crosses it too. Until association pushdown
reaches the population itself (DEV-1909), a row-level filter conjunct that reaches the
population root only across a non-determining path, in a query that keeps at least one
aggregate evaluated inline over the population rows, SHALL fail with a typed error naming
the filter, the hop, and the remedy — never a silently multiplied aggregate; a query
whose aggregates all compute in producers, or that has no aggregates, is unaffected, in
every `to_many_handling` mode.

#### Scenario: Cross-path filter restricts the population by association
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects
  `customers.spend:sum`
- **THEN** by executed values the metric counts exactly the customers with at least
  one order passing the predicate, each once

#### Scenario: A restriction is never silently ignored
- **WHEN** a filter conjunct cannot be applied to an aggregate's population
- **THEN** the response carries the dropped-filter warning (or the query errors under
  `to_many_handling: "error"`) — never an unrestricted value presented as restricted

#### Scenario: Population filter across a fanning hop with an inline aggregate fails closed
- **WHEN** a query rooted at `customers` filters on `orders.status = 'ok'` and selects the
  local `spend:sum`, or a query rooted at `orders` filters on `customers.regions.bad_pop > 0`
  (a derived column whose definition crosses the one-to-many `regions → region_events`
  hop) and selects the local `amount:sum`, in any `to_many_handling` mode
- **THEN** the query fails at plan time with a typed error naming the filter, the hop, and
  the remedy, containing no issue reference — never the join-multiplied total

#### Scenario: Population filter over a provably to-one path stays inline
- **WHEN** a query rooted at `orders` filters on `customers.tier = 'gold'` and selects the
  local `amount:sum`
- **THEN** the query executes with the filter applied as a plain row restriction, by
  executed values unchanged from today

#### Scenario: Dimension-only and producer-only queries are unaffected by the guard
- **WHEN** a query rooted at `customers` filters on `orders.status = 'ok'` and selects no
  measures, or selects only aggregates that compute in their own producers (cross-model,
  partitioned, or windowed)
- **THEN** the query executes, the filter applied by association where a producer inherits
  it, with values unchanged from today
