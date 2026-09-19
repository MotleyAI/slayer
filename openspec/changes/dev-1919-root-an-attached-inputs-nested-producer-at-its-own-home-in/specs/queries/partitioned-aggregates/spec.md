## MODIFIED Requirements

### Requirement: Attached parameters on row-level sources
An aggregation over a row-level source whose parameter — keyword or
positional; a definition default is Mode-A text and cannot carry an attached
value — is an attached value (an aggregate or a grained transform) SHALL
compile when the aggregation's operating grain determines the parameter. An
aggregate is determined by a grain iff that grain determines each of its
partition keys: a grain member, a column reached from a grain member over
provably to-one join hops, or an aggregate-valued key whose own grain is so
determined; an expression-valued partition key is determined only as an exact
grain member; a transform is determined iff its result grain is. The
parameter's producer is computed at the parameter's OWN home — the deepest
dataset determining the parameter's own inputs, resolved bottom-up — at its own
declared grain, and its value attached into the aggregation's input relation
per home row of the aggregation, in every `to_many_handling` mode, null-safely
on the producer's complete grain; the mode governs only the aggregation's own
unattributable dimensions, never where a parameter is computed or whether it
compiles. The source alone decides that the aggregation runs over rows: a
source with any row-level leaf, or with no attached constituent at all (a
literal), is row grain; a parameter never changes that, and an attached
parameter beside a mixed source is attached by the same mechanism as the
source's constituents. A cross-model attached parameter on a local root
attaches through a target-rooted producer like any cross-model attached
constituent, subject to the existing input-safety rules. A NULL grain-key value
forms its own cell and attaches null-safely, per the established null rules. A
parameter the operating grain does not determine SHALL keep its typed
rejection. A parameter whose own inputs are unsafe — a partition key fanning
from the parameter's own home, or a dependency no dialect can analyse — SHALL
fail closed with the existing typed error in every mode; the enclosing
aggregation never inspects the parameter's interior. Outer parameters on
fully-attached (re-aggregation) sources keep their existing rules. The shape
SHALL be legal in measure, filter (typing as a measure) and ORDER BY positions.

#### Scenario: Associate-mode attached parameter executes
- **WHEN** a query rooted at `orders` over `[status]` under
  `to_many_handling: "associate"` selects
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
- **THEN** each status cell carries the average of its distinct associated
  customers' spend, weighted by each customer's region total, by hand-computed
  executed values on SQLite and DuckDB

#### Scenario: A NULL-region entity weights by the NULL cell
- **WHEN** an associated customer has no region
- **THEN** its weight is the NULL-region cell's total — the NULL key forms its
  own cell and attaches null-safely, so an order with no customer counts in
  that same cell — by executed values

#### Scenario: Ordinary-mode attached parameter executes
- **WHEN** a locally-rooted query selects
  `weighted_avg(amount, weight=sum(amount, partition_by=region))` with `region`
  determined per row
- **THEN** it executes with the row-attached per-region weight, by executed
  values

#### Scenario: Attached parameter beside a mixed source
- **WHEN** a query over `[region]` selects
  `weighted_avg(quantity * avg(unit_price, partition_by=product), weight=sum(amount, partition_by=region))`
- **THEN** both attached inputs are row-attached into the aggregation's input
  relation, the executed value equals the hand-computed row-weighted average,
  and the emitted SQL leaks no placeholder — never a render-time failure

#### Scenario: Literal source with an attached parameter is row grain
- **WHEN** a query over `[region]` selects the model-defined
  `wsum(1, weight=sum(amount, partition_by=region))`
- **THEN** it aggregates over the population rows with the region total
  attached per row — each region equals its row count times its total — by
  executed values, never a re-aggregation over an empty grain

#### Scenario: Default-mode twin of the associate shape
- **WHEN** the associate-mode query above runs under the default
  `to_many_handling`
- **THEN** it executes: the parameter's producer is rooted at `orders` (its own
  home) grouped by the customer's region, attached per customer row inside the
  `customers`-rooted producer, and the customers-rooted weighted average over
  every customer — the orderless one weighted by its region's total, the
  region-less one by the NULL-region cell — is broadcast identically to both
  `status` cells (33780 / 407 on the reference dataset) with the usual broadcast
  warning naming `status`; under `to_many_handling: "error"` the query fails
  with the mode's refusal naming the dimension, never a parameter error

#### Scenario: Every mode agrees on attributable dimensions
- **WHEN** the same aggregation is selected by `customers.tier` — a dimension
  the home determines — under `broadcast`, `associate` and `error`
- **THEN** all three modes return identical hand-computed values on SQLite and
  DuckDB (gold 63.75, silver 138.33, bronze 40 on the reference dataset, the
  orphan order's NULL tier NULL) with no broadcast or association warning

#### Scenario: Cross-model attached parameter on a local root
- **WHEN** a query rooted at `orders` selects
  `amount:weighted_avg(weight=sum(customers.spend, partition_by=customers.regions.name))`
- **THEN** the parameter attaches through its target-rooted producer per order
  row and the query executes with hand-computed values; a parameter whose
  path crosses a fanning or unproven hop fails with the existing typed
  input-safety error, never wrong values

#### Scenario: Recursively nested attached parameters
- **WHEN** a query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=weighted_avg(amount, weight=sum(customers.regions.pop, partition_by=customers.regions.name), partition_by=customers.regions.name))`
  — three homes: `customers` for the outer, `orders` for the per-region
  weighted average of order amounts, `regions` for the innermost population sum
- **THEN** each level's producer is rooted at its own home and attached one
  level up by its grain, and the query executes under the default mode with the
  hand-computed value broadcast to every `status` cell (the region-less
  customer's NULL innermost weight excludes it), on SQLite and DuckDB

#### Scenario: Ranked transform as the attached parameter
- **WHEN** a query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=rank(sum(amount, partition_by=customers.regions.name)))`
  — the region cells ranked by their order-amount total, the NULL-name region
  forming its own ranked cell
- **THEN** the transform is the attached input at its result grain, and the query
  executes under the default mode by `status` (broadcast, warned) and under every
  mode by `customers.tier` with identical hand-computed values

#### Scenario: Transform parameter whose grain the home does not determine fails closed
- **WHEN** a query rooted at `orders` over a month time dimension on `ordered_at`
  selects
  `customers.spend:weighted_avg(weight=cumsum(sum(amount, partition_by=[customers.regions.name, ordered_at])))`
- **THEN** the query fails in every mode with the typed determination error
  naming the parameter — `customers` does not determine the order month in the
  transform's grain — never a multiplied or broadcast value

#### Scenario: Windowed aggregation with an attached parameter
- **WHEN** a query rooted at `orders` over a month time dimension on
  `customers.signup_at` selects
  `customers.spend:weighted_avg(window='1y', weight=sum(amount, partition_by=customers.regions.name))`
- **THEN** each signup-month bucket carries the trailing-window weighted average
  over the customers signed up in the window, each weighted by its region's
  total, identical under every mode with no warning (100, 125, 28080 / 267,
  33780 / 407 on the reference dataset; the orphan order's NULL bucket NULL)

#### Scenario: Unanalysable dependency inside an attached parameter fails closed
- **WHEN** an attached parameter's own source names a derived column whose
  definition no supported dialect can parse
- **THEN** the query fails at plan time in every mode with the analyzability
  error naming that column — the parameter's own producer fails closed even
  though the enclosing aggregation never inspects it

#### Scenario: Attached parameter whose own partition key fans from its own home fails closed
- **WHEN** a query rooted at `orders` selects
  `amount:weighted_avg(weight=sum(customers.spend, partition_by=status))` — the
  parameter's home is `customers` and `status` fans from it
- **THEN** the query fails in every mode with the existing mode-invariant
  partition-key error, never a multiplied value

#### Scenario: Attached parameter in filter and order positions
- **WHEN** the ordinary-mode measure or the mixed-plus-parameter measure above
  appears only in a filter, only as a raw ORDER BY formula, or is ordered by
  the name of its projected measure
- **THEN** the filter types as a measure — pruning result rows with surviving
  values unchanged — and each order form sorts by the same value the measure
  form returns

#### Scenario: Partition-key kinds of a parameter aggregate
- **WHEN** an associate-mode parameter aggregate is grained by an aggregate-valued
  key whose own grain the entity determines, or by an expression key that is not
  a grain member
- **THEN** the first executes with the parameter read once per entity and the
  second fails with the typed determination error naming the parameter

#### Scenario: Undetermined attached parameter stays rejected
- **WHEN** the aggregation's operating grain does not determine the attached
  parameter
- **THEN** the query fails with the typed determination error, never wrong
  values
