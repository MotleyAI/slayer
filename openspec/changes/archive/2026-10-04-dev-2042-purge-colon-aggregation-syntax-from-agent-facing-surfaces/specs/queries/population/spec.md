## MODIFIED Requirements

### Requirement: Dimension-determined default

For a query omitting `source_model`, the population SHALL be the model with the
fewest total routed join hops among viable candidates, where a candidate is viable
iff every determination item binds from it under the prevailing reference grammar
along a routed chain whose every oriented hop is provably to-one (declared
many-to-one/one-to-one, or unique-key covered; unknown cardinality does not count).
Viability probing SHALL apply the same short-form auto-routing binding applies
after selection, evaluated per candidate: literal resolution takes precedence (an
item whose dotted path resolves literally from the candidate is never
reinterpreted through routing), short-form routing is attempted only where the
literal single-hop anchor does not resolve, every hop of a selected route MUST be
provably to-one for determination, and hops are counted along the selected route.
A determination item spelled as a short form and the same item spelled as its full
routed path SHALL yield the same inferred population, or fail closed identically.
Determination items SHALL be the query's dimensions and time dimensions (for
computed dimensions: their row-valued references and `partition_by` references,
never references inside an aggregation) plus the field references of field-typed
(aggregate-free) query filters. Measures, measure-typed filters, references
resolving to saved measures, order entries, and model-level filters SHALL
contribute nothing to inference. Filter references SHALL be read after
substituting source-independent variables; a reference introduced only by a
variable value does not participate. Time dimensions on `time_spine.timestamp`
and frame-bound filters on it SHALL NOT be determination items: they factor out
of the population as its spine factor (`queries/time-spine`), and this rule
infers only the factor P from the remaining items.

#### Scenario: Canonical default picks the coarse side

- **WHEN** `dimensions=[customers.region]` and `measures=[sum(orders.total)]` are
  queried without `source_model`, with orders joined many-to-one to customers
- **THEN** the population is `customers`: one row per region present among
  customers, with order totals attached and NULL where a region has no orders

#### Scenario: Field filter participates as a hidden dimension

- **WHEN** a query without `source_model` has `dimensions=[customers.region]` and a
  field-typed filter on `orders.status`
- **THEN** the population must determine `orders.status` too, so `orders` is chosen

#### Scenario: Measure-typed filter does not participate

- **WHEN** a query without `source_model` adds a filter referencing an aggregation
  or a saved measure
- **THEN** the inferred population is the same as without that filter

#### Scenario: Population invariant under measures

- **WHEN** a measure is added to or removed from a query whose population was
  inferred
- **THEN** the inferred population and the result row set are unchanged (or the
  added measure fails to bind — never a different population)

#### Scenario: Not-provably-to-one path is not determination

- **WHEN** the only route from a candidate to a queried dimension crosses a hop
  whose oriented cardinality is to-many or unknown
- **THEN** that candidate is not viable

#### Scenario: Short-form dimension infers like its full path

- **WHEN** a query without `source_model` has `dimensions=[regions.name,
  orders.status]` where `regions.name` is reachable from `orders` only as the
  routed path `customers.regions.name`
- **THEN** inference routes the short form per candidate and picks the same
  population as the query spelled with `customers.regions.name`

#### Scenario: Literal resolution beats short-form reinterpretation

- **WHEN** a determination item's first segment resolves literally from a
  candidate (a direct join edge or the candidate's own name)
- **THEN** that literal resolution is used for the probe; the item is not
  reinterpreted through a different auto-route

#### Scenario: Unique but fanning route is not determination

- **WHEN** the only route from a candidate to a short-form target is unique in the
  join graph but crosses a to-many hop
- **THEN** that candidate is not viable, even though binding would route the same
  short form for other purposes

#### Scenario: Raw-row mode uses the same rule

- **WHEN** a `distinct_dimension_values=false` query omits `source_model`
- **THEN** the same inference rule applies and the result has one row per row of
  the inferred population

#### Scenario: Spine time dimensions factor out

- **WHEN** a query without `source_model` groups by `customers.region` and a month
  time dimension on `time_spine.timestamp`, with orders and returns each joined
  many-to-one to customers and each wired to the spine
- **THEN** P is `customers` (not `orders` or `returns`, which also determine the
  spine), and the population is `time_spine × customers`
