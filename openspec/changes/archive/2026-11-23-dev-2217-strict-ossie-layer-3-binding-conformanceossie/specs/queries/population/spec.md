## MODIFIED Requirements

### Requirement: Dimension-determined default

For a query omitting `population`, when exactly one dataset of the determination items reaches
the dataset of every other determination item along paths of Ossie steps
(`queries/ossie-l3-binding` › The strict binding reads the model as an Ossie model) and is a
viable candidate, that dataset SHALL be the **dimension anchor** (**Ossie precedence**, the anchor
Ossie qualifies). Otherwise the dimension anchor SHALL be the model with the
fewest total routed join hops among viable candidates, where a candidate is viable
iff every determination item binds from it under the prevailing reference grammar
along a routed chain whose every oriented hop is provably to-one (declared
many-to-one/one-to-one, or unique-key covered) or of unknown cardinality, an unknown hop
being read by association; in raw-row mode an unknown hop SHALL NOT count.
Viability probing SHALL apply, per candidate, the same anchor routing binding
applies after selection (`queries/references` › The anchor is reached by its only
to-one path): an item anchored at the candidate has an empty path; otherwise the
candidate's routes to the item's anchor are enumerated with no precedence for a
relationship incident to the candidate; every hop of a selected route, and of the
item's explicit route after its anchor, MUST satisfy the hop rule above, and hops are
counted along the full path. An item spelled by its anchor and the same item spelled as
its full path from a candidate SHALL yield the same dimension anchor, or fail closed
identically.
Determination items SHALL be the query's dimensions and time dimensions only (for
computed dimensions: their row-valued references and `partition_by` references,
never references inside an aggregation). Filters, measures, references resolving to
saved measures, order entries, and model-level filters SHALL contribute nothing to the
dimension anchor. A reference introduced only by a variable value SHALL NOT participate.
Time dimensions on `time_spine.timestamp` SHALL NOT be determination items: they factor out
of the population as its spine factor (`queries/time-spine`), and this rule
infers only the factor P from the remaining items.
When no candidate is viable and the determination items fall into groups with no join path
between any two of them, the dimension anchor SHALL be the product of each group's own
dimension anchor. With no determination items the dimension anchor SHALL be the unit, one row
with no columns. Tied minimal candidates, or connected items no candidate determines, leave the
dimension anchor undefined.

#### Scenario: Canonical default picks the coarse side

- **WHEN** `dimensions=[customers.region]` and `metrics=[sum(orders.total)]` are
  queried without `population`, with orders joined many-to-one to customers
- **THEN** the dimension anchor is `customers`, while the rows are the regions the orders reach:
  a region with no orders is not a row

#### Scenario: A field filter is not a determination item

- **WHEN** a query without `population` has `dimensions=[customers.region]` and a
  field-typed filter on `orders.status`
- **THEN** the filter is not a determination item: the dimension anchor stays `customers`, and the
  filter restricts `customers` by association

#### Scenario: Measure-typed filter does not participate

- **WHEN** a query without `population` adds a filter referencing an aggregation
  or a saved measure
- **THEN** the dimension anchor is the same as without that filter

#### Scenario: Population invariant under measures

- **WHEN** a measure is added to or removed from a query without `population`
- **THEN** the dimension anchor is unchanged (or the added measure fails to bind — never a
  different anchor)

#### Scenario: A to-many path is not determination

- **WHEN** the only route from a candidate to a queried dimension crosses a hop
  whose oriented cardinality is to-many
- **THEN** that candidate is not viable

#### Scenario: Short-form dimension infers like its full path

- **WHEN** a query without `population` has `dimensions=[regions.name,
  orders.status]` where `regions` is reachable from `orders` only through
  `customers`
- **THEN** inference routes `regions.name` per candidate and picks the same
  dimension anchor as the query spelled with `customers.regions.name`

#### Scenario: Literal resolution beats short-form reinterpretation

- **WHEN** a candidate reaches a determination item's anchor both by a direct
  many-to-one relationship and by another path of declared many-to-one steps
- **THEN** the item is ambiguous from that candidate, which is not viable for it;
  the direct relationship has no precedence

#### Scenario: Unique but fanning route is not determination

- **WHEN** the only route from a candidate to an item's anchor is unique in the
  join graph but crosses a to-many hop
- **THEN** that candidate is not viable, even though binding would route the same
  reference for other purposes

#### Scenario: Raw-row mode infers from the dimensions without unproven hops

- **WHEN** a `distinct_dimension_values=false` query omits `population`
- **THEN** the dimension anchor is inferred from the dimensions alone, an unknown-cardinality hop
  not counting, and the result has one row per row of the anchor; a field filter on a finer
  dataset restricts those rows and never changes the anchor

#### Scenario: Spine time dimensions factor out

- **WHEN** a query without `population` groups by `customers.region` and a month
  time dimension on `time_spine.timestamp`, with orders and returns each joined
  many-to-one to customers and each wired to the spine
- **THEN** P is `customers` (not `orders` or `returns`, which also determine the
  spine), and the population is `time_spine × customers`

#### Scenario: A directed cycle needs an explicit population

- **WHEN** `x`, `y` and `z` are joined many-to-one in a directed cycle `x → y → z → x`
  and a query without `population` has `dimensions=[x.v, y.v, z.v]`
- **THEN** the dimension anchor is undefined and the dimension-only query fails closed,
  because `x`, `y` and `z` each determine every item along their to-one paths with the same
  number of hops; the same query with `population="x"` executes, reading `z.v` through
  `x → y → z`

#### Scenario: Unconnected dimension groups take a product

- **WHEN** a query without `population` has `dimensions=[customers.region, tickets.priority]`,
  no join path connects `tickets` to `customers`, and it has no metrics
- **THEN** the dimension anchor is `customers × tickets` and the rows are every region paired
  with every priority

#### Scenario: An unknown-cardinality hop counts by association

- **WHEN** `orders` joins `customers` by a relationship with no declared cardinality and no
  covering unique key, and a query without `population` has `dimensions=[orders.status,
  customers.region]` and no metrics
- **THEN** the dimension anchor is `orders` and the rows are the (status, region) pairs the
  orders reach; with `distinct_dimension_values=false` the same query fails closed


#### Scenario: Ossie's anchor takes precedence over fewer hops

- **WHEN** `a → p → q → b`, `r → b` and `r → a` are many-to-one, `r`'s primary key is `b_id` so
  `b` reaches `a` to-one in two hops through `r`, and a query without `population` has
  `dimensions=[a.id, b.name]` and no metrics
- **THEN** the dimension anchor is `a`, the only dimension dataset that reaches the other along
  Ossie steps, and the rows are each `a` row with the `b.name` it reaches through `p` and `q`,
  under every conformance; `b` is not chosen though its total hops are fewer

#### Scenario: Ossie settles a tie between candidates

- **WHEN** `a → b`, `c → a` and `c → b` are many-to-one, `c`'s primary key is `a_id` so `a` and
  `c` each determine the other, and a query without `population` has
  `dimensions=[a.b_id, c.b_id]` and no metrics
- **THEN** the dimension anchor is `c`, the only dimension dataset that reaches the other along
  Ossie steps, and the query executes from it under every conformance

#### Scenario: Several anchors Ossie qualifies leave native rules in charge

- **WHEN** two or more datasets of the determination items each reach every other's dataset along
  Ossie steps
- **THEN** Ossie precedence does not apply and the dimension anchor follows the fewest-hops rule
  above
