## MODIFIED Requirements

### Requirement: Dimension-determined default

For a query omitting `population`, the population SHALL be the model with the
fewest total routed join hops among viable candidates, where a candidate is viable
iff every determination item binds from it under the prevailing reference grammar
along a routed chain whose every oriented hop is provably to-one (declared
many-to-one/one-to-one, or unique-key covered; unknown cardinality does not count).
Viability probing SHALL apply, per candidate, the same anchor routing binding
applies after selection (`queries/references` › The anchor is reached by its only
to-one path): an item anchored at the candidate has an empty path; otherwise the
candidate's routes to the item's anchor are enumerated with no precedence for a
relationship incident to the candidate; every hop of a selected route, and of the
item's explicit route after its anchor, MUST be provably to-one for determination,
and hops are counted along the full path. An item spelled by its anchor and the same
item spelled as its full path from a candidate SHALL yield the same inferred
population, or fail closed identically.
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

- **WHEN** `dimensions=[customers.region]` and `metrics=[sum(orders.total)]` are
  queried without `population`, with orders joined many-to-one to customers
- **THEN** the population is `customers`: one row per region present among
  customers, with order totals attached and NULL where a region has no orders

#### Scenario: Field filter participates as a hidden dimension

- **WHEN** a query without `population` has `dimensions=[customers.region]` and a
  field-typed filter on `orders.status`
- **THEN** the population must determine `orders.status` too, so `orders` is chosen

#### Scenario: Measure-typed filter does not participate

- **WHEN** a query without `population` adds a filter referencing an aggregation
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

- **WHEN** a query without `population` has `dimensions=[regions.name,
  orders.status]` where `regions` is reachable from `orders` only through
  `customers`
- **THEN** inference routes `regions.name` per candidate and picks the same
  population as the query spelled with `customers.regions.name`

#### Scenario: Literal resolution beats short-form reinterpretation

- **WHEN** a candidate reaches a determination item's anchor both by a direct
  many-to-one relationship and by another to-one path
- **THEN** the item is ambiguous from that candidate, which is not viable for it;
  the direct relationship has no precedence

#### Scenario: Unique but fanning route is not determination

- **WHEN** the only route from a candidate to an item's anchor is unique in the
  join graph but crosses a to-many hop
- **THEN** that candidate is not viable, even though binding would route the same
  reference for other purposes

#### Scenario: Raw-row mode uses the same rule

- **WHEN** a `distinct_dimension_values=false` query omits `population`
- **THEN** the same inference rule applies and the result has one row per row of
  the inferred population

#### Scenario: Spine time dimensions factor out

- **WHEN** a query without `population` groups by `customers.region` and a month
  time dimension on `time_spine.timestamp`, with orders and returns each joined
  many-to-one to customers and each wired to the spine
- **THEN** P is `customers` (not `orders` or `returns`, which also determine the
  spine), and the population is `time_spine × customers`

#### Scenario: A directed cycle needs an explicit population

- **WHEN** `x`, `y` and `z` are joined many-to-one in a directed cycle `x → y → z → x`
  and a query without `population` has `dimensions=[x.v, y.v, z.v]`
- **THEN** inference fails closed, because `x`, `y` and `z` each determine every
  item along their to-one paths with the same number of hops; the same query with
  `population="x"` executes, reading `z.v` through `x → y → z`

### Requirement: Root recommendation follows the population rule

`recommend_population` SHALL classify each resolved item by entity type: columns
are determination items (determined, counting routed hops); saved measures,
aggregation-suffixed items and row counts `count(<dataset>.*)` are attachments
requiring reachability only and reported as such, a row count being an attachment
on its anchor. An unqualified `count(*)` SHALL be refused with the typed error of
`queries/references` › A row count names its dataset, and a bare name with the
unknown-metric error, each suggesting the qualified spellings. Selection SHALL use
the same rule as population inference so the two
surfaces cannot disagree whenever inference succeeds: a column item is determined
from a candidate exactly when the inference probe finds it viable from that
candidate. Only when no candidate is viable and an item's probe failed solely because
a hop or its route is ambiguous — never fanning or unreachable — SHALL
recommendation resolve that item along its tie-break-best fan-out-free route and emit
that full canonical path, so the recommended root executes once named as
`population`. When the items include at least one determination item, an attachment
on a dataset that no join path connects to a candidate SHALL NOT count against that
candidate; once a root is chosen, each such attachment SHALL be reported with the
spelling a query rooted there uses (its anchor, `queries/unrelated-datasets`)
and flagged `unrelated`, and SHALL never steer the choice.
Each item's path SHALL be a qualified reference (`queries/references`) that a query naming the
recommended root as `population` executes with the item's meaning: the item as written when it
resolves from the root along the chosen path, else its explicit route from the root.
Without determination items,
attachments steer selection by join-path reachability alone. Every other item
SHALL carry `unrelated` false. A feasible `root_hint` (determines all determination
items under either rule, reaches all attachments, by a join path or as unrelated
datasets when determination items exist) SHALL override selection; an infeasible one
falls back with a warning; a malformed one raises. The no-common-root coverage output
SHALL be retained with the per-item criterion updated to match. A pair whose
unaddressable edges keep an item from being reached SHALL be named in the response
warnings with the remedy to name its edges.

#### Scenario: Saved measure does not steer the recommendation

- **WHEN** items mix columns with a saved measure written `<dataset>.<metric>`
- **THEN** the saved measure is reported as an attachment and does not change the
  recommended root

#### Scenario: Recommendation matches inference

- **WHEN** the same dimension items are given to `recommend_population` and queried
  without `population`
- **THEN** the recommended root equals the inferred population

#### Scenario: Ambiguity is resolved by an explicit path

- **WHEN** `orders` reaches `regions` by two fan-out-free routes (via `customers` and
  via `warehouses`) and `recommend_population` is given `orders.status` and
  `regions.population`
- **THEN** the root is `orders` and the path for `regions.population` is
  `orders.customers.regions.population`, while
  `{"dimensions": ["orders.status", "regions.population"]}` without `population`
  fails with the ambiguous-path error

#### Scenario: Recommendation and inference agree on a directed cycle

- **WHEN** `recommend_population` is given `x.v`, `y.v` and `z.v` on the directed
  cycle `x → y → z → x`
- **THEN** no root is recommended, matching inference's failure, and the coverage
  output reports which items each candidate determines

#### Scenario: An unaddressable pair is named in the warnings

- **WHEN** `orders` declares two unnamed joins to `addresses` and
  `recommend_population` is given `orders.id` and `addresses.city`
- **THEN** the response warnings name `orders`↔`addresses` and the remedy to name
  the edges

#### Scenario: An unrelated attachment is reported through the product hop

- **WHEN** `customers` and `events` share no join path and `recommend_population` is
  given `customers.city` and `count(events.id)`
- **THEN** the root is `customers`, `count(events.id)` is reported as an attachment
  flagged `unrelated` with the path `count(events.id)`, `customers.city` is flagged
  `unrelated` false, and querying that path rooted at `customers` succeeds

#### Scenario: An unrelated column stays unreachable

- **WHEN** `recommend_population` is given `customers.city` and `events.kind`
- **THEN** no root is recommended, exactly as before this change

#### Scenario: Attachments alone keep join-path selection

- **WHEN** `recommend_population` is given only `count(customers.id)` and
  `count(events.id)`
- **THEN** no root is recommended, exactly as before this change

#### Scenario: A row count is an attachment on its dataset

- **WHEN** `recommend_population` is given `customers.region` and `count(orders.*)`
- **THEN** the root is `customers` and `count(orders.*)` is reported as an
  attachment on `orders` with the path `count(orders.*)`

#### Scenario: An unqualified row count is refused

- **WHEN** `recommend_population` is given `customers.region` and `count(*)`
- **THEN** it fails with the typed error saying `count(*)` names no dataset and
  suggesting `count(<dataset>.*)`
