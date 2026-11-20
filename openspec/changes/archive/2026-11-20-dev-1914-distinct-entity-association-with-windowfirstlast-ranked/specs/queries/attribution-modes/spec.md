## MODIFIED Requirements

### Requirement: Mode selection and the association default
A query SHALL accept `to_many_handling` with values `"broadcast"`, `"associate"`, and
`"error"`, or omit it (`null`), which selects the default. The default SHALL resolve
exactly as `"associate"`. The mode governs, uniformly for cross-model and local
aggregates alike, for every aggregation (plain, `first`/`last`, windowed, windowed
`first`/`last`), and in every consumer context (measure, composite leaf, filter, ORDER BY,
computed dimension, nested producer), how each pair of (aggregate, query dimension unattributable from the
aggregate's home) resolves. A dimension is attributable from a home iff every path in
its dependency closure — its own join path and every path the definition of a derived
column it names crosses, recursively — is provably many-to-one from that home; a
fanning hop and an unproven hop (no declared to-one cardinality and no covering unique
key) make it unattributable alike, and a dimension naming a derived column whose
definition crosses such a hop is unattributable exactly as a structural one is. The
mode MUST NOT affect aggregates all of whose grain dimensions are attributable,
dimension-free totals, or filter pushdown (semi-join pushdown applies in every mode). An
unrecognized value SHALL fail with a clear validation error. A refinement leaving the
mode unset inherits the refined query's mode.

#### Scenario: The default associates
- **WHEN** a query omitting `to_many_handling` slices a cross-model metric by an
  unattributable dimension
- **THEN** each cell carries the per-cell value over the distinct associated home
  entities — identical to the same query under `"associate"` — and the response carries
  the associated-cells warning and no broadcast warning

#### Scenario: Explicit broadcast keeps the pre-change behaviour
- **WHEN** the same query sets `to_many_handling: "broadcast"`
- **THEN** generated SQL and executed values are identical to the pre-change broadcast
  behaviour, and the response carries the broadcast warning

#### Scenario: An unproven hop associates by default
- **WHEN** `customers` declares no primary or unique key, the `orders → customers` join
  declares no cardinality, and a query omitting `to_many_handling` selects
  `sum(orders.amount)` by `customers.region` over Ossie Appendix B.1 data (EAST C1, C2;
  WEST C3; NORTH C4; orders O1 C1 100, O2 C1 200, O3 C2 50, O4 C3 300)
- **THEN** the root-less query returns EAST 350, WEST 300 and NORTH NULL, the query
  rooted at `orders` returns EAST 350 and WEST 300, and explicit `"broadcast"` returns
  650 on every region with the broadcast warning, by executed values on SQLite and DuckDB

#### Scenario: Fully attributable queries are mode-invariant
- **WHEN** the same query whose metrics are all computable at the full query grain runs
  once omitting the mode and once per mode value
- **THEN** all four runs return identical executed values with no mode-related
  warnings or errors, and every cell partitions the home rows and sums to the total

#### Scenario: Unrecognized mode value fails
- **WHEN** a query sets `to_many_handling` to a value outside the three modes
- **THEN** the query fails with a clear validation error naming the accepted values

#### Scenario: Derived fanning dimension associates by default
- **WHEN** a query rooted at `orders` selects the local `sum(amount)` and the cross-model
  `sum(customers.spend)` by the dimension `customers.regions.bad_pop`, a derived column
  defined as `pop + region_events.value` over the one-to-many `regions → region_events`
  hop, omitting `to_many_handling` (and again with `"associate"`)
- **THEN** each cell aggregates over the distinct root entities associated with that
  dimension value — each order once for the local metric, each customer once for the
  cross-model one — identical in both runs, by hand-computed executed values on SQLite
  and DuckDB

#### Scenario: Derived fanning dimension broadcasts under explicit broadcast
- **WHEN** the same query runs with `to_many_handling: "broadcast"`
- **THEN** each metric is broadcast across that dimension with a warning naming the hop,
  by executed values — never the figure multiplied once per region event

#### Scenario: Derived fanning dimension refuses under error mode
- **WHEN** the same query runs with `to_many_handling: "error"`
- **THEN** the query fails with the error-mode broadcast refusal naming the metric and the
  dimension

### Requirement: Association eligibility, input handling and row identity
Association SHALL support the full plain scalar aggregation family (including count,
count_distinct, avg, min, max, median, percentile, and stddev-class aggregations),
subject to each dialect's existing aggregate capability — grouped `median`/`percentile`
remains a `NotImplementedError` on T-SQL and MySQL, unchanged by mode (per the
divergence ledger). Every input expression of the aggregation — arguments and
aggregation-parameter fragments alike — is evaluated per associated entity (constant per
entity under the established unsafe-aggregate-inputs rule, which keeps applying
unchanged in every mode); a column-reference or aggregate-valued parameter — explicit,
positional, or supplied by the aggregation definition's default — is legal exactly when
the entity grain determines it (per `queries/semantics` › Aggregation parameters are
typed by the home dataset's grain) and is then picked once per associated entity
alongside the aggregate's own value; `count(*)` counts the distinct associated entities
per cell. An aggregation's own column filter restricts the associated entities before
per-cell aggregation. Association is needed only when at least one grain dimension is
unattributable from the aggregate's home: an aggregate whose grain dimensions the home
all determines takes the plain path in every mode, its attached inputs compiled at
their own homes. Every aggregation is **association-eligible** — `first`/`last`,
windowed and windowed `first`/`last` aggregations included (per "Ranked and windowed
association"); a parameter the entity grain does not determine is a typed parameter-grain
error, never a reason to resolve the aggregate any other way. For an association over
list children only, each associated row is one root row, so the entity grain is the root
row and a parameter the root row determines is determined. A home declaring no primary key SHALL be associated through a per-query row
identity (per "Per-query row identity for keyless homes"), never refused.

#### Scenario: Percentile attributes over the association
- **WHEN** an associate-mode query slices a cross-model percentile aggregate by an
  unattributable dimension, on a dialect that supports grouped percentile
- **THEN** each cell's value is the percentile over the distinct associated entities'
  values, by executed values; on a dialect without grouped percentile (T-SQL, MySQL)
  the query raises the established `NotImplementedError`, unchanged by mode

#### Scenario: Star-count counts distinct associated entities
- **WHEN** an associate-mode query rooted at `orders` selects `count(customers.*)` by an
  orders-level dimension
- **THEN** each cell counts the distinct customers associated with it, by executed
  values

#### Scenario: Measure-local filter restricts the association
- **WHEN** an associate-mode aggregate carries its own column filter
- **THEN** each cell aggregates only the associated entities passing the filter, and
  result cardinality is unchanged

#### Scenario: Weighted association by executed values
- **WHEN** an associate-mode query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=customers.spend)`, the custom `wsum(customers.spend)`
  whose `weight` defaults to `spend`, and `weighted_avg(customers.spend, weight=customers.regions.pop)`
  by the orders-level dimension `status`
- **THEN** each executes with hand-computed per-cell values over the distinct associated
  customers on SQLite and DuckDB — the explicit and defaulted spellings identical, a
  customer with two orders in one cell weighted once — with unchanged result grain and
  sibling values

#### Scenario: Windowed or first/last combination fails closed under explicit associate
- **WHEN** a query with `to_many_handling: "associate"` needs association for an
  aggregate that also declares `window=` or uses `first`/`last`
- **THEN** the combination no longer fails: the query executes with the per-cell values
  of "Ranked and windowed association", by executed values — never a broadcast or a
  silently wrong value

#### Scenario: A list-child association reads a root-determined parameter
- **WHEN** a query selects `weighted_avg(contractors.rate, weight=contractors.rate)` by
  `contractors.skills.value`, omitting the mode and again under `"associate"`
- **THEN** each skill cell is the weighted average over the distinct contractors listing
  that skill (go 43.33, sql 16.67 on the reference data), by executed values on SQLite
  and DuckDB, with the associated-cells warning — never a broadcast or a parameter-grain
  error

#### Scenario: Every parameter shape associates or fails with its own typed error
- **WHEN** an associated aggregate takes a field-valued custom parameter, a definition
  default, an attached aggregate- or transform-valued parameter, a parameter over a
  provably to-one path, or a parameter across a fanning hop, against keyed and keyless
  homes, in every mode
- **THEN** each executes with hand-computed per-entity values, or fails before SQL
  generation with its established typed error (input safety across the fanning hop in
  every mode) — never a broadcast fallback

#### Scenario: A keyless home associates
- **WHEN** a query rooted at `orders`, omitting the mode and again under `"associate"`,
  selects `sum(customers.spend)` and the custom `wsum(customers.spend)` (whose `weight`
  defaults to `spend`) by `status`, against a `customers` model declaring no primary or
  unique key
- **THEN** both execute with the same per-cell values as against the keyed `customers`
  model, by executed values on SQLite and DuckDB

#### Scenario: Attributable dimensions need no association
- **WHEN** an associate-mode query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=sum(amount, partition_by=customers.regions.name))`
  by `customers.tier` against a `customers` model declaring no primary or unique key,
  the `orders → customers` hop declared many-to-one
- **THEN** the query executes with the same values as under `broadcast` and no
  association warning — the home determines every dimension, so no entity
  deduplication is needed

### Requirement: Ungrained re-aggregation inners resolve per the mode
A re-aggregation whose inner aggregate takes the query grain (no explicit
`partition_by=`) and needs association SHALL resolve that inner per the query's mode
exactly as the same aggregate standing alone: associated when the mode is omitted or
`"associate"`, broadcast under `"broadcast"`, refused under `"error"` — a `first`/`last`
or windowed inner included.

#### Scenario: A plain ungrained inner associates
- **WHEN** a query rooted at `orders` selects `sum(sum(customers.spend))` by `status`,
  omitting the mode and again under `"associate"`
- **THEN** each status cell equals the associated cell of the bare `sum(customers.spend)`
  (new 290, ok 420 on the reference data), by executed values on SQLite and DuckDB

#### Scenario: A ranked ungrained inner falls back or fails
- **WHEN** the query selects `sum(last(customers.spend, customers.signup_at))` by `status`,
  omitting the mode and again under `"associate"`
- **THEN** it neither falls back nor fails: each status cell equals the associated `last`
  of that cell (new 40, ok 30 on the reference data), by executed values on SQLite and
  DuckDB; under `"broadcast"` the inner broadcasts and under `"error"` the query refuses

## ADDED Requirements

### Requirement: Ranked and windowed association
An aggregate that associates SHALL first take each cell's distinct associated home
entities — each counted once — and then apply its own reduction over them, whatever the
reduction:

- **`first` / `last`**: the cell's value is the value of the associated entity whose
  ranking key is lowest / highest. The ranking key SHALL be determined by the home (per
  "Unsafe aggregate inputs fail closed" in `queries/cross-model-aggregates`, unchanged in
  every mode); an entity with a NULL ranking key is never picked, a cell whose associated
  entities all have NULL keys is NULL, and ties are broken arbitrarily by the database.
- **`window=`**: a home entity belongs to the cell of bucket *t* iff one of its associated
  rows carrying the cell's other dimension values has its time in *t*'s trailing frame —
  the home row itself when the time axis is attributable from the home, the related rows
  reached over the home's join path when it is not. The entity counts once in that cell,
  however many of its rows fall in the frame, so cells are not additive across buckets. A
  population filter conjunct on the time axis's branch SHALL be satisfied by the same
  related row that places the entity in the frame. The buckets are the population's cells;
  a bucket whose frame holds no associated row — the NULL bucket included — takes the
  aggregation's empty value.
- **windowed `first` / `last`**: the pick runs over the entities in the cell's frame.

Every such aggregate SHALL carry the associated-cells warning when its grain is implicit,
naming each unattributable dimension — the time dimension included when the axis fans —
and none for an explicit `partition_by=` grain. Under `"broadcast"` and `"error"` it
broadcasts or refuses exactly as a plain aggregate does, except that a windowed aggregate
whose time axis is not attributable from its home SHALL fail in both modes with the typed
time-axis error naming the time dimension and naming association (omitting
`to_many_handling`) among the remedies.

#### Scenario: Cross-model first/last associates per cell
- **WHEN** a query rooted at `orders` selects `last(customers.spend, customers.signup_at)`
  and `first(customers.spend, customers.signup_at)` by `status`, omitting the mode and
  again under `"associate"`
- **THEN** `last` is new 40, ok 30 and `first` is new 100, ok 100 on the reference data —
  the pick among the distinct customers having an order of that status — by executed
  values on SQLite and DuckDB, with the associated-cells warning

#### Scenario: Unequal fan-out never changes the pick
- **WHEN** the associated `last` runs where the entity with the highest ranking key has
  one related row in the cell and other entities have several, some entities have a NULL
  ranking key, and the home is keyless
- **THEN** each cell picks the value of the highest non-NULL key among its distinct
  entities, identical for the keyed and the keyless home, by executed values

#### Scenario: Explicit partition key associates a ranked aggregate
- **WHEN** a query rooted at `orders` selects
  `last(customers.spend, customers.signup_at, partition_by=status)` by `status`, omitting
  the mode and again under `"associate"`
- **THEN** it returns new 40, ok 30 with no warning; under `"broadcast"` and `"error"` it
  fails with the unattributable-partition-key error

#### Scenario: Window over a fanning time axis associates
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` by the month of
  `orders.ordered_at`, and another rooted at `orders` selects
  `sum(customers.spend, window='1y')` by the month of `ordered_at`, omitting the mode and
  again under `"associate"`
- **THEN** both return Jan 100, Feb 250, Mar 350, Apr 460 on the reference data — each
  customer counted once in every month whose trailing year holds one of its orders — and
  the customers-rooted NULL bucket (the customer with no orders) is NULL, by executed
  values on SQLite and DuckDB, with the associated-cells warning naming the time dimension

#### Scenario: A windowed count counts distinct entities in the frame
- **WHEN** a query rooted at `orders` selects `count(customers.*, window='1y')` by the month
  of `ordered_at`, and a query rooted at `customers` selects `count(customers.*,
  window='1y')` by the month of `orders.ordered_at`
- **THEN** the months are 1, 2, 4, 6 — the active customers in each trailing year — and the
  customers-rooted NULL bucket is 0, never NULL

#### Scenario: Empty frames take each aggregation's empty value
- **WHEN** associated windowed `count(*)`, `count(<field>)`, `sum`, a custom aggregation and
  `collect` are read at a bucket whose frame holds no associated row, including the NULL
  bucket and a bucket whose source rows a filter removed
- **THEN** the counts are 0, `collect` is the empty list, and the others are NULL

#### Scenario: The axis filter binds to the in-frame related row
- **WHEN** a customer has an in-frame order whose status is not `ok` and an `ok` order
  outside the frame but no in-frame `ok` order, and the fanning-axis window queries above
  filter `orders.status = 'ok'`
- **THEN** that customer is absent from the bucket, by executed values on SQLite and
  DuckDB, in both the customers-rooted and the orders-rooted spelling (on the reference
  data without that customer: Jan 100, Feb 250, Mar 310, Apr 420)

#### Scenario: Window over an attributable axis associates the other dimensions
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` by
  `orders.status` and the month of `signup_at`, omitting the mode and again under
  `"associate"`
- **THEN** each cell sums the distinct customers having an order of that status and a
  signup in the trailing year — new 100, 250, 290; ok 100, 250, 310, 420; the NULL status
  in April 55 on the reference data — never a customer counted once per order

#### Scenario: Windowed first/last picks among the in-frame entities
- **WHEN** a query rooted at `customers` selects `last(spend, signup_at, window='1y')` by
  the month of `orders.ordered_at`
- **THEN** the months are 100, 150, 40, 30 on the reference data, by executed values; the
  same measure spelled `last(spend, window='1y')` — whose implicit ranking key is the
  fanning time dimension — fails in every mode with the input-safety error naming it

#### Scenario: A fanning time axis is refused outside association
- **WHEN** the fanning-axis window queries above run under `"broadcast"` and under
  `"error"`
- **THEN** each fails at plan time with the typed time-axis error naming the time
  dimension and the remedies, association among them, containing no issue reference

#### Scenario: Every position reads the associated ranked and windowed value
- **WHEN** an associated `last` and an associated window are used as a measure, in a
  measure filter, as an order key, and spelled with an explicit `partition_by=` naming the
  unattributable dimension
- **THEN** each position reads the same per-cell value, adding them never changes the
  result rows or any other column, and filtering to `d = v` equals the `v` cell of the
  grouped query, by executed values

#### Scenario: The windowed association renders on every dialect
- **WHEN** a ranked and a windowed association query are rendered for every supported
  dialect
- **THEN** each statement renders with the dialect's accepted syntax, and executes on the
  in-process and locally runnable engines

### Requirement: Local first/last and windowed aggregates follow the mode axis
A first/last or windowed aggregate homed at the query population SHALL resolve a query
dimension it does not attribute exactly as every other aggregate does: associated when the
mode is omitted or `"associate"`, broadcast with the ordinary warning under
`"broadcast"`, refused under `"error"`. It SHALL never be evaluated over the rows a fanning
join multiplies.

#### Scenario: Local first/last over a fanning dimension follows the mode
- **WHEN** a query rooted at `customers` selects `last(spend, signup_at)` by
  `orders.status`, omitting the mode, and under each explicit mode
- **THEN** omitting the mode and `"associate"` return new 40, ok 30, NULL 55 with the
  associated-cells warning; `"broadcast"` returns 55 on every status with the broadcast
  warning; `"error"` refuses naming the metric and `orders.status`

#### Scenario: A local window over a fanning dimension never double-counts
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` by
  `orders.status` and the month of `signup_at`, under each explicit mode
- **THEN** `"broadcast"` returns each month's trailing total on every status (Jan 100,
  Feb 250, Mar 350, Apr 515 on the reference data) with the broadcast warning, and
  `"error"` refuses — never the multiplied (ok, Jan) 200

## REMOVED Requirements

### Requirement: Broadcast fallback for association-ineligible aggregates
**Reason**: Every aggregation is association-eligible, so no aggregate needs a fallback;
the fallback and its `association_unsupported` warning field were never released.
**Migration**: None needed — the default associates these aggregates. A query wanting the
repeated value spells `partition_by=` with the dimensions the home attributes, or sets
`to_many_handling: "broadcast"`.
