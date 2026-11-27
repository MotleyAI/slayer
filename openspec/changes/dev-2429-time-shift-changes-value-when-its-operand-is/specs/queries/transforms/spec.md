## ADDED Requirements

### Requirement: time_shift evaluates its operand in the moved frame

`time_shift(E, k)` (and therefore `change` / `change_pct`) SHALL read, at each
visible cell, the value `E` takes at the shifted bucket in the same query with
its frame moved by `k`: every frame bound on the time axis's column moves so
that the frame covers exactly the source buckets of the visible buckets, in
whole buckets, and every other filter is unchanged. The rule SHALL hold for
every operand alike — a bare or composite aggregate, a cross-model leaf, a
predicate, a nested transform — so two operands equal at every cell of every
query shift to equal values. Consequently:

- an aggregate (plain, windowed, ranked or cross-model) and a nested `time_shift`
  / `change` / `change_pct` read their source bucket even when it lies outside
  the visible frame, and a forward shift reads after it;
- a nested frame-relative transform (`cumsum`, `lag`, `lead`, the rank family,
  `consecutive_periods`) evaluates over the moved frame — a running total
  restarts at the moved frame's first bucket, and `lag` steps over the moved
  frame's cells;
- a constituent whose grain does not contain the time axis keeps its in-frame
  value;
- a windowed constituent reads rows before the moved frame exactly as a windowed
  aggregate reads rows before the visible frame;
- shifts whose moved frames differ are evaluated separately.

Unshifted transforms are unchanged: `cumsum` accumulates from the first visible
bucket and `lag` / `lead` step over the visible cells.

Scenarios marked "the moved-frame seed" use `orders(region, amount, ordered_at)`
joined many-to-one to `customers(id, spend)` with North `sum(amount)` by month
2023-12 20, 2024-01 30 (dated 2024-01-10), 2024-02 40, 2024-04 30, 2024-05 5,
2024-06 7 and no March rows, queried by `orders.region` and the month of
`orders.ordered_at` with `"date_range": ["2024-02-01", "2024-05-31"]`; values
are read for North on SQLite and DuckDB.

#### Scenario: An equal rewrite shifts to the same value

- **WHEN** the moved-frame seed query requests `time_shift(sum(orders.amount), -1)`
  and `time_shift(sum(orders.amount) + 0 * cumsum(sum(orders.amount)), -1)`
- **THEN** both read Feb 30, Apr NULL and May 30, by executed values

#### Scenario: change and change_pct of an equal rewrite agree

- **WHEN** the moved-frame seed query requests `change` and `change_pct` of
  `sum(orders.amount)` and of `sum(orders.amount) + 0 * cumsum(sum(orders.amount))`
- **THEN** both `change` columns read Feb 10, Apr NULL, May −25 and both
  `change_pct` columns read Feb ⅓, Apr NULL, May −⅚, by executed values

#### Scenario: A NULL-absorbing equal rewrite agrees

- **WHEN** the moved-frame seed query requests
  `time_shift(coalesce(sum(orders.amount), 0), -1)` and
  `time_shift(coalesce(sum(orders.amount), 0) + 0 * cumsum(sum(orders.amount)), -1)`
- **THEN** the two columns are equal in every row, by executed values

#### Scenario: A shifted running total reads the moved frame

- **WHEN** the moved-frame seed query requests
  `time_shift(cumsum(sum(orders.amount)), -1)` and `cumsum(sum(orders.amount))`
- **THEN** the shift reads Feb 30, Apr NULL, May 100 — the running total from
  January, the moved frame's first bucket — while the unshifted running total
  reads Feb 40, Apr 70, May 75, by executed values

#### Scenario: Two offsets read their own moved frames

- **WHEN** the same query also requests `time_shift(cumsum(sum(orders.amount)), -2)`
- **THEN** it reads Feb 20, Apr 90, May NULL — the running total from December —
  while the `-1` shift keeps its values, by executed values

#### Scenario: A shifted lag steps over the moved frame

- **WHEN** the moved-frame seed query requests `lag(sum(orders.amount))` and
  `time_shift(lag(sum(orders.amount)), -1)`
- **THEN** `lag` reads Feb NULL, Apr 40, May 30 and the shift reads Feb NULL,
  Apr NULL, May 40, by executed values

#### Scenario: A nested shift reads outside the frame

- **WHEN** the moved-frame seed query requests
  `time_shift(time_shift(sum(orders.amount), -1), -1)`
- **THEN** it reads Feb 20 (December's sum), Apr NULL and May NULL, by executed
  values

#### Scenario: A shifted predicate reads outside the frame

- **WHEN** the moved-frame seed query requests `time_shift(sum(orders.amount) > 35, -1)`
- **THEN** it reads Feb false, Apr NULL and May false, by executed values

#### Scenario: A cross-model operand reads outside the frame

- **WHEN** the moved-frame seed query requests
  `time_shift(sum(customers.spend), -1)` and
  `time_shift(sum(orders.amount) + sum(customers.spend), -1)`
- **THEN** each row's values equal `sum(customers.spend)` and
  `sum(orders.amount) + sum(customers.spend)` at the shifted bucket of the same
  query with `"date_range": ["2024-01-01", "2024-04-30"]`, and Feb is non-NULL
  for both, by executed values

#### Scenario: A forward shift reads after the frame

- **WHEN** the moved-frame seed query requests `time_shift(sum(orders.amount), 1)`
- **THEN** it reads Feb NULL, Apr 5 and May 7, by executed values

#### Scenario: An unaligned shift reads the whole source bucket

- **WHEN** the moved-frame seed query requests
  `time_shift(sum(orders.amount), -1, 'day')`, over the plain month dimension
  and over a month dimension on `time_spine.timestamp` with the same range
- **THEN** Feb reads 30 — all of January, whose row is dated 2024-01-10 — on
  both, by executed values

#### Scenario: A shifted windowed operand reads before the moved frame

- **WHEN** the moved-frame seed query requests
  `time_shift(sum(orders.amount, window='60d'), -1)`
- **THEN** Feb equals `sum(orders.amount, window='60d')` at January in the same
  query without a date range — December's row included — by executed values

#### Scenario: A year-over-year running total

- **WHEN** a monthly query with `"date_range": "2025"` requests
  `time_shift(cumsum(sum(orders.amount)), -1, 'year')`
- **THEN** each 2025 month equals `cumsum(sum(orders.amount))` at the same month
  of 2024 in the query with `"date_range": "2024"`, by executed values

#### Scenario: Filter and order positions read the same rule

- **WHEN** the moved-frame seed query filters on
  `time_shift(sum(orders.amount) + 0 * cumsum(sum(orders.amount)), -1) > 25`,
  once as a filter and once as an order key over `sum(orders.amount)`
- **THEN** the filter keeps exactly the rows `time_shift(sum(orders.amount), -1) > 25`
  keeps (Feb and May), and the order equals ordering by that shift, by executed
  values

#### Scenario: Spine and non-spine agree

- **WHEN** each shape above, except the order position (spine queries refuse
  transform order keys until DEV-2506), is evaluated over a month dimension on
  `time_spine.timestamp` with the same range and `population: "orders"`
- **THEN** every bucket that has a row in the non-spine query carries the same
  value in the spine query wherever the shifted evaluation reads no bucket that
  only the spine query has (North's empty March holds a value for `coalesce`,
  running totals, `lag`, nested shifts and windowed operands), by executed values

## MODIFIED Requirements

### Requirement: Composite-input time_shift

`time_shift` (and therefore `change` / `change_pct`, which desugar onto it)
SHALL accept an input that is an arithmetic / scalar-call composite whose
slottable leaves are all aggregates (literals and arbitrary nesting allowed),
and an input containing a nested transform anywhere in its tree or a top-level
aggregate-typed predicate (`IN` / comparison over aggregates; `time_shift` only —
`change` / `change_pct` reject boolean-shaped inputs). Every input SHALL
evaluate by the one rule of "time_shift evaluates its operand in the moved
frame"; no input's evaluation SHALL depend on how it is built — a bare
aggregate, a local or cross-model composite, a predicate and a nested transform
are evaluated alike.

The result SHALL equal the input evaluated at the shifted time bucket within the
same partition — matching what the same expression as a measure would return
for that bucket in the query with its frame moved by the shift — and SHALL be
NULL when the input has no cell at the shifted bucket, including under
NULL-absorbing wrappers such as `coalesce`. Aggregation parameters, parameter
fragments, and column filters SHALL apply per leaf without leaking between
leaves. **Every constituent keeps its own grain** (Axioms 10 and 11.1): a
constituent whose grain — its explicit `partition_by=`, else the query grain —
contains the time axis, and any ranked or windowed leaf, is evaluated afresh in
the shifted evaluation at that grain; a constituent whose grain does not contain
the axis is constant along the axis and contributes its unshifted, in-frame
value. A partition key that is attributable across a join is honoured the same
way; a partition key whose dependency closure crosses a fanning or unproven hop
from the leaf's home resolves per the mode (`queries/attribution-modes` ›
Unattributable explicit partition keys resolve per the mode): under the default
and `"associate"` the leaf reads its associated cell at the shifted bucket, and
under `"broadcast"` and `"error"` the query fails with the typed partition-key
error, never a multiplied value. Adding the shifted measure SHALL NOT change the
row count or any other column's values.

**Frame rule.** A frame bound — `date_range`, or a relational bound with a
temporal literal on the raw column of one of the query's time dimensions —
selects the visible buckets. In the shifted evaluation every frame bound on the
time axis's column is moved so that it covers exactly the source buckets of the
visible buckets, so the earliest visible bucket is never short-changed. Every
other predicate, including a frame bound on another time dimension, a population
predicate conjoined with a frame bound and every model-level filter, applies to
the shifted evaluation unchanged, and a population restriction the query
resolves by association restricts the shifted evaluation the same way.

**Join-back.** A row reads the shifted relation at the bucket
containing its own bucket offset by the shift, matched on every other member of
the shifted relation's grain — every projected dimension and secondary time
dimension, except those an operand whose leaves are all coarser does not carry
(Axiom 11.1). For a shift whose granularity is a
multiple of the bucket this is the bucket `N` periods away; for a shift not
aligned to the bucket it is the bucket containing the offset instant.

#### Scenario: Ratio shifted one period back

- **WHEN** a query with a month time dimension and a dimension requests
  `time_shift(sum(revenue) / sum(qty), -1)`
- **THEN** each row carries the previous month's ratio for its dimension group,
  with executed values matching hand-computed expectations on SQLite and DuckDB

#### Scenario: change_pct over a ratio resets per partition

- **WHEN** a query grouped by store and month requests
  `change_pct(sum(revenue) / count(*))`
- **THEN** each store's first month yields NULL and later months yield that
  store's own month-over-month ratio growth, never another store's

#### Scenario: Missing shifted bucket yields NULL under a scalar wrap

- **WHEN** `time_shift(coalesce(sum(revenue), 0), -1)` is evaluated for the
  earliest bucket in the data
- **THEN** the shifted value is NULL (no shifted bucket exists), not 0

#### Scenario: Two differently-parameterized aggregate leaves

- **WHEN** the composite input combines two aggregates with distinct resolved
  parameters (for example a fragment-kwarg aggregation and a column-filtered
  aggregation)
- **THEN** each leaf re-aggregates with its own parameters and filter in the
  shifted period and the executed composite value matches hand-computed
  expectations

#### Scenario: Crossing aggregation parameter registers its join per leaf

- **WHEN** a composite leaf's aggregation parameter references a joined model's
  column
- **THEN** the shifted computation binds that column through the required join
  and executes correctly

#### Scenario: Nested transform input shifts the materialised series

- **WHEN** a query with a month time dimension requests
  `time_shift(cumsum(sum(revenue)), -1)`
- **THEN** each row carries the previous bucket's cumulative sum — with no frame
  bound, the value the inner transform holds at the shifted bucket — with NULL
  at the first bucket, correct by hand-computed executed values on SQLite and
  DuckDB

#### Scenario: Cross-model aggregate leaf inside a composite shifts

- **WHEN** a `time_shift` composite input contains an aggregate over another
  model's column (dotted path), alone or mixed with local aggregate leaves
- **THEN** the composite's shifted value is correct by executed values — never
  the former cross-model-leaf rejection — and equals the composite's value at
  the shifted bucket

#### Scenario: Aggregate-typed predicate input shifts as a boolean series

- **WHEN** a query requests `time_shift(sum(revenue) > 100, -1)` over a month
  time dimension
- **THEN** each row carries the previous bucket's boolean, NULL where the
  shifted bucket is absent, by executed values

#### Scenario: change over a predicate stays rejected by the typing contract

- **WHEN** a query requests `change(sum(revenue) > 100)`
- **THEN** the query fails with the existing boolean-in-arithmetic-context
  typing error (the desugared subtraction consumes a boolean), not an internal
  error

#### Scenario: Partitioned leaf keeps its grain inside a shifted composite

- **WHEN** a query over `monthly` by `region` and month requests
  `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -1)`
  (rows: North Jan 10 / Feb 20 / Mar 30, South Jan 5 / Feb 15, West Feb NULL)
- **THEN** each row carries the prior month's share of that month's
  cross-region total — Feb North ≈ 0.667, Feb South ≈ 0.333, Mar North ≈ 0.571,
  every Jan row and West Feb NULL — by executed values on SQLite and DuckDB,
  never 1.0

#### Scenario: change and change_pct over a partitioned-leaf composite

- **WHEN** the same query requests
  `change(sum(amount) / sum(amount, partition_by=[ordered_at]))` and
  `change_pct(sum(amount) / sum(amount, partition_by=[ordered_at]))`
- **THEN** the change is Feb North ≈ −0.095, Feb South ≈ +0.095, Mar North
  ≈ +0.429 and the percentage change Feb North ≈ −0.143, Feb South ≈ +0.286,
  Mar North ≈ +0.75, NULL elsewhere, by executed values

#### Scenario: Single partitioned leaf in a trivial composite

- **WHEN** the same query requests
  `time_shift(sum(amount, partition_by=[ordered_at]) / 2, -1)`
- **THEN** each row carries half the prior month's cross-region total — Feb
  North 7.5, Feb South 7.5, Mar North 17.5, West Feb 7.5 (the operand grain is
  the month alone, so a region absent from January still reads it), Jan rows
  NULL — by executed values

#### Scenario: Partitioned leaf at a non-time grain is constant along the axis

- **WHEN** the same query requests
  `time_shift(sum(amount) / sum(amount, partition_by=[region]), -1)`
- **THEN** each row carries the prior month's sum divided by the region's own
  total — Feb North ≈ 0.167, Mar North ≈ 0.333, Feb South 0.25, NULL elsewhere —
  by executed values

#### Scenario: Re-aggregation over a shifted partitioned composite

- **WHEN** a query over `monthly` with only a month time dimension requests
  `avg(time_shift(sum(amount, partition_by=[region, ordered_at]) / sum(amount, partition_by=[ordered_at]), -1))`
- **THEN** each month carries the mean of the prior month's regional shares —
  Feb 0.5, Mar ≈ 0.571, Jan NULL — by executed values

#### Scenario: Bare partitioned leaf reaches outside the date range

- **WHEN** the same query restricts the month time dimension with
  `"date_range": ["2024-02-01", "2024-03-31"]` and requests
  `time_shift(sum(amount, partition_by=[ordered_at]), -1)`
- **THEN** Feb North and Feb South carry January's total 15 and Mar North
  February's total 35, by executed values — never NULL at the first visible
  bucket

#### Scenario: Bare ranked leaf reaches outside the date range

- **WHEN** the same date-ranged query requests `time_shift(last(amount), -1)`
- **THEN** Feb North carries 10, Feb South 5 and Mar North 20 — the prior
  month's last value — by executed values

#### Scenario: Non-time partition under a date range keeps its in-frame value

- **WHEN** the same date-ranged query requests
  `time_shift(sum(amount) / sum(amount, partition_by=[region]), -1)`
- **THEN** the denominator is the region's total within the date range (North
  50, South 15): Feb North 0.2, Mar North 0.4, Feb South ≈ 0.333, by executed
  values

#### Scenario: Population predicate conjoined with a frame bound survives

- **WHEN** a query over `monthly` by `region` and month with the filter
  `ordered_at >= '2024-02-01' and region = 'North'` requests
  `time_shift(sum(amount) + sum(amount, partition_by=[ordered_at]), -1)`
- **THEN** Feb North carries 20 and Mar North 40 — the bound is moved in the
  shifted evaluation while the region predicate restricts it — by executed
  values

#### Scenario: Population restriction by association reaches the shifted evaluation

- **WHEN** a query rooted at `orders` by `status` and month carries a row
  filter across the fanning `regions → region_events` hop
  (`customers.regions.region_events.value > 40`) and requests
  `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -1)`
- **THEN** the shifted evaluation is restricted to the same associated orders:
  Feb ok ≈ 0.394, Feb new ≈ 0.606, Apr ok NULL (no associated March rows), by
  executed values

#### Scenario: Association-resolved leaf is re-evaluated by association

- **WHEN** a query rooted at `orders` in associate mode groups by the fanning
  dimension `customers.regions.region_events.value` and month and requests
  `time_shift(sum(amount), -1)`
- **THEN** each row carries the prior month's associated sum for its value —
  (50, Feb) 33, (30, Apr) 5, NULL elsewhere — by executed values, with the row
  count unchanged by the measure

#### Scenario: Partition key attributable across a join

- **WHEN** a query rooted at `orders` by `customers.tier` and month requests
  `time_shift(sum(amount) / sum(amount, partition_by=[customers.tier]), -1)`
- **THEN** the tier total is resolved through the to-one join and each row
  carries the prior month's share of it — gold Apr 0.1, NULL where the prior
  month has no rows for the tier — by executed values

#### Scenario: Shifted leaf with a fanning partition key associates per bucket

- **WHEN** a query rooted at `customers` by `orders.status` and signup month
  requests `time_shift(sum(spend, partition_by=[orders.status, signup_at]), -1)`,
  omitting the mode
- **THEN** each row carries the prior signup month's associated cell — ok Jan
  NULL, Feb 100, Mar 150, Apr 60; new Jan NULL, Feb 100, Mar 150; NULL Apr NULL
  — by executed values, never a multiplied value; under `"broadcast"` and
  `"error"` the query fails with the typed partition-key error naming
  `orders.status`

#### Scenario: Ranked leaf inside a shifted composite executes

- **WHEN** the `monthly` query requests `time_shift(last(amount) / 2, -1)`
- **THEN** each row carries half the prior month's last value — Feb North 5,
  Mar North 10, Feb South 2.5 — by executed values, never an internal error

#### Scenario: Windowed leaf inside a shifted composite keeps its window

- **WHEN** the `monthly` query requests
  `time_shift(sum(amount, window='90d') / 2, -1)`
- **THEN** each row carries half the prior month's trailing-window sum — Feb
  North 5, Mar North 15, Feb South 2.5, Jan rows and West Feb NULL — by
  executed values, never a value with the window silently dropped

#### Scenario: Bare windowed leaf reaches outside the date range

- **WHEN** the date-ranged `monthly` query requests
  `time_shift(sum(amount, window='90d'), -1)`
- **THEN** Feb North carries January's trailing-window sum 10 and Mar North
  February's 30, by executed values

#### Scenario: Two offsets share one shifted evaluation

- **WHEN** the `monthly` query requests both
  `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -1)` and
  `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -2)`
- **THEN** the generated SQL computes the shifted composite in exactly one
  relation read by both, and the two-period value at Mar North ≈ 0.667, by
  executed values

#### Scenario: Unaligned shift reads the bucket containing the offset instant

- **WHEN** the `monthly` query requests `time_shift(sum(amount), -1, 'day')`
  over month buckets
- **THEN** Feb North carries January's 10 and Mar North February's 20 — the
  bucket containing the day before each bucket start — by executed values

#### Scenario: Shifted composite over a stage time dimension

- **WHEN** an inner stage projects `region`, `ordered_at` at `month` and
  `sum(amount)` as `rev`, and the outer stage declares a month time dimension
  on that column with `"date_range": ["2024-02-01", "2024-03-31"]` and
  requests `time_shift(sum(rev) / sum(rev, partition_by=[ordered_at]), -1)` by
  `region`
- **THEN** Feb North ≈ 0.667, Feb South ≈ 0.333 and Mar North ≈ 0.571, by
  executed values on SQLite and DuckDB

#### Scenario: Shifted evaluation is closed and dependency-ordered

- **WHEN** SQL is generated for any shape above on the golden dialect set
- **THEN** every emitted CTE references only relations declared before it,
  the shifted relation and every producer it reads are scope-closed, and the
  generated SQL matches recorded golden baselines

### Requirement: Composites over transforms

A composite (arithmetic or scalar-call expression) whose operands include a transform
SHALL be legal in measure, filter and ORDER BY positions whatever its other operands are:
a local aggregate, a cross-model aggregate, another transform, or a literal, in any
nesting. Every operand the composite needs SHALL be materialised for it, including a local
aggregate that appears nowhere else in the query, and the composite SHALL evaluate to the
same value whether or not any operand is also selected on its own. The transform's value
SHALL be unchanged by the composite around it. No such
composite SHALL fail with an internal render error. The scenarios below run under
`to_many_handling: "broadcast"`, where a cross-model inner over an order month repeats
its total.

#### Scenario: Transform over a cross-model inner plus a hidden local aggregate

- **WHEN** a query rooted at `orders` with a month time dimension selects
  `change(sum(customers.spend)) + sum(amount)` and nothing else
- **THEN** each row carries the month-over-month change of the cross-model aggregate
  plus that month's local sum, by hand-computed executed values on SQLite and DuckDB,
  with NULL in the first month

#### Scenario: Hidden local operand of another aggregation kind

- **WHEN** the composite's hidden local operand is `count(*)` or `max(amount)` (for example
  `change(sum(customers.spend)) + count(*)`)
- **THEN** the query executes with the operand materialised, by executed values

#### Scenario: Transform over a crossing-fragment inner plus a hidden local aggregate

- **WHEN** a query selects `change(wscaled_sum(amount)) + sum(amount)`, where `wscaled_sum`
  is an aggregation whose default parameter crosses a join
- **THEN** each row carries the weighted-scaled delta plus the local sum, by executed
  values on SQLite and DuckDB

#### Scenario: Other transform families compose the same way

- **WHEN** the transform is `time_shift(sum(customers.spend), -1)` or
  `cumsum(sum(customers.spend))` combined with a hidden local aggregate
- **THEN** the composite executes with the transform's own semantics unchanged, by
  executed values

#### Scenario: Conditional over a transform and mixed aggregates

- **WHEN** a query selects
  `iif(change(sum(customers.spend)) > 0, sum(customers.spend), sum(amount))`
- **THEN** each row carries the cross-model total where the change is positive and the
  local sum otherwise, by executed values

#### Scenario: Selecting an operand on its own changes nothing

- **WHEN** the same query additionally selects `sum(amount)` as its own measure
- **THEN** the composite's values, the row count and every other column are identical to
  the query without it

#### Scenario: Composite in filter and order positions

- **WHEN** `change(sum(customers.spend)) + sum(amount)` appears only in a filter or only as
  an ORDER BY key, with a dimension present
- **THEN** rows are masked or sorted by the composite's value exactly as when it is
  selected, by executed values

#### Scenario: Inner varying along the time axis

- **WHEN** the cross-model inner is attributable to the query's time axis (for example
  `change(sum(orders.amount))` by customer signup month from `customers`, or
  `change(sum(customers.spend))` by `customers.signup_at` from `orders`)
- **THEN** the delta and `change_pct` percentages are correct per period by hand-computed
  executed values on SQLite and DuckDB, and adding the measure changes no row and no
  other column

#### Scenario: The issue's named shapes execute directly

- **WHEN** a query selects `change(wscaled_sum(amount))`, `change_pct(wscaled_sum(amount))`,
  `change(sum(customers.spend))` or `change_pct(sum(customers.spend))` alone
- **THEN** each executes with hand-computed values on SQLite and DuckDB, the broadcast
  cross-model inner yielding a zero delta and zero percentage after the first period
