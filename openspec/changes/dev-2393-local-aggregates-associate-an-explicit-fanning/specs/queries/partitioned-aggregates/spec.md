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
grain member; a transform is determined iff its result grain is — the union of its
inner aggregates' grains (each inner's explicit `partition_by=`, else the query's
dimensions and time buckets, a windowed inner contributing the query's active time
bucket), minus its time axis for `first`/`last`. A transform parameter is classified,
normalised, lowered and checked exactly as a transform source constituent, in the
keyword and positional positions alike: a time-ordered transform parameter whose
grain does not contain its time axis SHALL fail with the same time-axis error in
every position and mode; its own `partition_by=` is exempt from the
combined-consumer partition-key rule exactly as the source's constituents are; the
association and trailing-window kernels pick it once per cell exactly like an
aggregate-valued parameter; and a transform nested inside an attached aggregate
parameter resolves bottom-up like any nested attached input. The
parameter's producer is computed at the parameter's OWN home (Axiom 2, resolved
bottom-up from the parameter's own inputs) — at its own
declared grain, and its value attached into the aggregation's input relation
per home row of the aggregation, in every `to_many_handling` mode, null-safely
on the producer's complete grain; the attachment is well-defined only when the
home determines every member of the parameter's resolved grain, judged by one
home-determination rule on the plain and association paths alike; the mode
governs the aggregation's own unattributable dimensions and, inside the
parameter's own producer, its explicit-partition-key rule — never where the
parameter is computed. The source alone decides that the aggregation runs over rows: a
source with any row-level leaf, or with no attached constituent at all (a
literal), is row grain; a parameter never changes that, and an attached
parameter beside a mixed source is attached by the same mechanism as the
source's constituents. A cross-model attached parameter on a local root
attaches through a target-rooted producer like any cross-model attached
constituent, subject to the existing input-safety rules. A NULL grain-key value
forms its own cell and attaches null-safely, per the established null rules. A
parameter the operating grain does not determine SHALL keep its typed
rejection. A parameter whose own explicit partition key is unattributable from
the parameter's own home resolves per the mode inside the parameter's own
producer (`queries/attribution-modes` › Unattributable explicit partition keys
resolve per the mode); a parameter with a dependency no dialect can analyse
SHALL fail closed with the existing typed error in every mode; the enclosing
aggregation never inspects the parameter's interior. Outer parameters on
fully-attached (re-aggregation) sources keep their existing rules. The shape
SHALL be legal in measure, filter (typing as a measure) and ORDER BY positions.

#### Scenario: Associate-mode attached parameter executes
- **WHEN** a query rooted at `orders` over `[status]` under
  `to_many_handling: "associate"` selects
  `weighted_avg(customers.spend, weight=sum(amount, partition_by=customers.regions.name))`
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
- **WHEN** the associate-mode query above runs under
  `to_many_handling: "broadcast"`
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
  `weighted_avg(amount, weight=sum(customers.spend, partition_by=customers.regions.name))`
- **THEN** the parameter attaches through its target-rooted producer per order
  row and the query executes with hand-computed values; a parameter whose
  path crosses a fanning or unproven hop fails with the existing typed
  input-safety error, never wrong values

#### Scenario: Recursively nested attached parameters
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=weighted_avg(amount, weight=sum(customers.regions.pop, partition_by=customers.regions.name), partition_by=customers.regions.name))`
  — three homes: `customers` for the outer, `orders` for the per-region
  weighted average of order amounts, `regions` for the innermost population sum
- **THEN** each level's producer is rooted at its own home and attached one
  level up by its grain, and the query executes under `to_many_handling: "broadcast"`
  with the hand-computed value broadcast to every `status` cell (the region-less
  customer's NULL innermost weight excludes it), on SQLite and DuckDB

#### Scenario: Ranked transform as the attached parameter
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
  — the region cells ranked by their order-amount total, the NULL-name region
  forming its own ranked cell
- **THEN** the transform is the attached input at its result grain, and the query
  executes under `to_many_handling: "broadcast"` by `status` (broadcast, warned) and under every
  mode by `customers.tier` with identical hand-computed values — 945 / 14 on both
  `status` cells; gold 475 / 8, silver 97.5, bronze 40, the orphan order's NULL tier
  NULL by tier — on SQLite and DuckDB

#### Scenario: Transform parameter whose grain the home does not determine fails closed
- **WHEN** a query rooted at `orders` over a month time dimension on `ordered_at`
  selects
  `weighted_avg(customers.spend, weight=cumsum(sum(amount, partition_by=[customers.regions.name, ordered_at])))`
- **THEN** the query fails in every mode with the typed determination error
  naming the parameter — `customers` does not determine the order month in the
  transform's grain — never a multiplied or broadcast value

#### Scenario: Windowed aggregation with an attached parameter
- **WHEN** a query rooted at `orders` over a month time dimension on
  `customers.signup_at` selects
  `weighted_avg(customers.spend, window='1y', weight=sum(amount, partition_by=customers.regions.name))`
- **THEN** each signup-month bucket carries the trailing-window weighted average
  over the customers signed up in the window, each weighted by its region's
  total, identical under every mode with no warning (100, 125, 28080 / 267,
  33780 / 407 on the reference dataset; the orphan order's NULL bucket NULL);
  in a filter or as an ORDER BY target the same value prunes or sorts the
  buckets, with no windowed column in the response

#### Scenario: Custom aggregation with a windowed attached parameter
- **WHEN** a model defines `wsum` as `SUM({value} * {weight})` on `customers`
  and a query rooted at `orders` over a month time dimension on
  `customers.signup_at` selects
  `wsum(customers.spend, window='1y', weight=sum(amount, partition_by=customers.regions.name))`
- **THEN** each signup-month bucket carries the trailing-window sum, over the
  customers signed up in the window, of spend times the region's order total,
  identical under every mode with no warning (10000, 25000, 28080, 33780 on the
  reference dataset; the orphan order's NULL bucket NULL) — the same values as
  the constituent form, the custom definition resolving on the source owner and
  the attached weight read on each interval row

#### Scenario: Unanalysable dependency inside an attached parameter fails closed
- **WHEN** an attached parameter's own source names a derived column whose
  definition no supported dialect can parse
- **THEN** the query fails at plan time in every mode with the analyzability
  error naming that column — the parameter's own producer fails closed even
  though the enclosing aggregation never inspects it

#### Scenario: Attached parameter whose own partition key fans from its own home fails closed
- **WHEN** a query rooted at `orders` over `[status]`, and again over
  `[customers.regions.bad_pop]`, selects
  `weighted_avg(amount, weight=sum(customers.regions.pop, partition_by=customers.regions.bad_pop))`
  — the parameter's home is `regions` and `bad_pop` crosses the fanning
  `regions → region_events` hop from it
- **THEN** the parameter's own producer resolves `bad_pop` per the mode, and an
  order row does not determine the parameter's grain, so the query fails, never
  with a value: with the typed determination error naming `weight` when the mode
  is omitted or `associate`, and with the typed partition-key error naming
  `bad_pop` under `broadcast` and `error`

#### Scenario: Attached parameter keyed by a host column keeps the mode-aware rule
- **WHEN** a query rooted at `orders` over `[status]` selects
  `weighted_avg(amount, weight=sum(customers.spend, partition_by=status))` — `status`
  is a plain host column, unattributable only from the parameter's home
  `customers`
- **THEN** under `broadcast` and `error` the query fails with the explicit
  partition-key error naming `status` and `customers`; omitting the mode and under
  `associate` the parameter associates `status` per cell (per `queries/attribution-modes`) and
  each order is weighted by its cell's distinct-customer spend total — 82 / 7 for
  `ok`, 85 / 3 for `new` on the reference dataset — by executed values

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
  parameter — e.g. `weighted_avg(customers.spend, weight=sum(customers.spend, partition_by=status))`
  rooted at `orders`, whose home `customers` does not determine the grain member
  `status` — under any `to_many_handling` mode, by an attributable
  (`customers.tier`) or an unattributable (`status`) dimension
- **THEN** the query fails with the typed determination error naming the
  parameter, never wrong values; the plain and association paths refuse alike

#### Scenario: Positional transform parameter folds onto the declared name
- **WHEN** a query rooted at `orders` over `[customers.tier]` selects
  `weighted_avg(customers.spend, rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
- **THEN** it binds to the same aggregation identity as the `weight=` spelling and
  returns identical result keys and values (gold 475 / 8, silver 97.5, bronze 40)

#### Scenario: Associate-mode transform parameter
- **WHEN** the ranked-transform shape above is selected by `status` under
  `to_many_handling: "associate"`
- **THEN** each status cell aggregates its distinct associated customers weighted by
  their region's rank — `ok` 700 / 9 (customers 1, 2, 3, 5, 6), `new` 330 / 4
  (customers 1, 2, 4) — with the association warning, never NULL, on SQLite and DuckDB

#### Scenario: Local-root transform parameter
- **WHEN** a query rooted at `sales` selects
  `weighted_avg(amount, weight=rank(sum(amount, partition_by=region), direction='desc'))` by `region`,
  by `[region, city]` with the inner grained by `city`, and with no dimensions
- **THEN** it executes with the row-attached rank of the row's cell — by region North
  22.5, South 140 / 3, East 60, Gap 20 / 3, Void NULL; by region and city Alpha/North
  10, Beta/North 60, Alpha/South 20, Gamma/South 100, Delta/East 50, Epsilon/East 50,
  Zeta/East 80, NULL/Gap 6, Kappa/Gap 8, Xi/Void NULL; globally 810 / 33, the
  NULL-total Void cell carrying a NULL rank and so no weight, on SQLite and DuckDB alike

#### Scenario: Collapsing transform parameter drops the axis
- **WHEN** a query rooted at `orders` over an `ordered_at` month time dimension selects
  `weighted_avg(customers.spend, weight=last(sum(amount, partition_by=[customers.regions.name, ordered_at])))`
- **THEN** the parameter is typed at `[customers.regions.name]` — each region's
  latest-month total (North 12, South 15, NULL 47) — so the query executes: under
  `to_many_handling: "broadcast"` 8165 / 128 on every month with the broadcast warning naming the month,
  and by `[customers.tier]` and month gold 3285 / 54, silver 3000 / 27, bronze 40 on
  each of the tier's months with the same warning; never a determination error

#### Scenario: Time-ordered transform parameter without its axis fails closed
- **WHEN** a query rooted at `orders` over an `ordered_at` month time dimension selects
  `weighted_avg(customers.spend, weight=cumsum(sum(amount, partition_by=customers.regions.name)))`,
  as a measure or only as a filter
- **THEN** it fails in every mode with the time-axis error naming the `partition_by=`
  remedy, never a value

#### Scenario: Time-ordered transform parameter over a determined axis executes
- **WHEN** a query rooted at `orders` over a `customers.signup_at` month time dimension
  selects
  `weighted_avg(customers.spend, weight=cumsum(sum(amount, partition_by=[customers.regions.name, customers.signup_at])))`
- **THEN** it executes in every mode with no warning: 100, 150, 1900 / 45, 5700 / 140
  for January to April 2024, the NULL bucket NULL; as a filter `> 50` it keeps January
  and February with every other value unchanged, and as a raw ORDER BY descending it
  orders February, January, March, April

#### Scenario: Transform over an ungrained inner types at the query grain
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=rank(sum(amount), direction='desc'))`
- **THEN** by `customers.tier` the inner is grained at `[customers.tier]` and the query
  executes in every mode with no warning (gold 61.25, silver 115, bronze 40, NULL tier
  NULL); by `status` the query fails in every mode with the typed determination error
  naming `weight`, since `customers` does not determine `status`

#### Scenario: Windowed inner joins the bucket to the parameter's grain
- **WHEN** a query rooted at `orders` over an `ordered_at` month time dimension selects
  `weighted_avg(customers.spend, weight=rank(sum(amount, window='1y', partition_by=customers.regions.name), direction='desc'))`
- **THEN** it fails in every mode with the typed determination error naming `weight` —
  the order-month bucket is in the transform's grain and `customers` does not
  determine it

#### Scenario: Windowed aggregation with a transform parameter
- **WHEN** a query rooted at `orders` over a `customers.signup_at` month time dimension
  selects
  `weighted_avg(customers.spend, window='1y', weight=rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
- **THEN** each signup-month bucket carries the trailing-window weighted average over
  the customers signed up in the window, each weighted by its region's rank — 100,
  125, 510 / 7, 945 / 14, the NULL bucket NULL — identical under every mode with no
  warning, and the trailing-window producer picks the transform once per interval row

#### Scenario: Transform parameter in filter and order positions
- **WHEN** the ranked-transform shape above appears only as a filter `> 50` by
  `customers.tier`, or only as a raw ORDER BY formula descending
- **THEN** the filter keeps gold and silver with every other value unchanged, and the
  order is silver, gold, bronze

#### Scenario: Transform parameter plan shape
- **WHEN** the ranked-transform shape above is planned under the `broadcast` and `error` modes
- **THEN** the `customers`-rooted attach uses the plain kernel (not an association
  kernel), the emitted SQL is scope-closed with no placeholder leak, and adding the
  measure changes neither the row count nor any other column

#### Scenario: Nested transform parameter inside an attached aggregate parameter
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=weighted_avg(amount, weight=rank(sum(amount, partition_by=customers.regions.name), direction='desc'), partition_by=customers.regions.name))`
- **THEN** each level resolves bottom-up — the innermost rank over the region cells,
  the middle weighted average per region (North 50 / 3, South 10, NULL 23.5), the
  outer over customers — and the query executes: 7556.67 / 103.5 broadcast to both
  `status` cells under `to_many_handling: "broadcast"` with the warning, and by `customers.tier`
  under every mode gold 3316.67 / 53.33, silver 123.75, bronze 40, NULL tier NULL

#### Scenario: Cross-model transform parameter on a local root
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(amount, weight=rank(sum(customers.spend, partition_by=customers.regions.name), direction='desc'))`
- **THEN** the parameter's producer is rooted at `customers` grouped by region (spend
  North 280, South 195, NULL 40 → ranks 1, 2, 3) and attached per order row, the
  orphan order taking the NULL cell; the query executes in every mode with no warning —
  281 / 16 globally, `ok` 116 / 11 and `new` 33 by `status`

#### Scenario: Transform parameter with its own partition_by outside the query dimensions
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=rank(sum(amount, partition_by=[customers.regions.name, customers.tier]), partition_by=customers.regions.name, direction='desc'))`
  with no dimensions, and by `customers.tier`
- **THEN** the transform's own partition key is exempt from the combined-consumer
  partition-key rule exactly as a source constituent's is, and the query executes in
  every mode with no warning: 760 / 11 globally; gold 61.25, silver 115, bronze 40 by
  tier


### Requirement: A re-aggregation's outer grain is judged against its operand dataset
A re-aggregation's home is its operand dataset (per `queries/semantics` › Second-order
aggregation over attached values), so every member of its outer grain — each explicit
outer `partition_by=` key, else each query dimension of its default grain — SHALL be
judged once, against the operand dataset's grain, by the determination rule: a grain
member, or a field reached from a grain member over provably to-one join hops. A member
the operand grain determines SHALL be attributed — the outer aggregation partitions the
operand cells by it with exact values — even when the query's root model reaches it only
across a fanning or unproven join hop (a joined to-many model's column or time bucket).
A member the operand grain does not determine SHALL resolve by how it was stated: a
default (query-dimension) member per `to_many_handling` (per-cell association when the
mode is omitted or `"associate"`, broadcast with a self-announcing warning under
`"broadcast"`, the re-aggregation error under `"error"`); an explicit `partition_by=` key
by per-cell association when the mode is omitted or `"associate"`, and under
`"broadcast"` / `"error"` a typed partition-key error naming the key, the operand grain,
and the remedy (add it to the inner `partition_by=`, or omit the mode) — an explicit
grain is never silently broadcast. The re-aggregation's own outer keys SHALL NOT be judged against the
query's root model; each constituent's and parameter's own partition keys keep their
existing attributability rules. A query that the operand dataset makes well-typed SHALL
never fail with an internal error. Adding the re-aggregated measure MUST NOT change the
result row count or any other column's values, and a population cell with no operand
cells takes the outer aggregation's empty value (0 for the count family, NULL otherwise).

#### Scenario: Joined to-many time bucket determined by the operand grain
- **WHEN** a query rooted at `customers` (joined many-to-one from `account_snapshots`)
  over dimension `name` and a month time dimension on `account_snapshots.snapshot_date`
  selects `sum(<inner>)` where `<inner>` is `sum`, `max` or `last` of
  `account_snapshots.balance` with
  `partition_by=[account_snapshots.account_id, id, account_snapshots.snapshot_date]`
- **THEN** it executes on SQLite and DuckDB with each `(name, month)` value equal to the
  sum over that customer's `(account, month)` cells of the inner value — by
  hand-computed values, and for `sum` and `max` equal to the same query rooted at
  `account_snapshots` — and a customer with no snapshots keeps its row with a NULL
  month and a NULL value, never an internal error

#### Scenario: Windowed inner contributes the bucket to the operand grain
- **WHEN** the same query selects
  `sum(sum(account_snapshots.balance, window='60d', partition_by=[account_snapshots.account_id, id]))`
- **THEN** each `(name, month)` value is the sum over that customer's accounts of the
  trailing 60-day total ending in that month, by hand-computed values on SQLite and
  DuckDB

#### Scenario: Plain to-many column in the outer grain
- **WHEN** a query rooted at `customers` over dimensions
  `[name, account_snapshots.account_id]` selects
  `sum(max(account_snapshots.balance, partition_by=[account_snapshots.account_id, id]))`
- **THEN** each `(name, account)` row carries that account's maximum balance, a customer
  with no accounts keeps its NULL row, and the row set equals the query without the
  measure

#### Scenario: Outer grain holding only the joined bucket
- **WHEN** the time-bucket query drops `name` and keeps only the month
- **THEN** each month carries the sum over all `(account, customer, month)` cells in it,
  and the row set equals that of `sum(account_snapshots.balance)` over the same month

#### Scenario: Associated default dimension
- **WHEN** the inner is `max(account_snapshots.balance, partition_by=[account_snapshots.account_id, id])`
  (no bucket) and the query over `name` and the month runs omitting the mode, and again
  under `to_many_handling: "associate"`
- **THEN** in both runs each `(name, month)` value re-aggregates the customer's account cells
  associated with that month, with the associated-dimension warning, never an internal
  error

#### Scenario: Ungrained undetermined dimension resolves per mode unchanged
- **WHEN** the same query runs under `to_many_handling: "broadcast"` and under `"error"`
- **THEN** `"broadcast"` broadcasts the per-customer value across the months with the
  broadcast warning naming the month (the outer grain excluding it), and `"error"` fails
  with the re-aggregation attributability error, both exactly as before this change

#### Scenario: Explicit outer key determined by the operand grain
- **WHEN** a query over `[name, account_snapshots.account_id]` selects
  `sum(max(account_snapshots.balance, partition_by=[account_snapshots.account_id, id]), partition_by=[account_snapshots.account_id])`
- **THEN** it executes with each row carrying its account's value, never a partition-key
  error, though the query root reaches the key only across a fanning hop

#### Scenario: Explicit outer key the operand grain does not determine
- **WHEN** a query over `region` selects
  `avg(sum(amount, partition_by=city), partition_by=[region])` (a key the query root
  reaches over to-one hops), or a query over `name` and the month selects
  `sum(max(account_snapshots.balance, partition_by=[account_snapshots.account_id, id]), partition_by=[name, account_snapshots.snapshot_date])`
  (a key the query root reaches only across a fanning hop)
- **THEN** under `"broadcast"` and `"error"` each fails at plan time with the typed
  partition-key error naming the key, the operand grain and the remedy, and omitting the
  mode or under `"associate"` each executes by per-cell association with hand-computed
  values

#### Scenario: Positions and outer operators
- **WHEN** the plain to-many re-aggregation above is consumed as an ORDER BY key, inside
  an arithmetic measure, as a filter conjunct alongside the same projected measure, or
  with `count` as the outer operator over dimension `account_snapshots.account_id` alone
- **THEN** each executes by executed values, the arithmetic and order agree with the
  plain measure, and the customer without accounts carries 0 under `count`

#### Scenario: Other consumer shapes of an operand-determined outer key
- **WHEN** the re-aggregation with an operand-determined, fanning-reached outer key is
  nested one level deeper (a depth-three re-aggregation), used inside a computed
  dimension with an explicit outer grain, used as a transform input, or passed as an
  aggregate parameter of another aggregation
- **THEN** each executes by executed values, while a constituent whose own
  `partition_by=` crosses a fanning hop from its home resolves per the mode
  (`queries/attribution-modes` › Unattributable explicit partition keys resolve
  per the mode): associated cells when the mode is omitted or `associate`, the
  typed partition-key error under `broadcast` and `error`

#### Scenario: Outer grain determined through an attached-aggregate expression
- **WHEN** a query over `[name, band]` with
  `band = CASE WHEN max(account_snapshots.balance, partition_by=[account_snapshots.account_id, id]) > 100 THEN 'hi' ELSE 'lo' END`
  selects `sum(max(account_snapshots.balance, partition_by=[account_snapshots.account_id, id]))`
- **THEN** each `(name, band)` row carries the sum of that customer's account maxima in
  the band, by hand-computed values, with no reserved placeholder name in the result or
  any message

#### Scenario: Keyless outer grain
- **WHEN** a query rooted at `customers` with no dimensions selects
  `sum(max(account_snapshots.balance, partition_by=[account_snapshots.account_id, id]))`
- **THEN** it returns exactly one row carrying the sum over all account cells

