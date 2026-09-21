## MODIFIED Requirements

### Requirement: Re-aggregation consumes attached operands as datasets
A partitioned aggregate, an explicitly grained transform, or a composite of them
SHALL be a legal aggregation source: the outer aggregation consumes the operand
dataset's cells per `queries/semantics` › Second-order aggregation over attached
values. A transform is a constituent like a partitioned aggregate, typed at the union of
its inner aggregates' effective grains — each inner's explicit `partition_by=`, else
the query grain (its dimensions and time buckets), a windowed inner contributing the
query's active time bucket (per `queries/computed-dimensions` › Transforms inside
dimension expressions) — and evaluated at that grain, the query's active time bucket
reaching the constituent's own producer so a windowed inner resolves it; a time-ordered
transform constituent whose grain does not contain its time axis SHALL fail with the same
time-axis error a dimension-position transform raises, the axis being named in
`partition_by=` exactly as in dimension position (a top-level measure transform is
unchanged and keeps evaluating at the query grain over the attached value); an inner
aggregate homed on a joined model whose `partition_by=` names the host's time axis — a
key reachable from the inner's own root only across a fanning join hop — SHALL fail with
the partition-key attributability error in every mode, associate included, the
mode-invariant input-safety rule for a partition key whose closure fans from the inner's
host (Axiom 8): not a deferred shape and not a boundary a mode resolves, though a future
change (DEV-1941) would let associate mode compute it by distinct-entity association per
bucket; an
axis-collapsing transform constituent (`first`, `last`) is typed at that union minus
its time axis, realised as its axis-preserving evaluation followed by an exact
per-partition pick, so the axis resolves per `to_many_handling` like any dimension
the operand grain lacks; a transform with no explicitly grained inner aggregate types
at the query grain and follows the degenerate rule. The outer
aggregation SHALL support the plain scalar aggregation family —
`sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `median`,
parametric aggregations, and model-defined custom aggregations; `count` counts
the operand's cells with a non-null value and `count_distinct` its distinct
values. The outer aggregation MAY declare its own `partition_by=`, resolved by
the same attributability and mode rules as its default (query-grain) form, and
its value behaves as a normal attached value in every consumer context —
measure, arithmetic or transform input, ORDER BY target, filter-only reference,
and computed dimension (with an explicit outer grain, per the dimension
grain-self-containment rule). `first`/`last` over an aggregated first argument
keep their transform dispatch. The outer aggregation's parameters — explicit,
positional, or defaulted by the aggregation definition — follow
`queries/semantics` › Aggregation parameters are typed by the home dataset's
grain against the operand dataset's grain: an aggregate grained within the
operand grain, a grained transform whose result grain the operand grain determines
(riding the carrier as a constituent exactly like a transform source constituent),
or a column that grain determines, is picked once per cell and
read by the outer aggregation, and its partition keys are exempt from the
combined-consumer partition-key rule exactly as the source's constituents are;
a population-row column against a coarser cell grain, a definition default
naming such a column, or an aggregate or transform grained outside the operand grain
is a typed error naming the parameter and the remedy. The outer aggregation SHALL
reject, with typed errors naming the combination and the remedy: `window=` or
ranked (`first`/`last`) aggregation over an attached operand, and a
measure-local `filter=` on the outer aggregation.

#### Scenario: Count and parametric outer aggregations
- **WHEN** a query over `[region]` selects
  `count(sum(amount, partition_by=[city, region]))` and
  `percentile(sum(amount, partition_by=[city, region]), p=0.9)`
- **THEN** each region row carries the number of its city cells with a non-null
  total and the 0.9-percentile of those totals, by executed values

#### Scenario: Grained transform constituent executes through the carrier
- **WHEN** a query over a month time dimension selects
  `sum(cumsum(amount:sum(partition_by=[region, ordered_at])) - 1)`
- **THEN** it executes with the hand-computed sum over regions of running totals
  minus one per cell on SQLite and DuckDB, the plan carries exactly one producer for
  the transform at its `(region, month)` grain inside the carrier, the emitted
  statement has one flat `WITH`, scopes are closed, and no placeholder leaks

#### Scenario: Every transform family executes as a constituent
- **WHEN** a query over a month time dimension selects, over
  `amount:sum(partition_by=[region, ordered_at])`, a `change`, a `lag`, a
  `consecutive_periods` and a `first`/`last` constituent under `sum`
- **THEN** each executes with hand-computed values on SQLite and DuckDB: the
  shift family through its self-join series, `lag` and `consecutive_periods` per
  cell, and `first`/`last` at the collapsed `(region)` grain

#### Scenario: Windowed inner under a transform constituent fails closed
- **WHEN** a query over a month time dimension selects
  `sum(rank(amount:sum(window='90d', partition_by=region)))`
- **THEN** it no longer fails with the windowed time-dimension error — it executes per
  the next scenario; the former fail-closed pin is retired

#### Scenario: Windowed inner under a transform constituent executes
- **WHEN** a query over a month time dimension selects
  `sum(rank(amount:sum(window='90d', partition_by=region)))`
- **THEN** it executes with hand-computed values on SQLite and DuckDB — exactly one
  result row per bucket, every value non-NULL — the plan carries exactly one nested
  producer for the windowed inner grained by the query's active bucket, that exact
  bucket key is among the producer's projected grain and join keys, the emitted
  statement has one flat `WITH`, scopes are closed, and no placeholder leaks — never
  the former windowed time-dimension error

#### Scenario: A pure re-aggregation counts operand cells, not base rows
- **WHEN** a query over a month time dimension selects
  `sum(rank(amount:sum(window='90d', partition_by=region)))` over a source with several
  base rows per (region, month) cell
- **THEN** the outer aggregation counts each operand cell once — its home is the operand
  dataset (Axiom 2.4), so the producer joins at the query grain as a second-order
  re-aggregation, never a row-grain attach that would multiply by the base-row count;
  the mixed `sum(amount * min(X, partition_by=region))` over the same rows instead counts
  every base row, since its row leaf homes it on the model rows

#### Scenario: Cross-model grained inner naming a host time axis is a permanent boundary
- **WHEN** a query rooted at `orders` over a month time dimension selects
  `sum(cumsum(customers.spend:sum(partition_by=[customers.tier, ordered_at])))`
- **THEN** under every mode — the default (broadcast), error AND associate — it fails at
  plan time with the partition-key attributability error naming `ordered_at` and the
  remedy: the inner is homed at `customers`, and `ordered_at` is an `orders` column
  reachable from `customers` only across the fanning `customers → orders` hop, so it is
  a mode-invariant input-safety error (Axiom 8) — never a duplicated or misgrained
  result, and never a deferral wording. The value is well-defined under distinct-entity
  association (a future change, DEV-1941, would compute it), so this is the boundary a
  fanning-crossing time key hits today, not a fundamental impossibility.

#### Scenario: A to-one cross-model partition key on a local-homed inner stays legal
- **WHEN** a query rooted at `orders` over a month time dimension selects
  `sum(cumsum(amount:sum(partition_by=[customers.tier, ordered_at])))`
- **THEN** it compiles: `customers.tier` is determined from `orders` over the to-one
  hop, so the constituent is grained at `(customers.tier, month)` — the boundary above
  is specific to a target-homed inner naming a host axis, not to cross-model partition
  keys

#### Scenario: Transform constituent without its time axis fails cleanly
- **WHEN** a query over a month time dimension selects
  `sum(cumsum(amount:sum(partition_by=region)))`
- **THEN** it fails with the time-axis error directing the author to include the time
  key in `partition_by=`, the same error the dimension-position form raises, and
  never returns duplicated or misgrained rows

#### Scenario: Time transform without a time dimension fails as a constituent
- **WHEN** a query with no `time_dimensions` selects
  `sum(cumsum(amount:sum(partition_by=[region, ordered_at])))`
- **THEN** it fails with the same unambiguous-time-dimension error a top-level
  `cumsum` raises

#### Scenario: Operand-grain parameter executes
- **WHEN** a query over `[region]` selects
  `weighted_avg(sum(amount, partition_by=[city, region]), weight=count(id, partition_by=[city, region]))`,
  and separately the custom `wavg(sum(amount, partition_by=[city, region]), weight=count(id, partition_by=[city, region]))`
  with the weight passed positionally
- **THEN** each region row carries the row-count-weighted average of its city totals,
  by hand-computed executed values on SQLite and DuckDB, the keyword and positional
  spellings identical, and the emitted SQL carries the parameter as a column of the
  operand carrier — one producer relation for the operand, no duplicate

#### Scenario: Parameter partition keys need not be query dimensions
- **WHEN** the outer aggregation's parameter is an aggregate grained at the operand grain
  and that grain's keys are not query dimensions
- **THEN** the query plans and executes without the combined-consumer partition-key
  error, exactly as the source's constituents are exempt

#### Scenario: Transform over a re-aggregated value
- **WHEN** a query selects `rank(avg(sum(amount, partition_by=[city, region])))`
- **THEN** result rows are ranked at the query grain by the attached
  re-aggregated value, and no other column's values change

#### Scenario: Explicit outer grain broadcasts per the combined rules
- **WHEN** a query over `[region, product]` selects
  `avg(sum(amount, partition_by=[city, region]), partition_by=region)`
- **THEN** the per-region average broadcasts across `product` exactly as any
  explicit-grain partitioned measure does, with no implicit-broadcast warning

#### Scenario: Filter on the re-aggregated value prunes only
- **WHEN** a query filters on `avg(sum(amount, partition_by=[city, region])) > 100`
- **THEN** only qualifying result rows remain and every surviving value equals
  the unfiltered query's value for that row

#### Scenario: Row-phase filters reach the inner producer
- **WHEN** the query carries a row-level filter conjunct
- **THEN** it restricts the inner producer's population per the established
  producer filter routing, and the re-aggregated value reflects it

#### Scenario: Row filters bound the operand dataset's cells
- **WHEN** a row filter removes every operand row of a union-grain cell and the
  operand is consumed through a NULL-restoring composite (e.g. `coalesce(…, 0)`)
- **THEN** the operand dataset excludes that cell — the composite cannot
  fabricate it — by executed values

#### Scenario: Column-reference outer parameter fails closed
- **WHEN** the outer aggregation carries a parameter the operand grain does not
  determine — explicit (`wavg(sum(amount, partition_by=[city, region]), weight=id)`)
  or defaulted by its aggregation definition to such a column
- **THEN** it fails at plan time with a typed error naming the parameter, the grain,
  and the remedy — never invalid SQL, a render-time failure, or a silently wrong value

#### Scenario: Outer window and outer filter fail closed
- **WHEN** a query selects `sum(sum(amount, partition_by=[city, region]), window='90d')`
  or gives the outer aggregation a measure-local `filter=`
- **THEN** each fails with a typed error naming the unsupported combination and
  the remedy — never a silently wrong value

#### Scenario: First and last keep transform dispatch
- **WHEN** a query selects `last(sum(amount, partition_by=[city, region]))`
- **THEN** it is the `last` transform over the aggregated series, unchanged

#### Scenario: Transform-valued outer parameter rides the carrier
- **WHEN** a query over `[region]` selects
  `weighted_avg(sum(amount, partition_by=[city, region]), weight=rank(count(id, partition_by=[city, region])))`
- **THEN** the rank of each city cell's row count (across all cells: Alpha/North 1;
  Alpha/South, NULL/Gap and Xi/Void 2; every other cell 5) is a constituent of the
  operand carrier, and each region carries the rank-weighted average of its city
  totals — North 55, South 580 / 7, East 60, Gap 64 / 7, Void NULL — by executed
  values on SQLite and DuckDB, the emitted SQL scope-closed

#### Scenario: Transform-valued outer parameter outside the operand grain fails closed
- **WHEN** the outer parameter is `rank(count(id, partition_by=product))` — a grain
  `[product]` the operand grain `[city, region]` does not determine
- **THEN** the query fails at plan time with the typed determination error naming the
  parameter and the grain, never a scope leak or a value

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
parameter's producer is computed at the parameter's OWN home — the deepest
dataset determining the parameter's own inputs, resolved bottom-up — at its own
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
  mode by `customers.tier` with identical hand-computed values — 945 / 14 on both
  `status` cells; gold 475 / 8, silver 97.5, bronze 40, the orphan order's NULL tier
  NULL by tier — on SQLite and DuckDB

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
  33780 / 407 on the reference dataset; the orphan order's NULL bucket NULL);
  in a filter or as an ORDER BY target the same value prunes or sorts the
  buckets, with no windowed column in the response

#### Scenario: Custom aggregation with a windowed attached parameter
- **WHEN** a model defines `wsum` as `SUM({value} * {weight})` on `customers`
  and a query rooted at `orders` over a month time dimension on
  `customers.signup_at` selects
  `customers.spend:wsum(window='1y', weight=sum(amount, partition_by=customers.regions.name))`
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
- **WHEN** a query rooted at `orders` selects
  `amount:weighted_avg(weight=sum(customers.regions.pop, partition_by=customers.regions.bad_pop))`
  — the parameter's home is `regions` and `bad_pop` crosses the fanning
  `regions → region_events` hop from it
- **THEN** the query fails in every mode with the existing mode-invariant
  partition-key error naming `bad_pop` and the hop `region_events`, never a
  multiplied value

#### Scenario: Attached parameter keyed by a host column keeps the mode-aware rule
- **WHEN** a query rooted at `orders` over `[status]` selects
  `amount:weighted_avg(weight=sum(customers.spend, partition_by=status))` — `status`
  is a plain host column, unattributable only from the parameter's home
  `customers`
- **THEN** under `broadcast` and `error` the query fails with the explicit
  partition-key error naming `status` and `customers`; under `associate` the
  parameter associates `status` per cell (per `queries/attribution-modes`) and
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
  parameter — e.g. `customers.spend:weighted_avg(weight=sum(customers.spend, partition_by=status))`
  rooted at `orders`, whose home `customers` does not determine the grain member
  `status` — under any `to_many_handling` mode, by an attributable
  (`customers.tier`) or an unattributable (`status`) dimension
- **THEN** the query fails with the typed determination error naming the
  parameter, never wrong values; the plain and association paths refuse alike

#### Scenario: Positional transform parameter folds onto the declared name
- **WHEN** a query rooted at `orders` over `[customers.tier]` selects
  `customers.spend:weighted_avg(rank(sum(amount, partition_by=customers.regions.name)))`
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
  `weighted_avg(amount, weight=rank(sum(amount, partition_by=region)))` by `region`,
  by `[region, city]` with the inner grained by `city`, and with no dimensions
- **THEN** it executes with the row-attached rank of the row's cell — by region North
  22.5, South 140 / 3, East 60, Gap 20 / 3, Void NULL; by region and city Alpha/North
  10, Beta/North 60, Alpha/South 20, Gamma/South 100, Delta/East 50, Epsilon/East 50,
  Zeta/East 80, NULL/Gap 6, Kappa/Gap 8, Xi/Void NULL; globally 810 / 43, the
  NULL-total Void cell ranking last (weight 5) on SQLite and DuckDB alike

#### Scenario: Collapsing transform parameter drops the axis
- **WHEN** a query rooted at `orders` over an `ordered_at` month time dimension selects
  `customers.spend:weighted_avg(weight=last(sum(amount, partition_by=[customers.regions.name, ordered_at])))`
- **THEN** the parameter is typed at `[customers.regions.name]` — each region's
  latest-month total (North 12, South 15, NULL 47) — so the query executes: under the
  default mode 8165 / 128 on every month with the broadcast warning naming the month,
  and by `[customers.tier]` and month gold 3285 / 54, silver 3000 / 27, bronze 40 on
  each of the tier's months with the same warning; never a determination error

#### Scenario: Time-ordered transform parameter without its axis fails closed
- **WHEN** a query rooted at `orders` over an `ordered_at` month time dimension selects
  `customers.spend:weighted_avg(weight=cumsum(sum(amount, partition_by=customers.regions.name)))`,
  as a measure or only as a filter
- **THEN** it fails in every mode with the time-axis error naming the `partition_by=`
  remedy, never a value

#### Scenario: Time-ordered transform parameter over a determined axis executes
- **WHEN** a query rooted at `orders` over a `customers.signup_at` month time dimension
  selects
  `customers.spend:weighted_avg(weight=cumsum(sum(amount, partition_by=[customers.regions.name, customers.signup_at])))`
- **THEN** it executes in every mode with no warning: 100, 150, 1900 / 45, 5700 / 140
  for January to April 2024, the NULL bucket NULL; as a filter `> 50` it keeps January
  and February with every other value unchanged, and as a raw ORDER BY descending it
  orders February, January, March, April

#### Scenario: Transform over an ungrained inner types at the query grain
- **WHEN** a query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=rank(sum(amount)))`
- **THEN** by `customers.tier` the inner is grained at `[customers.tier]` and the query
  executes in every mode with no warning (gold 61.25, silver 115, bronze 40, NULL tier
  NULL); by `status` the query fails in every mode with the typed determination error
  naming `weight`, since `customers` does not determine `status`

#### Scenario: Windowed inner joins the bucket to the parameter's grain
- **WHEN** a query rooted at `orders` over an `ordered_at` month time dimension selects
  `customers.spend:weighted_avg(weight=rank(sum(amount, window='1y', partition_by=customers.regions.name)))`
- **THEN** it fails in every mode with the typed determination error naming `weight` —
  the order-month bucket is in the transform's grain and `customers` does not
  determine it

#### Scenario: Windowed aggregation with a transform parameter
- **WHEN** a query rooted at `orders` over a `customers.signup_at` month time dimension
  selects
  `customers.spend:weighted_avg(window='1y', weight=rank(sum(amount, partition_by=customers.regions.name)))`
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
- **WHEN** the ranked-transform shape above is planned under the default and error modes
- **THEN** the `customers`-rooted attach uses the plain kernel (not an association
  kernel), the emitted SQL is scope-closed with no placeholder leak, and adding the
  measure changes neither the row count nor any other column

#### Scenario: Nested transform parameter inside an attached aggregate parameter
- **WHEN** a query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=weighted_avg(amount, weight=rank(sum(amount, partition_by=customers.regions.name)), partition_by=customers.regions.name))`
- **THEN** each level resolves bottom-up — the innermost rank over the region cells,
  the middle weighted average per region (North 50 / 3, South 10, NULL 23.5), the
  outer over customers — and the query executes: 7556.67 / 103.5 broadcast to both
  `status` cells under the default mode with the warning, and by `customers.tier`
  under every mode gold 3316.67 / 53.33, silver 123.75, bronze 40, NULL tier NULL

#### Scenario: Cross-model transform parameter on a local root
- **WHEN** a query rooted at `orders` selects
  `amount:weighted_avg(weight=rank(sum(customers.spend, partition_by=customers.regions.name)))`
- **THEN** the parameter's producer is rooted at `customers` grouped by region (spend
  North 280, South 195, NULL 40 → ranks 1, 2, 3) and attached per order row, the
  orphan order taking the NULL cell; the query executes in every mode with no warning —
  281 / 16 globally, `ok` 116 / 11 and `new` 33 by `status`

#### Scenario: Transform parameter with its own partition_by outside the query dimensions
- **WHEN** a query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=rank(sum(amount, partition_by=[customers.regions.name, customers.tier]), partition_by=customers.regions.name))`
  with no dimensions, and by `customers.tier`
- **THEN** the transform's own partition key is exempt from the combined-consumer
  partition-key rule exactly as a source constituent's is, and the query executes in
  every mode with no warning: 760 / 11 globally; gold 61.25, silver 115, bronze 40 by
  tier
