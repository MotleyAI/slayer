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
values. The outer aggregation MAY declare its own `partition_by=`, its keys judged
against the operand dataset per › A re-aggregation's outer grain is judged against
its operand dataset, and its value behaves as a normal attached value in every consumer context —
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

## ADDED Requirements

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
default (query-dimension) member per `to_many_handling` exactly as today (broadcast with
a self-announcing warning, per-cell association, or the re-aggregation error); an
explicit `partition_by=` key by per-cell association under `"associate"`, and otherwise
a typed partition-key error naming the key, the operand grain, and the remedy (add it to
the inner `partition_by=`, or choose `"associate"`) — an explicit grain is never
silently broadcast. The re-aggregation's own outer keys SHALL NOT be judged against the
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
  and the row set equals that of `account_snapshots.balance:sum` over the same month

#### Scenario: Associated default dimension
- **WHEN** the inner is `max(account_snapshots.balance, partition_by=[account_snapshots.account_id, id])`
  (no bucket) and the query over `name` and the month runs under
  `to_many_handling: "associate"`
- **THEN** each `(name, month)` value re-aggregates the customer's account cells
  associated with that month, with the associated-dimension warning, never an internal
  error

#### Scenario: Ungrained undetermined dimension resolves per mode unchanged
- **WHEN** the same query runs under the default mode and under `"error"`
- **THEN** the default broadcasts the per-customer value across the months with the
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
- **THEN** under the default mode and `"error"` each fails at plan time with the typed
  partition-key error naming the key, the operand grain and the remedy, and under
  `"associate"` each executes by per-cell association with hand-computed values

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
  `partition_by=` crosses a fanning hop from its home still fails with the
  partition-key error in every mode

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
