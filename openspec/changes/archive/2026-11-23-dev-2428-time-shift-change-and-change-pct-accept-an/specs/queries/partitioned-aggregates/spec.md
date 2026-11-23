## MODIFIED Requirements

### Requirement: Partitioned aggregates nested inside transforms
A transform SHALL accept a partitioned aggregate as its input when used as a measure — rank-family transforms and temporal transforms (`time_shift`, `change`, `change_pct`, `lag`, `lead`, `cumsum`, `consecutive_periods`) alike, a temporal transform's operand grain containing the query's active time bucket (Axiom 11.3; otherwise it fails per `queries/type-errors` › The time-axis violation is a type error, not an unimplemented feature). The transform evaluates over its operand's domain — the attached aggregate's cells at its `partition_by=` grain, else at the query grain (Axiom 11.1; `queries/transforms` › Transforms evaluate over their operand's domain) — and the measure consumer broadcasts the result onto the query grain (Axiom 11.4); it MUST never fail with an internal error.

#### Scenario: Running total of partition-grain values
- WHEN a query selects dimensions `[region, city, date_trunc('month', ordered_at)]` and the measure `cumsum(sum(revenue, partition_by=[region, ordered_at]))`
- THEN the running total accumulates the region-month totals across months within each region (the operand's non-time grain), each `(region, city, month)` row carrying its region-month value, verified by executed values

#### Scenario: Ranking result rows by an attached total
- WHEN a query selects the measure `rank(sum(revenue, partition_by=region), direction='desc')`
- THEN the regions are ranked by their totals and each row carries its region's rank: `rank(sum(amount, partition_by=region))` over the `sales` fixture by `[region, city]` reads ranks 1 to 4 over the four regions with a total, Void's all-NULL total ranking NULL, never ranks over the city rows

#### Scenario: Change over a partitioned aggregate executes
- WHEN a query by `region` over a month time dimension selects `change(sum(amount, partition_by=[region, ordered_at]))` or `change_pct(sum(amount, partition_by=[region, ordered_at]))`
- THEN the query executes with the hand-computed bucket-over-previous-bucket difference (or ratio) of the attached value, never an internal rendering error; the same transforms over `sum(amount, partition_by=region)` fail with a `TimeAxisError`

#### Scenario: Ranking partition cells of a computed band
- WHEN a `sales` query by `[spend_band, city]` selects `rank(sum(amount, partition_by=spend_band), direction='desc')`
- THEN the bands are ranked among themselves and `lo` reads 2 on each of its cities

### Requirement: Cross-model partitioned aggregates nest inside transforms
A transform whose input contains a cross-model `partition_by=` aggregate SHALL compile:
the inner aggregate is computed in its own producer exactly as when consumed directly, and
the transform consumes the attached value like any local partitioned input. This includes
ranked inners (`first`/`last`) and holds in measure, filter, and order positions; a
time-ordered transform's operand grain SHALL contain the query's active time bucket
(`queries/type-errors` › The time-axis violation is a type error, not an unimplemented
feature).

#### Scenario: Transform over a cross-model partitioned sum executes
- **WHEN** a query selects `rank(sum(customers.spend, partition_by=region), direction='desc')` with `region` among the query dimensions, and another selects `cumsum(sum(customers.spend, partition_by=region))` over a month time dimension
- **THEN** the rank orders the regions by their cross-model totals, correct by hand-computed executed values on SQLite and DuckDB; the `cumsum` fails with the typed time-axis error whose remedy adds the time key to `partition_by`, in measure, filter and order positions alike, as in dimension position

#### Scenario: Transform over a cross-model ranked partitioned aggregate executes
- **WHEN** a query over a month time dimension with `region` among the query dimensions
  selects `rank(last(customers.spend, partition_by=region), direction='desc')`, and another
  uses `change(last(customers.spend, partition_by=region))` as a measure, in a filter and as
  an order key
- **THEN** the rank orders the regions by their last values and each row carries its
  region's rank, correct by executed values; the `change` fails with a `TimeAxisError` in
  every position

#### Scenario: Nested producer plan shape is pinned
- **WHEN** two consumers (for example a measure and a filter) share one cross-model
  partitioned inner aggregate under transforms
- **THEN** the plan contains exactly one producer for that aggregate, attached in the
  combined phase on its complete partition grain, and the emitted SQL contains one producer
  relation for it — no duplicate producers and no incomplete attach key


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
`partition_by=` exactly as in dimension position (and as in every other position, per
`queries/type-errors` › The time-axis violation is a type error, not an unimplemented
feature); an inner
aggregate homed on a joined model whose `partition_by=` names the host's time axis — a
key reachable from the inner's own root only across a fanning join hop — SHALL resolve
per `to_many_handling` exactly like the same aggregate outside the re-aggregation:
associated per bucket under the default and `associate`, and failing with the
partition-key attributability error under `broadcast` and `error`; an
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
  `sum(cumsum(sum(amount, partition_by=[region, ordered_at])) - 1)`
- **THEN** it executes with the hand-computed sum over regions of running totals
  minus one per cell on SQLite and DuckDB, the plan carries exactly one producer for
  the transform at its `(region, month)` grain inside the carrier, the emitted
  statement has one flat `WITH`, scopes are closed, and no placeholder leaks

#### Scenario: Every transform family executes as a constituent
- **WHEN** a query over a month time dimension selects, over
  `sum(amount, partition_by=[region, ordered_at])`, a `change`, a `lag`, a
  `consecutive_periods` and a `first`/`last` constituent under `sum`
- **THEN** each executes with hand-computed values on SQLite and DuckDB: the
  shift family through its self-join series, `lag` and `consecutive_periods` per
  cell, and `first`/`last` at the collapsed `(region)` grain

#### Scenario: Windowed inner under a transform constituent fails closed
- **WHEN** a query over a month time dimension selects
  `sum(rank(sum(amount, window='90d', partition_by=region), direction='desc'))`
- **THEN** it no longer fails with the windowed time-dimension error — it executes per
  the next scenario; the former fail-closed pin is retired

#### Scenario: Windowed inner under a transform constituent executes
- **WHEN** a query over a month time dimension selects
  `sum(rank(sum(amount, window='90d', partition_by=region), direction='desc'))`
- **THEN** it executes with hand-computed values on SQLite and DuckDB — exactly one
  result row per bucket, every value non-NULL — the plan carries exactly one nested
  producer for the windowed inner grained by the query's active bucket, that exact
  bucket key is among the producer's projected grain and join keys, the emitted
  statement has one flat `WITH`, scopes are closed, and no placeholder leaks — never
  the former windowed time-dimension error

#### Scenario: A pure re-aggregation counts operand cells, not base rows
- **WHEN** a query over a month time dimension selects
  `sum(rank(sum(amount, window='90d', partition_by=region), direction='desc'))` over a source with several
  base rows per (region, month) cell
- **THEN** the outer aggregation counts each operand cell once — its home is the operand
  dataset (Axiom 2.4), so the producer joins at the query grain as a second-order
  re-aggregation, never a row-grain attach that would multiply by the base-row count;
  the mixed `sum(amount * min(X, partition_by=region))` over the same rows instead counts
  every base row, since its row leaf homes it on the model rows

#### Scenario: Cross-model grained inner naming a host time axis is a permanent boundary
- **WHEN** a query rooted at `orders` over a month time dimension selects
  `sum(cumsum(sum(customers.spend, partition_by=[customers.tier, ordered_at])))`
- **THEN** under `broadcast` and `error` it fails at plan time with the partition-key
  attributability error naming `ordered_at` and the remedy; under the default and
  `associate` it returns Jan 100 · Feb 150 · Mar 200 · Apr 420 per month, the inner
  associated per (tier, month) over the distinct customers ordering in that month, exactly
  as the bare inner is.

#### Scenario: A to-one cross-model partition key on a local-homed inner stays legal
- **WHEN** a query rooted at `orders` over a month time dimension selects
  `sum(cumsum(sum(amount, partition_by=[customers.tier, ordered_at])))`
- **THEN** it compiles: `customers.tier` is determined from `orders` over the to-one
  hop, so the constituent is grained at `(customers.tier, month)` — the boundary above
  is specific to a target-homed inner naming a host axis, not to cross-model partition
  keys

#### Scenario: Transform constituent without its time axis fails cleanly
- **WHEN** a query over a month time dimension selects
  `sum(cumsum(sum(amount, partition_by=region)))`
- **THEN** it fails with the time-axis error directing the author to include the time
  key in `partition_by=`, the same error the dimension-position form raises, and
  never returns duplicated or misgrained rows

#### Scenario: Time transform without a time dimension fails as a constituent
- **WHEN** a query with no `time_dimensions` selects
  `sum(cumsum(sum(amount, partition_by=[region, ordered_at])))`
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
- **WHEN** a query selects `rank(avg(sum(amount, partition_by=[city, region])), direction='desc')`
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
  `weighted_avg(sum(amount, partition_by=[city, region]), weight=rank(count(id, partition_by=[city, region]), direction='desc'))`
- **THEN** the rank of each city cell's row count (across all cells: Alpha/North 1;
  Alpha/South, NULL/Gap and Xi/Void 2; every other cell 5) is a constituent of the
  operand carrier, and each region carries the rank-weighted average of its city
  totals — North 55, South 580 / 7, East 60, Gap 64 / 7, Void NULL — by executed
  values on SQLite and DuckDB, the emitted SQL scope-closed

#### Scenario: Transform-valued outer parameter outside the operand grain fails closed
- **WHEN** the outer parameter is `rank(count(id, partition_by=product), direction='desc')` — a grain
  `[product]` the operand grain `[city, region]` does not determine
- **THEN** the query fails at plan time with the typed determination error naming the
  parameter and the grain, never a scope leak or a value

