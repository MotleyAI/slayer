## MODIFIED Requirements

### Requirement: Partitioned aggregates nested inside transforms
A transform SHALL accept a partitioned aggregate as its input when used as a measure — rank-family transforms and temporal transforms (`time_shift`, `change`, `change_pct`, `lag`, `lead`, `cumsum`, `consecutive_periods`) alike. The transform evaluates over its operand's domain — the attached aggregate's cells at its `partition_by=` grain, else at the query grain (Axiom 11.1; `queries/transforms` › Transforms evaluate over their operand's domain) — and the measure consumer broadcasts the result onto the query grain (Axiom 11.4); it MUST never fail with an internal error.

#### Scenario: Running total of partition-grain values
- WHEN a query selects dimensions `[region, city, date_trunc('month', ordered_at)]` and the measure `cumsum(sum(revenue, partition_by=[region, ordered_at]))`
- THEN the running total accumulates the region-month totals across months within each region (the operand's non-time grain), each `(region, city, month)` row carrying its region-month value, verified by executed values

#### Scenario: Ranking result rows by an attached total
- WHEN a query selects the measure `rank(sum(revenue, partition_by=region), direction='desc')`
- THEN the regions are ranked by their totals and each row carries its region's rank: `rank(sum(amount, partition_by=region))` over the `sales` fixture by `[region, city]` reads ranks 1 to 4 over the four regions with a total, Void's all-NULL total ranking NULL, never ranks over the city rows

#### Scenario: Change over a partitioned aggregate executes
- WHEN a query selects `change(sum(amount, partition_by=region))` or `change_pct(sum(amount, partition_by=region))` over a month time dimension
- THEN the query executes with the hand-computed bucket-over-previous-bucket difference (or ratio) of the attached value, instead of failing with an internal rendering error

#### Scenario: Ranking partition cells of a computed band
- WHEN a `sales` query by `[spend_band, city]` selects `rank(sum(amount, partition_by=spend_band), direction='desc')`
- THEN the bands are ranked among themselves and `lo` reads 2 on each of its cities

### Requirement: Grain-union broadcasting across consumption contexts
An arithmetic expression combining aggregates at different declared partition grains SHALL be well-defined at the union of those grains: each aggregate is computed at its own declared grain and broadcast over the grain keys it lacks. The expression SHALL be consumable at any grain refining the union — as a measure, the query grain — with every operand broadcast to the consuming rows; combining aggregates at different grains is never, by itself, an error. A transform over such an expression SHALL evaluate over the union grain's domain and broadcast its result onto the consuming rows. Filter conjuncts referencing such expressions apply after attachment and MUST NOT alter surviving rows' values.

#### Scenario: Mixed-grain arithmetic as a measure
- WHEN a query over dimensions `[region, city]` selects the measure `sum(a, partition_by=region) - sum(b, partition_by=city)`
- THEN each row's value is its region total minus its city total, by executed values

#### Scenario: Same-grain partitioned arithmetic as a measure
- WHEN a query selects the measure `sum(a, partition_by=region) - sum(b, partition_by=region)` (the degenerate union of two identical grains)
- THEN each row's value is the difference of its two broadcast region totals, by executed values

#### Scenario: Plain and partitioned aggregates mix in one expression
- WHEN a query selects the measure `sum(amount) - sum(amount, partition_by=region)`
- THEN each row's value is its query-grain total minus its broadcast region total, by executed values

#### Scenario: Transform over mixed-grain arithmetic as a measure
- WHEN a query selects the measure `rank(sum(a, partition_by=region) - sum(b, partition_by=city), direction='desc')`
- THEN the `(region, city)` cells are ranked by the difference and each row carries its cell's rank, and adding the measure changes no other column's values

#### Scenario: Filter over mixed-grain arithmetic
- WHEN a query filters on `sum(a, partition_by=region) - sum(b, partition_by=city) > 0`
- THEN only qualifying rows remain and every surviving value equals the unfiltered query's value for that row

### Requirement: Combined-consumer partition keys are query dimensions
Every explicit partition key of a partitioned aggregate consumed in a combined position — as a non-dimension measure, inside an arithmetic / scalar-call composite or transform used as a measure, as a raw ORDER BY target, or as a filter-only reference — SHALL be a query dimension or a time dimension's source column (rewritten to its truncated bucket), for local and cross-model aggregates alike. A violation SHALL fail at plan time with a clear error naming the offending key and the remedy, never with an internal join-back failure. A partitioned aggregate consumed only inside computed dimensions or as a re-aggregation operand keeps the finer-grain exemption (its partition set declares an internal producer grain; the outer aggregation is the combined consumer and carries the rule). A filter or ORDER BY reference to a computed dimension's own aggregate is a row-scope reference, legal at any partition grain: such a filter restricts the aggregated population per base row at the partition grain, and MAY therefore change surviving groups' aggregate values — unlike a combined-scope partitioned-aggregate filter, which only prunes result rows. Whether a filter conjunct or order target is a combined consumer SHALL be decided by its position typing (field vs measure) — a measure-typed conjunct that also references a computed dimension's own aggregate is a combined consumer of that aggregate. A sub-expression equal to a computed dimension's entire expression is instead that dimension's value in every position, measures included, a transform-bearing one too (per `queries/positions` › A dimension value reads the dimension's grouped value in every position), and is not a combined consumer of its partition keys; an aggregate nested inside a computed dimension's band or transform remains a combined consumer wherever else it is referenced. The rule SHALL apply identically to the explicit outer partition keys of a re-aggregation: a re-aggregation consumed only inside computed dimensions keeps the finer-grain exemption, and one consumed in any combined position carries the rule; the error SHALL name the consuming position (measure, filter or order), never a dimension.

#### Scenario: Keyless dual-role measure fails cleanly, local and cross-model alike
- WHEN the same partitioned aggregate — local or cross-model — is consumed inside a computed dimension's band or transform and selected as a measure while a partition key is not among the query dimensions
- THEN the query fails at plan time with the same clear error in both variants, naming the key and the remedy (add it to dimensions/time_dimensions), never with an internal error

#### Scenario: Keyless raw ORDER BY target fails cleanly
- WHEN `order` names the raw partitioned aggregate alongside a computed dimension banding or transforming it and a partition key is not a query dimension
- THEN the query fails at plan time with the same clear partition-key error as the measure role, local and cross-model alike

#### Scenario: Composite and transform consumers are combined positions
- WHEN the keyless partitioned aggregate is consumed inside an arithmetic composite measure or as a transform input used as a measure
- THEN the query fails at plan time with the same clear partition-key error

#### Scenario: Dimension-only consumption keeps the finer-grain exemption
- WHEN a partitioned aggregate with partition keys finer than the query grain is consumed only inside computed dimensions (with row-scope filter or ORDER-BY-name references at most)
- THEN the query plans and executes without any partition-key error

#### Scenario: Re-aggregation operands keep the finer-grain exemption
- WHEN a partitioned aggregate whose partition keys are not query dimensions is consumed only as the operand of an outer aggregation
- THEN the query plans and executes without any partition-key error, and the outer aggregation's own explicit keys still carry the combined-consumer rule

#### Scenario: Measure-typed filter over a computed dimension's finer-grained aggregate fails cleanly
- WHEN a computed dimension bands `sum(amount, partition_by=[city, region])` over dimensions `[region, <band>]` and the filter `sum(amount, partition_by=[city, region]) < sum(amount)` types as measure
- THEN the query fails at plan time with the partition-key error naming `city` — the same error as the query without the computed dimension — never with an internal join-back failure

#### Scenario: Re-aggregation in a computed dimension keeps the finer-grain exemption
- WHEN a query over dimensions `[region, x]` declares `x` = `CASE WHEN avg(sum(amount, partition_by=[city, region, product]), partition_by=[city, region]) > 30 THEN 'hi' ELSE 'lo' END`
- THEN the query executes, the re-aggregation is computed per `(city, region)` cell and broadcast onto its rows, and `sum(amount)` by `(region, x)` is correct by executed values

#### Scenario: Re-aggregation outer keys carry the rule in every combined position
- WHEN the re-aggregation `avg(sum(amount, partition_by=[city, region, product]), partition_by=[city, region])` over dimensions `[region]` (with or without a computed dimension banding it) is consumed as a measure, as a raw ORDER BY target, inside an arithmetic measure, inside a transform used as a measure, or in a measure-typed filter conjunct (including one produced by splitting an AND filter)
- THEN the query fails at plan time with the partition-key error naming `city` and the consuming position, never with an internal error

#### Scenario: Cross-model re-aggregation outer key carries the rule
- WHEN a cross-model re-aggregation declares an outer partition key that is not a query dimension and is consumed as a measure
- THEN the query fails at plan time with the partition-key error naming that key

#### Scenario: An aggregate equal to a whole computed dimension is that dimension's value
- WHEN a query over dimensions `[region, x]` declares `x` = `sum(amount, partition_by=[city, region])` and consumes `sum(amount, partition_by=[city, region])` as a measure, as an order target, or in the measure-typed filter `sum(amount, partition_by=[city, region]) < sum(amount)`
- THEN the query executes with each position reading `x`, while the same consumers beside a dimension that only bands that aggregate still fail with the partition-key error naming `city`

### Requirement: Cross-model partitioned aggregates nest inside transforms
A transform whose input contains a cross-model `partition_by=` aggregate SHALL compile:
the inner aggregate is computed in its own producer exactly as when consumed directly, and
the transform consumes the attached value like any local partitioned input. This includes
ranked inners (`first`/`last`) and holds in measure, filter, and order positions.

#### Scenario: Transform over a cross-model partitioned sum executes
- **WHEN** a query selects `rank(sum(customers.spend, partition_by=region), direction='desc')` with `region` among the query dimensions, and another selects `cumsum(sum(customers.spend, partition_by=region))` over a month time dimension
- **THEN** the rank orders the regions by their cross-model totals, correct by hand-computed executed values on SQLite and DuckDB; the `cumsum` fails with the typed time-axis error whose remedy adds the time key to `partition_by`, in measure, filter and order positions alike, as in dimension position

#### Scenario: Transform over a cross-model ranked partitioned aggregate executes
- **WHEN** a query selects `change(last(customers.spend, partition_by=region))` over a month
  time dimension with `region` among the query dimensions
- **THEN** each row carries the period-over-period difference of its region's last value,
  correct by executed values

#### Scenario: Nested producer plan shape is pinned
- **WHEN** two consumers (for example a measure and a filter) share one cross-model
  partitioned inner aggregate under transforms
- **THEN** the plan contains exactly one producer for that aggregate, attached in the
  combined phase on its complete partition grain, and the emitted SQL contains one producer
  relation for it — no duplicate producers and no incomplete attach key
