# queries/partitioned-aggregates Specification

## Purpose
Defines how `partition_by=` aggregations (measures computed at an explicitly declared grain, attached back to the query rows) compose with the rest of the query surface: `window=`, `first`/`last`, transforms, filters, ORDER BY, and other measure kinds.

## Requirements

### Requirement: Partitioned aggregate combined with window=
An aggregation SHALL accept `partition_by=` and `window=` together. The value is the rolling-window aggregate computed at the partition grain, evaluated per time bucket of the query's active time dimension, attached to every query row of the partition.

#### Scenario: Rolling total at a coarser grain
- WHEN a query selects dimensions `[region, city, month(ordered_at)]` and the measure `revenue:sum(partition_by=region, window='90d')`
- THEN every row carries the trailing-90-day region-wide revenue total as of that row's month, identical across cities of the same region and month, and adding the measure changes neither the row count nor any other column's values

#### Scenario: Requires a resolvable time dimension
- WHEN `window=` is combined with `partition_by=` in a query with no resolvable time dimension
- THEN the query fails with the same clear time-dimension resolution error that plain `window=` measures raise

### Requirement: Partitioned first and last aggregations
`first` and `last` aggregations SHALL accept `partition_by=`, returning the value at the earliest/latest ranking timestamp within the partition, attached to every query row of the partition without multiplying rows.

#### Scenario: Region-wide latest value on city rows
- WHEN a query selects dimensions `[region, city]` and the measure `price:last(partition_by=region)`
- THEN every city row of a region shows the price at the region's latest ranking timestamp, and the row count equals the same query without the measure

#### Scenario: Temporal partition keys do not hijack the ranking column
- WHEN the partition set contains a time-bucket dimension and the aggregation does not name an explicit ranking column
- THEN ranking still uses the model's resolved ranking time column, not the partition key

### Requirement: Partitioned aggregates nested inside transforms
A transform SHALL accept a partitioned aggregate as its input when used as a measure — rank-family transforms and temporal transforms (`time_shift`, `change`, `change_pct`, `lag`, `lead`, `cumsum`, `consecutive_periods`) alike. The transform evaluates at its operand grain — the attached aggregate's `partition_by=`, else the query grain (Axiom 11.1) — and the measure consumer broadcasts the result onto the query grain (Axiom 11.4); it MUST never fail with an internal error.

#### Scenario: Running total of partition-grain values
- WHEN a query selects dimensions `[region, city, month(ordered_at)]` and the measure `cumsum(revenue:sum(partition_by=[region, ordered_at]))`
- THEN each row's value is the cumulative sum across months, within the row's non-time dimensions, of the attached region-month totals, verified by executed values

#### Scenario: Ranking result rows by an attached total
- WHEN a query selects the measure `rank(revenue:sum(partition_by=region))`
- THEN result rows are ranked by their attached region total at the query grain

#### Scenario: Change over a partitioned aggregate executes
- WHEN a query selects `change(amount:sum(partition_by=region))` or `change_pct(amount:sum(partition_by=region))` over a month time dimension
- THEN the query executes with the hand-computed bucket-over-previous-bucket difference (or ratio) of the attached value, instead of failing with an internal rendering error

### Requirement: Filters referencing partitioned aggregates
Query filters SHALL be able to reference partitioned aggregates. Such predicates type as measures: they apply after attachment, prune result rows, and MUST NOT alter the aggregate values of surviving rows. Each top-level conjunct of a filter types independently as a field (aggregate-free after reference resolution) or a measure (legal as a declared measure in the same query); a conjunct valid as neither fails with a clear typing error naming both failed typings.

#### Scenario: Keep rows whose partition total qualifies
- WHEN a query over dimensions `[region, city]` filters on `revenue:sum(partition_by=region) > 5000`
- THEN only rows belonging to qualifying regions remain and every remaining value equals the unfiltered query's value for that row

#### Scenario: Conjunction splits by scope
- WHEN one filter string is an AND of a partitioned-aggregate predicate and a row-level predicate
- THEN the results equal the same query with the two predicates given as separate filters

#### Scenario: Mixing with a plain aggregate in one predicate is legal
- WHEN a single predicate combines a partitioned-aggregate reference with a plain aggregate reference (e.g. `revenue:sum(partition_by=region) > 5000 AND revenue:sum > 100`)
- THEN the whole predicate evaluates after aggregation and attachment, and the results are correct by executed values

#### Scenario: Mixing a computed dimension's aggregate with a row-level reference is legal
- WHEN a query bands a computed dimension on `amount:sum(partition_by=city)` and one predicate combines that aggregate with a row-level reference (e.g. `amount:sum(partition_by=city) > 5000 AND status = 'ok'`)
- THEN the predicate types as field — both references resolve at row scope — and applies per base row before re-aggregation, correct by executed values, never the former split-the-conjuncts error

#### Scenario: Mixing a computed dimension's aggregate with a plain aggregate is legal
- WHEN the same query's predicate combines the computed dimension's aggregate with a plain aggregate (e.g. `amount:sum(partition_by=city) > 5000 AND amount:sum > 100`)
- THEN the predicate types as measure and masks result cells after evaluation at query grain, with surviving values unchanged, by executed values

#### Scenario: No common scope fails closed
- WHEN a single OR predicate mixes a partitioned-aggregate reference with a reference resolvable only before aggregation
- THEN the query fails with the typing error naming the aggregate that blocks field typing and the row-level reference that blocks measure typing — not with an internal error

### Requirement: Row and combined attachment coexistence
A query SHALL support partitioned aggregates consumed inside computed dimensions and as measures simultaneously, whether they share the same partition set, use independent partition sets, or are the very same aggregate in both roles.

#### Scenario: Dimension banding and a partitioned measure together
- WHEN a query has a computed dimension banding `amount:sum(partition_by=city)` and the measure `amount:sum(partition_by=region)`
- THEN both the band and the measure are correct by executed values in one result

#### Scenario: Same aggregate in both roles
- WHEN the same partitioned aggregate appears inside a computed dimension and as a directly selected measure
- THEN both values are correct and consistent with each other

#### Scenario: ORDER BY the raw aggregate alongside a computed dimension using it
- WHEN a computed dimension bands `amount:sum(partition_by=city)` and `order` names the raw `amount:sum(partition_by=city)`
- THEN rows sort by the partition-grain value; ordering by the computed dimension's name instead sorts by the banded value; neither form raises an internal placeholder error

#### Scenario: Cross-model dual role without the partition key in the grain is rejected
- WHEN the same cross-model partitioned aggregate is consumed by a computed dimension and selected as a measure while its partition key is not among the query dimensions
- THEN the query fails at plan time with the clear partition-key error of the combined-consumer requirement — identical in shape to the local variant — never with an internal join-back failure

### Requirement: Coexistence with other isolated measure kinds
A partitioned aggregate SHALL be usable in the same query as windowed, `first`/`last`, cross-model, and transform measures, with every measure retaining the value it has when queried alone.

#### Scenario: Partitioned plus windowed measure
- WHEN a query selects both `revenue:sum(partition_by=region)` and `revenue:sum(window='90d')`
- THEN each measure's executed values equal its value in a query where it appears alone

#### Scenario: Partitioned plus first/last, cross-model, and transform measures
- WHEN a query combines a partitioned aggregate with a `first`/`last` measure, a cross-model measure, or a transform measure
- THEN all measures are correct by executed values and the row count is unchanged

### Requirement: Attachment preserves cardinality structurally
Attaching a partitioned aggregate MUST never change the query's row count or any other column's values. The planner SHALL verify structurally that the attachment joins on the producer's complete unique key, and that a keyless attachment is provably single-row. The same verification SHALL apply to every nested attachment inside a producer — including attachments nested inside target-rooted (cross-model) producers — so no attach at any depth can multiply rows.

#### Scenario: Adding a partitioned measure is cardinality-neutral
- WHEN any supported query runs with and without an additional partitioned-aggregate measure
- THEN both runs return the same rows and identical values in all shared columns

#### Scenario: Empty partition set attaches the overall total
- WHEN a measure declares `partition_by=[]`
- THEN every row carries the overall total and the row count is unchanged

#### Scenario: Nested attachments inside a target-rooted producer are cardinality-checked
- WHEN a cross-model producer internally attaches a nested producer (e.g. a computed dimension it groups by)
- THEN the nested attach joins on the nested producer's complete unique key and the outer producer's row count is unchanged by it

### Requirement: Producers may require their own intermediate relations
A partitioned aggregate whose computation itself needs intermediate relations (rolling windows, rankings, transform steps) SHALL render correctly, including several such producers in one query, with no name collisions in the generated SQL.

#### Scenario: Two complex producers in one query
- WHEN a query uses two partitioned aggregates whose producers each need internal intermediate relations of the same shape
- THEN the generated SQL is valid on every supported dialect's emission path and executes with correct values

### Requirement: Existing partitioned-aggregate behavior is preserved
All partitioned-aggregate shapes supported before this change SHALL keep byte-identical generated SQL against the committed golden baselines, except divergences individually approved and recorded.

#### Scenario: Golden baselines hold
- WHEN the golden-SQL suites for previously supported partitioned-aggregate shapes run
- THEN every baseline matches byte-for-byte

### Requirement: Temporal transforms compose with partitioned measures
A partitioned measure SHALL be usable in the same query as `time_shift`, `change`, and `change_pct`, producing valid SQL on every supported dialect: the shifted re-aggregation groups only by real query dimensions, and reserved internal placeholder names never reach the emitted statement.

#### Scenario: Partitioned measure with a time-shift measure executes
- WHEN a query selects `amount:sum(partition_by=region)` and `time_shift(amount:sum, periods=-1)` over a month time dimension
- THEN the query executes with correct values for both measures and the emitted SQL contains no reserved placeholder prefix

#### Scenario: Shifted re-aggregation grain excludes the attached value
- WHEN the shifted comparison period is computed for such a query
- THEN it groups by the query's dimensions and shifted time bucket only, and joins back on exactly that grain

### Requirement: Grain-union broadcasting across consumption contexts
An arithmetic expression combining aggregates at different declared partition grains SHALL be well-defined at the union of those grains: each aggregate is computed at its own declared grain and broadcast over the grain keys it lacks. The expression SHALL be consumable at any grain refining the union — as a measure, the query grain — with every operand broadcast to the consuming rows; combining aggregates at different grains is never, by itself, an error. A transform over such an expression used as a measure SHALL evaluate at the query grain over the broadcast operands. Filter conjuncts referencing such expressions apply after attachment and MUST NOT alter surviving rows' values.

#### Scenario: Mixed-grain arithmetic as a measure
- WHEN a query over dimensions `[region, city]` selects the measure `a:sum(partition_by=region) - b:sum(partition_by=city)`
- THEN each row's value is its region total minus its city total, by executed values

#### Scenario: Same-grain partitioned arithmetic as a measure
- WHEN a query selects the measure `a:sum(partition_by=region) - b:sum(partition_by=region)` (the degenerate union of two identical grains)
- THEN each row's value is the difference of its two broadcast region totals, by executed values

#### Scenario: Plain and partitioned aggregates mix in one expression
- WHEN a query selects the measure `amount:sum - amount:sum(partition_by=region)`
- THEN each row's value is its query-grain total minus its broadcast region total, by executed values

#### Scenario: Transform over mixed-grain arithmetic as a measure
- WHEN a query selects the measure `rank(a:sum(partition_by=region) - b:sum(partition_by=city))`
- THEN result rows are ranked at the query grain by the broadcast difference, and adding the measure changes no other column's values

#### Scenario: Filter over mixed-grain arithmetic
- WHEN a query filters on `a:sum(partition_by=region) - b:sum(partition_by=city) > 0`
- THEN only qualifying rows remain and every surviving value equals the unfiltered query's value for that row

### Requirement: Bare windowed and ranked measures compose as full-grain partitioned aggregates
A windowed aggregation without `partition_by=` and a `first`/`last` aggregation without `partition_by=` SHALL behave as partitioned at the query's full projected grain: they compose with transforms, arithmetic/composite/scalar expressions, and filters exactly as partitioned aggregates do, while keeping their established public result keys, aliases, and executed values. Explicit `partition_by=` on the same aggregation remains a strict generalization; the bare form and an explicit form at the same effective grain are equivalent.

#### Scenario: Transform over a bare windowed measure
- WHEN a query selects `cumsum(amount:sum(window='90d'))` over a month time dimension
- THEN the running total of the rolling window executes with hand-computed values

#### Scenario: Bare windowed measure inside arithmetic
- WHEN a query selects `amount:sum(window='90d') / amount:sum`
- THEN the composite evaluates per result row over the attached rolling total and the plain aggregate, correct by executed values

#### Scenario: Filter-only reference to a bare windowed measure
- WHEN a query filters on `amount:sum(window='90d') > 20` without selecting that measure
- THEN qualifying rows survive with unchanged values and the emitted SQL contains no leaked internal names

#### Scenario: One predicate mixing a bare windowed and a plain aggregate
- WHEN a single filter predicate combines `amount:sum(window='90d')` with `amount:sum`
- THEN the whole predicate evaluates after attachment, correct by executed values

#### Scenario: Temporal transform over a bare first/last measure
- WHEN a query selects `time_shift(amount:last, -1)` over a month time dimension
- THEN each row carries the previous bucket's last value, correct by executed values

#### Scenario: Bare and explicit partition twins are equivalent
- WHEN one query selects a bare windowed (or `first`/`last`) measure and another declares the same aggregation with `partition_by=` naming the full projected grain
- THEN both return identical executed values and render one shared producer relation when combined in a single query

#### Scenario: Migrated families keep their executed values
- WHEN previously supported bare windowed and `first`/`last` queries run after the migration
- THEN executed values are identical to before; generated-SQL divergences are individually approved and recorded

### Requirement: Structurally identical producers render once
When several consumed aggregates resolve to the same producer — same source and root, same effective grain, same normalized aggregate set including per-measure filters and ranking/window kernel context, the same inherited row-filter context, and recursively identical nested producers (a producer whose sub-plan embeds other producers matches only one whose embedded producers are identical by this same rule) — the query SHALL compute that producer once and attach it at every consuming position: across both attach phases (dimension and measure roles) and across nesting scopes, including a producer consumed both at the top level and inside another producer's sub-plan. Producers that differ in any part of that specification — a different inherited filter, a different frame-bound rewrite, a different window duration, measure-level filter, ranking column, or a differing nested sub-plan producer — MUST stay separate. Sharing a producer MUST NOT change, duplicate, or drop any response warning: each warning surfaces once per semantic event regardless of how many scopes consume the producer.

#### Scenario: Same aggregate in both roles shares one producer
- WHEN the same partitioned aggregate appears inside a computed dimension and as a selected measure
- THEN the emitted SQL contains a single producer relation for it, with both roles' values correct

#### Scenario: Different producer inputs stay separate
- WHEN two aggregations differ in window duration, in a measure-level filter, or in an explicit ranking column
- THEN they render as separate producers and each value is correct

#### Scenario: A producer shared between a nested sub-plan and the top level renders once
- WHEN a query groups by a dimension banding `amount:sum(partition_by=city)` and selects `amount:sum(window='1y')`, so the city-total producer is needed by both the base's computed dimension and the windowed producer's sub-plan
- THEN the emitted SQL contains exactly one city-total producer relation, referenced from both scopes, with executed values unchanged

#### Scenario: Differing inherited filter context prevents merging
- WHEN two structurally identical aggregates are consumed in scopes that inherit different row-filter conjuncts into their producers
- THEN each scope keeps its own producer relation and each consumer's executed values are correct

#### Scenario: Consumers at different depths keep their own attach coordinates
- WHEN one shared producer is consumed at two nesting depths whose attach join keys differ in coordinates
- THEN each consumer joins the shared relation on its own keys and both executed values are correct

#### Scenario: A shared producer's warning surfaces once
- WHEN a producer that triggers a broadcast or dropped-filter warning is consumed from two scopes
- THEN the response carries that warning exactly once per semantic event

### Requirement: Measure-local filters stay inside the producer
An aggregation's own filter SHALL restrict only the rows aggregated by its producer, never the query's result rows; query- and model-level row filters SHALL apply consistently to both the query and the producer.

#### Scenario: A filtered measure is cardinality-neutral
- WHEN a windowed or `first`/`last` measure carrying its own filter is added beside unfiltered measures
- THEN the row count and every companion value are unchanged, and only the filtered measure's value reflects the filter

### Requirement: Partition keys are attributable from the aggregate's root
Under `to_many_handling: "broadcast"` and `"error"`, every explicit `partition_by=`
key SHALL be attributable from the aggregate's root — expressible over join hops that
are provably many-to-one. An unattributable partition key is a hard error in those
modes, naming the key, the failing hop, and the remedy; the producer MUST never join
through an unproven or fanning hop to express a declared grain. Under `"associate"`,
an explicit partition key not attributable from the root is legal: the aggregate
attributes at the declared grain by distinct-entity association (per
`queries/attribution-modes`), without warning.

#### Scenario: Joined partition key over a provably safe hop works
- **WHEN** a local aggregate declares `partition_by=` naming a dimension reached over a
  provably many-to-one join
- **THEN** the producer computes at that grain with correct executed values

#### Scenario: Partition key over an unproven hop errors
- **WHEN** an aggregate declares `partition_by=` naming a dimension reachable only
  across a join with unproven arity, under `"broadcast"` or `"error"` mode
- **THEN** the query fails with a clear error naming the key and the remedy, never
  silently double-counting inside the producer

#### Scenario: Unattributable partition key attributes under associate
- **WHEN** the same aggregate runs under `to_many_handling: "associate"`
- **THEN** it computes at the declared grain over distinct associated entities with
  correct executed values and no warning

### Requirement: Combined-consumer partition keys are query dimensions
Every explicit partition key of a partitioned aggregate consumed in a combined position — as a non-dimension measure, inside an arithmetic / scalar-call composite or transform used as a measure, as a raw ORDER BY target, or as a filter-only reference — SHALL be a query dimension or a time dimension's source column (rewritten to its truncated bucket), for local and cross-model aggregates alike. A violation SHALL fail at plan time with a clear error naming the offending key and the remedy, never with an internal join-back failure. A partitioned aggregate consumed only inside computed dimensions or as a re-aggregation operand keeps the finer-grain exemption (its partition set declares an internal producer grain; the outer aggregation is the combined consumer and carries the rule). A filter or ORDER BY reference to a computed dimension's own aggregate is a row-scope reference, legal at any partition grain: such a filter restricts the aggregated population per base row at the partition grain, and MAY therefore change surviving groups' aggregate values — unlike a combined-scope partitioned-aggregate filter, which only prunes result rows.

#### Scenario: Keyless dual-role measure fails cleanly, local and cross-model alike
- WHEN the same partitioned aggregate — local or cross-model — is consumed by a computed dimension and selected as a measure while a partition key is not among the query dimensions
- THEN the query fails at plan time with the same clear error in both variants, naming the key and the remedy (add it to dimensions/time_dimensions), never with an internal error

#### Scenario: Keyless raw ORDER BY target fails cleanly
- WHEN `order` names the raw partitioned aggregate alongside a computed dimension using it and a partition key is not a query dimension
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
operand grain, or a column that grain determines, is picked once per cell and
read by the outer aggregation, and its partition keys are exempt from the
combined-consumer partition-key rule exactly as the source's constituents are;
a population-row column against a coarser cell grain, a definition default
naming such a column, or an aggregate grained outside the operand grain is a
typed error naming the parameter and the remedy. The outer aggregation SHALL
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

### Requirement: Nested producers compose to arbitrary depth
Producers SHALL nest to arbitrary depth: a producer's sub-plan may carry nested
producers that themselves carry producers, at any grain admitted by the
consumption seam (equality included). Every attach at every depth joins on the
nested producer's complete grain; structurally identical producers render once
across all depths and scopes; the emitted statement carries one flat `WITH`;
each warning surfaces once per semantic event regardless of consuming depth.

#### Scenario: Depth-three re-aggregation
- **WHEN** a query over `[product]` selects
  `max(avg(sum(amount, partition_by=[city, region, product]), partition_by=[region, product]))`
- **THEN** it executes with hand-computed values — per product, the maximum over
  regions of the average over cities — with one flat `WITH` and closed scopes

#### Scenario: One producer consumed at two depths renders once
- **WHEN** a computed dimension bands `sum(amount, partition_by=[city, region])`
  and a measure selects `avg(sum(amount, partition_by=[city, region]))`
- **THEN** the emitted SQL contains a single city-region producer relation,
  consumed by both the row attach and the outer producer, with correct values

#### Scenario: A producer attached at two phases emits its nested producer first
- **WHEN** a producer carrying a nested producer (a re-aggregation over an attached
  operand) is attached both before aggregation (as a mixed constituent) and after it
  (standalone), so the base relation and the final select both read it
- **THEN** on every Tier-1 dialect the one flat `WITH` lists the nested producer before
  the producer that reads it, and that producer before the base relation — no relation
  references one declared later — and the statement executes on engines that reject
  forward references

### Requirement: Re-aggregation null, empty, and keyless cases are pinned
A NULL grain-key value SHALL form its own cell (null-safe grouping and
attachment). An outer cell whose operand cells all carry NULL values SHALL
yield NULL for value aggregations and 0 for `count`. A population cell with no
operand rows at all is governed by the population rules (no fabricated rows). A
keyless inner aggregate (`partition_by=[]`) forms a single-cell dataset; its
re-aggregation is the degenerate identity and follows the degenerate-warning
rule.

#### Scenario: Null grain component is one cell
- **WHEN** the inner grain contains a nullable column and rows with NULL exist
- **THEN** the NULL value forms exactly one inner cell, aggregated and attached
  null-safely, on every supported engine

#### Scenario: All-null operand values
- **WHEN** every inner cell of an outer cell has a NULL value
- **THEN** `avg`/`sum` yield NULL and `count` yields 0 for that cell, by
  executed values

#### Scenario: Keyless inner aggregate
- **WHEN** a query selects `avg(sum(amount, partition_by=[]))`
- **THEN** it executes as the global total (identity re-aggregation) with the
  degenerate-re-aggregation warning and no error

### Requirement: Partitioning by an attach-carrying computed dimension
An aggregate whose `partition_by=` names a computed dimension whose expression
itself contains an attached aggregate SHALL compile: the aggregate's producer
materializes the dimension's value (row-attaching the nested producer) and
groups by it. It SHALL be legal as a measure when the computed dimension is a
query dimension, and inside another computed dimension under the dimension
finer-grain exemption.

#### Scenario: Measure partitioned by a banded dimension
- **WHEN** a computed dimension `spend_band` bands
  `sum(amount, partition_by=[city, region])` and a measure selects
  `sum(revenue, partition_by=spend_band)` with `spend_band` a query dimension
- **THEN** the query executes with correct values and unchanged cardinality

#### Scenario: Nested inside another computed dimension
- **WHEN** a second computed dimension's expression contains an aggregate
  partitioned by `spend_band`
- **THEN** the query plans and executes without the nested-attach error

### Requirement: Cross-model ranked partitioned aggregates
A `first`/`last` aggregation over another model's column with an explicit `partition_by=`
SHALL compile and execute like its local twin: ranked inside a producer rooted at the
aggregate's own model at the declared partition grain, attached back without changing
cardinality. The shape SHALL be legal in every position — measure, filter, order target,
and computed-dimension expression — with identical values in each (position parity).

#### Scenario: Cross-model last with partition_by executes
- **WHEN** a query rooted at `orders` selects `customers.spend:last(partition_by=region)`
  alongside a plain measure
- **THEN** each row carries its region's last customer-spend value, correct by
  hand-computed executed values on SQLite and DuckDB, with row count and the sibling
  measure's values unchanged — never the former not-yet-supported error

#### Scenario: Cross-model ranked partitioned aggregate in filter and order positions
- **WHEN** the same aggregate is referenced only in a filter, and separately only as an
  ORDER BY target
- **THEN** the filter masks by the same per-region value the measure form returns and the
  order sorts by it, both by executed values

### Requirement: Cross-model partitioned aggregates nest inside transforms
A transform whose input contains a cross-model `partition_by=` aggregate SHALL compile:
the inner aggregate is computed in its own producer exactly as when consumed directly, and
the transform consumes the attached value like any local partitioned input. This includes
ranked inners (`first`/`last`) and holds in measure, filter, and order positions.

#### Scenario: Transform over a cross-model partitioned sum executes
- **WHEN** a query selects `cumsum(customers.spend:sum(partition_by=region))` over a month
  time dimension with `region` among the query dimensions
- **THEN** the cumulative series accumulates the per-region cross-model totals, correct by
  hand-computed executed values on SQLite and DuckDB — never the former
  not-yet-supported error

#### Scenario: Transform over a cross-model ranked partitioned aggregate executes
- **WHEN** a query selects `change(customers.spend:last(partition_by=region))` over a month
  time dimension with `region` among the query dimensions
- **THEN** each row carries the period-over-period difference of its region's last value,
  correct by executed values

#### Scenario: Nested producer plan shape is pinned
- **WHEN** two consumers (for example a measure and a filter) share one cross-model
  partitioned inner aggregate under transforms
- **THEN** the plan contains exactly one producer for that aggregate, attached in the
  combined phase on its complete partition grain, and the emitted SQL contains one producer
  relation for it — no duplicate producers and no incomplete attach key

### Requirement: Mixed sources carry the full expression-source surface
An aggregation source mixing row-level references with attached values SHALL
behave as a row-level expression source: everything legal for a plain
expression source is legal for it, and nothing more. Row leaves MAY be host-model
or joined-model columns, homed per `queries/semantics` › Home dataset of a
row-level aggregation source; an explicitly grained transform is an attached
constituent exactly like a partitioned aggregate, and so is a re-aggregation — an
aggregate over attached values, hand-written or produced by the `first`/`last`
collapse — evaluated at its own grain and broadcast per partition onto the source's
rows, an empty grain broadcasting its one value onto every row. The outer aggregation
SHALL support the plain scalar family, `count` (base rows with a non-null
operand value) and `count_distinct`, parametric and model-defined custom
aggregations — including multi-input built-ins and column-reference parameters
resolvable at row scope — and its own `partition_by=` and `window=`, each
resolved by the same rules as over a plain expression source. `first`/`last`
over the mixed expression SHALL keep the existing not-supported-over-an-
expression error. The shape SHALL be legal in measure, filter (typing as a
measure: pruning result rows without altering surviving values), and ORDER BY
positions, and as a computed dimension when grain-self-contained (the outer and
every inner aggregate explicitly grained). No producer or grouping step may
group by the row leaf or the attached value themselves — they feed the outer
aggregation only — and the mixed shape SHALL never be compiled through the
fully-attached carrier (whose cell-over-cell value differs). Row leaves keep
every existing expression-source restriction; attached constituents may be
cross-model, windowed, grained transforms, or themselves attached-input
aggregations. Discovery SHALL treat an aggregation that
owns attached inputs as opaque below its inputs (source and parameters): those
inputs belong to it — row-attached when it evaluates inline, owned by its own
producer when it is itself a producer answer — and are never discovered as
consumers of the enclosing level; its partition keys are not inputs and stay
visible to the enclosing level, so an attach-carrying computed dimension in
its `partition_by=` still gets the outer attach the grain join needs.

#### Scenario: Outer partition_by over a mixed source
- **WHEN** a query over `[region, product]` selects
  `sum(quantity * avg(unit_price, partition_by=product), partition_by=region)`
- **THEN** every row of a region carries that region's row-weighted total,
  broadcast exactly as any explicit-grain partitioned measure, by executed
  values

#### Scenario: Outer window over a mixed source
- **WHEN** a query with a month time dimension selects
  `sum(quantity * avg(unit_price, partition_by=product), window='90d')`
- **THEN** each bucket carries the trailing-90-day row-weighted total as of that
  bucket, by executed values

#### Scenario: Windowed constituent inside a mixed source
- **WHEN** a query with a month time dimension selects
  `sum(qty * sum(revenue, window='90d'))`
- **THEN** the windowed inner is exactly one nested producer at the bucket
  grain, row-attached into the outer aggregation's input relation — never an
  attach of the enclosing level — and each bucket carries the hand-computed
  row-weighted value, by executed values

#### Scenario: Transform constituent inside a mixed source
- **WHEN** a query over `[region]` selects
  `sum(quantity * rank(avg(unit_price, partition_by=product)))`
- **THEN** the transform is exactly one nested producer at its `(product)` grain,
  row-attached into the outer aggregation's input relation, and each region carries
  the hand-computed row-weighted value, by executed values — never the former
  nested-transform rejection

#### Scenario: Collapsing constituent mixed with a row leaf fails closed
- **WHEN** a query over a month time dimension selects
  `sum(amount * last(amount:sum(partition_by=[region, ordered_at])))`
- **THEN** it no longer fails with the collapsing-transform error — it executes per the
  next scenario; the former fail-closed pin is retired

#### Scenario: Collapsing transform constituent inside a mixed source
- **WHEN** a query over a month time dimension selects `sum(amount * last(X))` with
  `X = amount:sum(partition_by=[region, ordered_at])`
- **THEN** it executes with the hand-computed values Jan 375 / Feb 825 / Mar 900 on
  SQLite and DuckDB: `last(X)` collapses to one value per region, broadcast onto each
  row of that region, multiplied by the row's `amount` and summed per month — never
  the former fail-closed collapse error

#### Scenario: Hand-written re-aggregation constituent inside a mixed source
- **WHEN** a query over a month time dimension selects
  `sum(amount * min(X, partition_by=region))`
- **THEN** it executes with hand-computed values on SQLite and DuckDB, the plan carries
  exactly one producer for the re-aggregation at its `(region)` grain, row-attached on
  `region` and never attached at the enclosing level, the emitted statement has one
  flat `WITH`, scopes are closed, no placeholder leaks, and the query's row count is
  unchanged — never the internal grain-cover assertion

#### Scenario: Empty-grain re-aggregation constituent broadcasts one value
- **WHEN** a query over a month time dimension selects
  `sum(amount * last(amount:sum(partition_by=ordered_at)))`
- **THEN** the collapsed constituent has an empty grain — one value — attached with no
  join keys onto every row, and the query executes with hand-computed values on both
  engines with unchanged cardinality, never an empty join predicate

#### Scenario: Re-aggregation constituent as an attached parameter
- **WHEN** a query over a month time dimension selects
  `weighted_avg(amount, weight=min(X, partition_by=region))`
- **THEN** the parameter is attached at its `(region)` grain through the same path as a
  source constituent and the query executes with hand-computed values on both engines

#### Scenario: Mixed re-aggregation root combined with a coarser measure
- **WHEN** a query over `[region]` with a month time dimension selects
  `sum(amount * min(X, partition_by=region)) + amount:sum(partition_by=region)`
- **THEN** it executes with hand-computed values on both engines, the coarser term
  broadcast across months exactly as any explicit-grain partitioned measure, with
  unchanged cardinality

#### Scenario: Mixed collapse in filter and order positions
- **WHEN** `sum(amount * last(X))` appears only in a filter or only as an ORDER BY target
- **THEN** the filter types as a measure — pruning result rows with surviving values
  unchanged — and the order sorts by the same value the measure form returns

#### Scenario: The mode axis is not bypassed by an attached re-aggregation
- **WHEN** a query selecting `sum(amount * min(X, partition_by=region))` also groups by
  a dimension the source's home does not determine
- **THEN** under broadcast mode the value repeats across that dimension's cells with
  the self-announcing warning, and under error mode the query fails with the mode
  error — exactly as a mixed source with a plain attached aggregate

#### Scenario: Joined-model row leaf inside a mixed source
- **WHEN** a query rooted at `orders` over `[status]` selects
  `sum(customers.discount * avg(amount, partition_by=status))`
- **THEN** the source is homed at `orders`, the attached average is row-attached per
  order, and each status carries the hand-computed value, by executed values

#### Scenario: Parametric outer with a row-valued parameter
- **WHEN** a query selects
  `wavg(quantity * avg(unit_price, partition_by=product), weight=qty)` (a
  weighted average whose weight is a row column)
- **THEN** it executes with the hand-computed row-weighted average, never a
  column-parameter rejection

#### Scenario: Custom and multi-input aggregations over a mixed source
- **WHEN** a model-defined custom aggregation or a two-input built-in (e.g.
  `corr(quantity * avg(unit_price, partition_by=product), qty)`) consumes a
  mixed source
- **THEN** each executes with correct values through its own rendering path

#### Scenario: Count semantics over a mixed source
- **WHEN** a query selects `count(quantity * avg(unit_price, partition_by=product))`
- **THEN** each cell counts its base rows whose operand value is non-null

#### Scenario: Ranked aggregation over a mixed source keeps the expression error
- **WHEN** a query selects `first(quantity * avg(unit_price, partition_by=product))`
- **THEN** it fails with the existing error that `first` is not supported over
  an expression — never a mixing error and never wrong values

#### Scenario: Filter and order positions
- **WHEN** the mixed measure appears only in a filter
  (`sum(quantity * avg(unit_price, partition_by=product)) > 100`) or only as an
  ORDER BY target
- **THEN** the filter types as a measure — pruning result rows with surviving
  values unchanged — and the order sorts by the same value the measure form
  returns

#### Scenario: Grain-self-contained dimension position
- **WHEN** a query groups by a computed dimension banding
  `sum(quantity * avg(unit_price, partition_by=product), partition_by=region)`
- **THEN** rows group by the band with correct executed values and unchanged
  cardinality, and the plan carries one attach for the banded aggregation with
  the inner `avg` nested inside its producer — no separate top-level attach for
  the inner

#### Scenario: Adding a mixed measure is cardinality-neutral
- **WHEN** any supported query runs with and without an additional mixed-source
  measure
- **THEN** both runs return the same rows and identical values in all shared
  columns, and the emitted SQL leaks no internal placeholder names

#### Scenario: Never the fully-attached carrier
- **WHEN** the emitted SQL for a mixed-source measure is inspected
- **THEN** no relation groups by the attached value or the row leaf; the outer
  aggregation consumes base rows with the attached value joined on the
  constituent's complete grain

#### Scenario: A producer-bound mixed aggregation owns its inners
- **WHEN** a mixed aggregation is itself a producer answer (it carries its own
  `partition_by=`, `window=`, or is cross-model) and the plan is inspected
- **THEN** its inner constituents appear only inside that producer's plan,
  never as attaches of the enclosing level

#### Scenario: Partition keys stay visible through an opaque root
- **WHEN** a mixed aggregation declares `partition_by=` on an attach-carrying
  computed dimension of the query
- **THEN** that dimension's own attach is still planned at the enclosing level
  and the aggregation's producer joins back on it, with executed values equal
  to the same query spelled with a plain dimension

#### Scenario: One producer per distinct attached input
- **WHEN** the same attached aggregate appears twice — as a source constituent
  and as a parameter of one aggregation, or as an input of two different
  row-attach aggregations
- **THEN** the plan carries exactly one producer and one attach for it, every
  occurrence substitutes to that attach, total routing holds after
  substitution, and the executed values are correct

#### Scenario: A re-aggregation used both standalone and as a mixed constituent is deferred
- **WHEN** one query selects both `min(X, partition_by=region)` on its own and
  `sum(amount * min(X, partition_by=region))` — the same re-aggregation standalone
  (combined phase) and as a mixed row-level constituent (row phase) — over `[region]`
  and a month time dimension, in measure, filter, or order position
- **THEN** it is no longer deferred — never the former checker error — and executes on
  every engine with the values each use has alone: the
  standalone value is the region minimum broadcast onto every month cell (North 10,
  South 5, NULL where the region has no non-null cell) and the mixed value is the
  per-cell `amount` times that minimum, summed (North 100 / 200 / 300, South 25 / 75);
  the emitted statement carries exactly one producer relation for the re-aggregation
  and one for its nested operand, and adding either measure changes no other value

### Requirement: Attached parameters on row-level sources
An aggregation over a row-level source whose parameter — keyword or
positional; a definition default is Mode-A text and cannot carry an attached
value — is an attached value (an aggregate or a grained transform) SHALL
compile when the aggregation's operating grain determines the parameter. An
aggregate is determined by a grain iff that grain determines each of its
partition keys: a grain member, a column reached from a grain member over
provably to-one join hops, or an aggregate-valued key whose own grain is so
determined; an expression-valued partition key is determined only as an exact
grain member; a transform is determined iff its result grain is. The
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
(Target behaviour, deferred to DEV-1903 — bind refuses a transform argument
today; pinned by a strict xfail.)
- **WHEN** a query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=rank(sum(amount, partition_by=customers.regions.name)))`
  — the region cells ranked by their order-amount total, the NULL-name region
  forming its own ranked cell
- **THEN** the transform is the attached input at its result grain, and the query
  executes under the default mode by `status` (broadcast, warned) and under every
  mode by `customers.tier` with identical hand-computed values

#### Scenario: Transform parameter whose grain the home does not determine fails closed
(Target behaviour, deferred to DEV-1903 — bind refuses a transform argument
today; pinned by a strict xfail.)
- **WHEN** a query rooted at `orders` over a month time dimension on `ordered_at`
  selects
  `customers.spend:weighted_avg(weight=cumsum(sum(amount, partition_by=[customers.regions.name, ordered_at])))`
- **THEN** the query fails in every mode with the typed determination error
  naming the parameter — `customers` does not determine the order month in the
  transform's grain — never a multiplied or broadcast value

#### Scenario: Windowed aggregation with an attached parameter
(Target behaviour, deferred to DEV-1915 — the `window=` sum/avg allowlist refuses
it today; pinned by a strict xfail. The constituent form executes: see
`aggregations/expression-aggregation` › Windowed aggregation with an attached
constituent.)
- **WHEN** a query rooted at `orders` over a month time dimension on
  `customers.signup_at` selects
  `customers.spend:weighted_avg(window='1y', weight=sum(amount, partition_by=customers.regions.name))`
- **THEN** each signup-month bucket carries the trailing-window weighted average
  over the customers signed up in the window, each weighted by its region's
  total, identical under every mode with no warning (100, 125, 28080 / 267,
  33780 / 407 on the reference dataset; the orphan order's NULL bucket NULL)

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
