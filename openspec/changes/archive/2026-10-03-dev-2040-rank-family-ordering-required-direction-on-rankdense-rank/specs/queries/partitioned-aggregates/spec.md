## MODIFIED Requirements

### Requirement: Partitioned aggregates nested inside transforms
A transform SHALL accept a partitioned aggregate as its input when used as a measure — rank-family transforms and temporal transforms (`time_shift`, `change`, `change_pct`, `lag`, `lead`, `cumsum`, `consecutive_periods`) alike. The transform evaluates at its operand grain — the attached aggregate's `partition_by=`, else the query grain (Axiom 11.1) — and the measure consumer broadcasts the result onto the query grain (Axiom 11.4); it MUST never fail with an internal error.

#### Scenario: Running total of partition-grain values
- WHEN a query selects dimensions `[region, city, month(ordered_at)]` and the measure `cumsum(revenue:sum(partition_by=[region, ordered_at]))`
- THEN each row's value is the cumulative sum across months, within the row's non-time dimensions, of the attached region-month totals, verified by executed values

#### Scenario: Ranking result rows by an attached total
- WHEN a query selects the measure `rank(revenue:sum(partition_by=region), direction='desc')`
- THEN result rows are ranked by their attached region total at the query grain

#### Scenario: Change over a partitioned aggregate executes
- WHEN a query selects `change(amount:sum(partition_by=region))` or `change_pct(amount:sum(partition_by=region))` over a month time dimension
- THEN the query executes with the hand-computed bucket-over-previous-bucket difference (or ratio) of the attached value, instead of failing with an internal rendering error

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
- WHEN a query selects the measure `rank(a:sum(partition_by=region) - b:sum(partition_by=city), direction='desc')`
- THEN result rows are ranked at the query grain by the broadcast difference, and adding the measure changes no other column's values

#### Scenario: Filter over mixed-grain arithmetic
- WHEN a query filters on `a:sum(partition_by=region) - b:sum(partition_by=city) > 0`
- THEN only qualifying rows remain and every surviving value equals the unfiltered query's value for that row

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
  `sum(rank(amount:sum(window='90d', partition_by=region), direction='desc'))`
- **THEN** it no longer fails with the windowed time-dimension error — it executes per
  the next scenario; the former fail-closed pin is retired

#### Scenario: Windowed inner under a transform constituent executes
- **WHEN** a query over a month time dimension selects
  `sum(rank(amount:sum(window='90d', partition_by=region), direction='desc'))`
- **THEN** it executes with hand-computed values on SQLite and DuckDB — exactly one
  result row per bucket, every value non-NULL — the plan carries exactly one nested
  producer for the windowed inner grained by the query's active bucket, that exact
  bucket key is among the producer's projected grain and join keys, the emitted
  statement has one flat `WITH`, scopes are closed, and no placeholder leaks — never
  the former windowed time-dimension error

#### Scenario: A pure re-aggregation counts operand cells, not base rows
- **WHEN** a query over a month time dimension selects
  `sum(rank(amount:sum(window='90d', partition_by=region), direction='desc'))` over a source with several
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
  `sum(quantity * rank(avg(unit_price, partition_by=product), direction='desc'))`
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
  `customers.spend:weighted_avg(weight=rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
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
  `customers.spend:weighted_avg(rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
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
  `customers.spend:weighted_avg(weight=rank(sum(amount), direction='desc'))`
- **THEN** by `customers.tier` the inner is grained at `[customers.tier]` and the query
  executes in every mode with no warning (gold 61.25, silver 115, bronze 40, NULL tier
  NULL); by `status` the query fails in every mode with the typed determination error
  naming `weight`, since `customers` does not determine `status`

#### Scenario: Windowed inner joins the bucket to the parameter's grain
- **WHEN** a query rooted at `orders` over an `ordered_at` month time dimension selects
  `customers.spend:weighted_avg(weight=rank(sum(amount, window='1y', partition_by=customers.regions.name), direction='desc'))`
- **THEN** it fails in every mode with the typed determination error naming `weight` —
  the order-month bucket is in the transform's grain and `customers` does not
  determine it

#### Scenario: Windowed aggregation with a transform parameter
- **WHEN** a query rooted at `orders` over a `customers.signup_at` month time dimension
  selects
  `customers.spend:weighted_avg(window='1y', weight=rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
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
  `customers.spend:weighted_avg(weight=weighted_avg(amount, weight=rank(sum(amount, partition_by=customers.regions.name), direction='desc'), partition_by=customers.regions.name))`
- **THEN** each level resolves bottom-up — the innermost rank over the region cells,
  the middle weighted average per region (North 50 / 3, South 10, NULL 23.5), the
  outer over customers — and the query executes: 7556.67 / 103.5 broadcast to both
  `status` cells under the default mode with the warning, and by `customers.tier`
  under every mode gold 3316.67 / 53.33, silver 123.75, bronze 40, NULL tier NULL

#### Scenario: Cross-model transform parameter on a local root
- **WHEN** a query rooted at `orders` selects
  `amount:weighted_avg(weight=rank(sum(customers.spend, partition_by=customers.regions.name), direction='desc'))`
- **THEN** the parameter's producer is rooted at `customers` grouped by region (spend
  North 280, South 195, NULL 40 → ranks 1, 2, 3) and attached per order row, the
  orphan order taking the NULL cell; the query executes in every mode with no warning —
  281 / 16 globally, `ok` 116 / 11 and `new` 33 by `status`

#### Scenario: Transform parameter with its own partition_by outside the query dimensions
- **WHEN** a query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=rank(sum(amount, partition_by=[customers.regions.name, customers.tier]), partition_by=customers.regions.name, direction='desc'))`
  with no dimensions, and by `customers.tier`
- **THEN** the transform's own partition key is exempt from the combined-consumer
  partition-key rule exactly as a source constituent's is, and the query executes in
  every mode with no warning: 760 / 11 globally; gold 61.25, silver 115, bronze 40 by
  tier

### Requirement: Transform partition keys bind like aggregate partition keys
A rank-family transform's own `partition_by=` SHALL bind exactly as an aggregation's
`partition_by=` does: each element — singly or in a list — is a column reference, a
dotted joined-model reference, or the name of a computed dimension declared in the
query, which resolves to that dimension's value; the same binding applies in the
measure, aggregation-parameter, filter, order-target and computed-dimension positions.
A name that is no declared computed dimension and no column SHALL fail as an unknown
reference; an element that is neither a column reference nor a computed-dimension name
SHALL fail with one error naming the construct — `aggregation` or `transform '<op>'` —
and the offending kind. An attach-carrying computed dimension (one whose expression
contains an aggregate) binds the same way; its filter and order positions execute, while
its measure and aggregation-parameter positions fail closed with a planner error, never
an unknown-reference error (target behaviour, DEV-1960: they execute like the
aggregation twin).

Values below are on the sales graph with `ureg` declared as the computed dimension
`upper(region)` and `spend_band` banding `sum(amount, partition_by=[city, region]) > 45`.

#### Scenario: Measure position
- **WHEN** a query over `[ureg, city]` selects `rank(sum(amount), partition_by=ureg, direction='desc')`
- **THEN** each city is ranked by its total within its upper-cased region — EAST: Zeta 1, Delta 2, Epsilon 2; NORTH: Beta 1, Alpha 2; SOUTH: Gamma 1, Alpha 2; GAP: the NULL city 1, Kappa 2; VOID: Xi NULL (its total is NULL) — and the bound partition grain equals that of `sum(amount, partition_by=ureg)`

#### Scenario: Aggregation-parameter position
- **WHEN** a query over `[ureg]` selects `weighted_avg(amount, weight=rank(sum(amount, partition_by=[ureg, city]), partition_by=ureg, direction='desc'))`
- **THEN** each region's rows are weighted by their city's rank within the region: EAST 56, NORTH 120/7, SOUTH 36, GAP 7, VOID NULL

#### Scenario: Filter position
- **WHEN** a query over `[ureg, city]` filters `rank(sum(amount), partition_by=ureg, direction='desc') <= 1`
- **THEN** exactly the rank-1 rows survive: EAST/Zeta, NORTH/Beta, SOUTH/Gamma, GAP/NULL (VOID/Xi's NULL rank fails the predicate)

#### Scenario: Order position
- **WHEN** a query over `[ureg, city]` orders by `rank(sum(amount), partition_by=ureg, direction='desc')` ascending
- **THEN** the four rank-1 rows precede every rank-2 row

#### Scenario: Computed-dimension position with a member key
- **WHEN** a query declares `ureg` and a second computed dimension `rank(sum(amount, partition_by=[city, ureg]), partition_by=ureg, direction='desc')` named `r`, selecting `sum(amount)`
- **THEN** rows group by `(ureg, r)`: EAST r=1 80 and r=2 100, NORTH r=1 60 and r=2 30, SOUTH r=1 100 and r=2 40, GAP r=1 12 and r=2 8, VOID r=NULL NULL

#### Scenario: Mixed list of a column and a computed dimension
- **WHEN** a measure names `rank(sum(amount), partition_by=[ureg, product], direction='desc')`
- **THEN** the bound partition grain is `{upper(region), product}`, identical to the grain `sum(amount, partition_by=[ureg, product])` binds

#### Scenario: Non-column element names the construct
- **WHEN** a measure names `rank(sum(amount), partition_by=sum(amount), direction='desc')` or `sum(amount, partition_by=sum(amount))`
- **THEN** binding fails with "transform 'rank' partition_by must resolve to a column reference; got AggregateKey." or "aggregation partition_by must resolve to a column reference; got AggregateKey." respectively

#### Scenario: Undeclared name stays unknown
- **WHEN** a query over `[city]` (no `ureg` dimension) selects `rank(sum(amount), partition_by=ureg, direction='desc')`
- **THEN** binding fails with the unknown-reference error naming `ureg`

#### Scenario: Attach-carrying computed dimension in filter and order positions
- **WHEN** a query over `[spend_band, city]` filters `rank(sum(amount), partition_by=spend_band, direction='desc') <= 1`
- **THEN** exactly the rows hi/Gamma (100) and lo/Alpha (70) survive, and the same expression as an ascending order target sorts those two rows first

#### Scenario: Attach-carrying computed dimension in measure and parameter positions fails closed
- **WHEN** a query over `[spend_band, city]` selects `rank(sum(amount), partition_by=spend_band, direction='desc')`, or a query over `[spend_band]` selects `weighted_avg(amount, weight=rank(sum(amount), partition_by=spend_band, direction='desc'))`
- **THEN** the key binds to the dimension's value and planning fails with a planner error, never an unknown-reference error (target behaviour, DEV-1960: both execute like `sum(amount, partition_by=spend_band)`)
