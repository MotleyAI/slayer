## MODIFIED Requirements

### Requirement: Second-order aggregation over attached values
An aggregation whose source operand resolves entirely to attached values —
partitioned aggregates, or explicitly grained transforms over them, directly or
combined through arithmetic and scalar functions — SHALL aggregate over the
operand dataset's cells, never over the query's population rows. The operand
dataset is typed by the union of its constituents' grains: an aggregate
constituent at its declared `partition_by=` grain; a transform constituent at the
union of its inner aggregates' grains — each inner's explicit `partition_by=`, else
the query's dimensions and time buckets — where a windowed inner's grain always
includes the query's time bucket whether or not its `partition_by=` names it, and an
axis-collapsing transform (`first`, `last`) at that union minus its time axis; a
constituent with no declared grain is typed at the query's dimensions. Its rows are the
row supply of its constituents at the union grain (`queries/population` › Rows of an
inferred query are the row supply), whatever the query's population: the NULL-safe union of
the cells of every constituent at the full union grain — the tuples its row-filtered home
rows reach, a measure-local `FILTER (WHERE …)` narrowing only its value — and, when no
constituent is at the full union grain, the cells of the union grain's dimension anchor
agreeing with the coarser constituents' cells, a query needing an undefined anchor failing
closed with the population-inference error. The population's rows never add or remove an
operand cell. Each constituent's value attaches null-safely at its own grain, coarser
constituents broadcast onto the finer cells, and a cell a constituent lacks contributes
its empty value (NULL for a transform). The
outer aggregation partitions those cells by the query dimensions attributable to
the operand dataset per Axiom 1 (Determination): a dimension is attributable iff
the grain determines it — a grain member, or a field reached from a grain member
over provably to-one join hops (a foreign-key grain field thus determines its
referenced model's fields and any column further along a to-one chain; an
entity-key grain field additionally determines all of its own model's columns).
A grain field does not determine its own model's other columns when the grain
does not fix that model's key — a foreign key does not identify the many-side
row — and an expression grain field (time bucket, computed dimension) determines
only itself — as a grain member it pins nothing further. Determination is closed
under row-level combination (Axiom 2.2): a dimension combining determined operands
through arithmetic, comparison, scalar functions or conditionals — a literal being
determined by every grain, a time bucket when the grain determines its column, and an
embedded aggregate when the grain determines its `partition_by=` members — is itself
attributable, whatever its spelling.
A derived column is determined when the grain determines every column its definition
reads (value and filter), recursively — exactly as its inline expression would be.
Unattributable dimensions resolve per `to_many_handling` exactly as for
model-rooted aggregates: per-cell association when the mode is omitted or
`"associate"`, broadcast with a self-announcing warning naming the dimension and the
remedy under `"broadcast"`, or a clear error under `"error"`. Adding a
re-aggregated measure MUST NOT change the result row count or any other
column's values.

#### Scenario: Average of city totals per region
- **WHEN** a query over dimensions `[region]` selects the measure
  `avg(sum(amount, partition_by=[city, region]))`
- **THEN** each region row carries the unweighted average of that region's city
  totals, by executed values, distinguishable from the row-count-weighted value

#### Scenario: Grained transform constituent aggregates the transform's cells
- **WHEN** a query over a month time dimension selects
  `sum(cumsum(sum(amount, partition_by=[region, ordered_at])) - 1)`
- **THEN** each month carries the sum over regions of that region's running total
  minus one per cell, by hand-computed executed values on SQLite and DuckDB

#### Scenario: Ungrained transform constituent is identity plus a warning
- **WHEN** a query selects `sum(cumsum(sum(amount)))` over a month time dimension
- **THEN** the value equals `cumsum(sum(amount))` per cell and the response carries
  the degenerate-re-aggregation warning, exactly as `sum(sum(amount))` does

#### Scenario: Ungrained inner of a mixed operand types at the query grain
- **WHEN** a query over a month time dimension selects
  `sum(rank(sum(amount, partition_by=[region, ordered_at]) - sum(amount), direction='desc'))`
- **THEN** the ungrained inner is the month total, computed at the query grain and
  broadcast onto the `(region, month)` cells before ranking — never re-evaluated per
  region — by hand-computed executed values distinguishable from the per-cell
  evaluation

#### Scenario: Collapsing transform constituent drops the time axis
- **WHEN** a query over a month time dimension under `to_many_handling: "broadcast"`
  selects `sum(last(sum(amount, partition_by=[region, ordered_at])))`
- **THEN** the constituent is typed at `(region)`: every month carries the sum over
  regions of each region's most recent monthly total, the response warns that the
  month dimension is broadcast, and a region absent from a month still counts —
  distinguishable from summing the `(region, month)` cells present in that month

#### Scenario: Collapsing and preserving constituents share one operand dataset
- **WHEN** the same query selects
  `sum(cumsum(sum(amount, partition_by=[region, ordered_at])) - last(sum(amount, partition_by=[region, ordered_at])))`
- **THEN** the operand dataset is the `(region, month)` cells, the collapsed value
  broadcasts onto them, each month is attributable through the preserving
  constituent, and the value is correct with no warning

#### Scenario: Composite operand keeps the population's cells
- **WHEN** the operand combines aggregates at `[city, region]` and `[region]`
  grains and some union-grain cell has no value for one constituent (e.g. a
  measure-local filter eliminates its rows)
- **THEN** the operand dataset has exactly the `(city, region)` cells the finer
  constituent's home rows reach, the measure-local filter narrowing only its value,
  the region-grain value broadcast onto them, and the missing value contributes its
  empty value to that cell without removing it

#### Scenario: Outer dimension attributed through a to-one chain
- **WHEN** the inner grain is an entity key (e.g. `customer_id`) and a query
  dimension is reached from it over a provably to-one join chain
- **THEN** the outer aggregation partitions the inner cells exactly by that
  dimension, with no broadcast warning

#### Scenario: Outer dimension seeded by a nested-path entity key
- **WHEN** the inner grain contains a joined model's unique key
  (e.g. `sum(amount, partition_by=customers.id)` rooted at `orders`) and a query
  dimension is a column of that model or reached from it over a to-one hop
  (`customers.region_id`, `customers.regions.name`)
- **THEN** the outer aggregation partitions the cells exactly by that dimension with
  no broadcast warning, by executed values

#### Scenario: A foreign-key grain field determines its to-one target
- **WHEN** the inner grain contains a foreign-key column
  (e.g. `sum(amount, partition_by=customers.region_id)` rooted at `orders`) and a
  query dimension is a field of the model that key points at over the provably
  to-one hop (`customers.regions.name`)
- **THEN** the outer aggregation partitions the cells exactly by that dimension with
  no broadcast warning, by executed values (Axiom 1: the fixed key value pins the
  to-one target row)

#### Scenario: A non-key grain field does not determine its own model's siblings
- **WHEN** the inner grain contains a joined model's foreign-key column
  (e.g. `partition_by=customers.region_id`, which does not identify a customer) and
  a query dimension is another column of that same model reached only by
  identifying its row (`customers.id`)
- **THEN** the dimension is not attributable and resolves per `to_many_handling`

#### Scenario: Unattributable outer dimension broadcasts with a warning
- **WHEN** a query over dimensions `[region]` selects
  `avg(sum(amount, partition_by=city))` under `to_many_handling: "broadcast"`
- **THEN** every region row carries the global average of city totals and the
  response warns, naming `region`, the reason it is not attributable, and the
  remedy (add it to the inner `partition_by=`)

#### Scenario: Unattributable outer dimension associates on request
- **WHEN** the same query runs omitting the mode, and again under
  `to_many_handling: "associate"`
- **THEN** in both runs each region cell aggregates the distinct city cells associated with
  it through its constituent's home rows — a city value co-occurring with two
  regions there counts in both — by executed values

#### Scenario: Unattributable outer dimension refuses under error mode
- **WHEN** the same query runs under `to_many_handling: "error"`
- **THEN** it fails with a clear error naming the measure, the dimension, and
  the remedy — never wrong numbers

#### Scenario: Degenerate re-aggregation is identity plus a warning
- **WHEN** a query selects `avg(sum(amount))` (operand grain equals the outer
  grain)
- **THEN** the value equals `sum(amount)` per cell and the response carries a
  degenerate-re-aggregation warning naming both grains and the
  `partition_by=` remedy

#### Scenario: Row-level expression over grain members partitions exactly
- **WHEN** a query over dimensions `[region, city = 'Alpha']` selects
  `avg(sum(amount, partition_by=[city, region]))` under each `to_many_handling` mode
- **THEN** each `(region, city = 'Alpha')` row carries the average of exactly its own
  city cells — `(North, true)` 30, `(North, false)` 60, `(South, true)` 40,
  `(South, false)` 100, `(East, false)` 60, Gap's NULL-city cell 12, `(Gap, false)` 8,
  `(Void, false)` NULL — by executed values on SQLite and DuckDB, with no broadcast or
  association warning and no error

#### Scenario: Expression over a to-one-determined dimension matches its plain spelling
- **WHEN** the operand is `sum(amount, partition_by=customer_id)` rooted at `corders` and
  the query dimension is `customers.regions.name = 'North'`
- **THEN** the `true` cell carries 35 and the `false` cell 100 — the values the plain
  `customers.regions.name` dimension gives North and South — by executed values, with no
  warning

#### Scenario: Aggregate-carrying expression dimension grained by a determined key
- **WHEN** the same operand is grouped by the dimension
  `sum(amount, partition_by=customers.regions.name) > 80`
- **THEN** the `false` cell carries 35 and the `true` cell 100, by executed values, with no
  warning — identical to grouping by the bare aggregate dimension

#### Scenario: Expression over an undetermined column still resolves per mode
- **WHEN** a query over dimensions `[region, is_p]`, with `is_p` defined as
  `product = 'P'`, selects `avg(sum(amount, partition_by=[city, region]))`
- **THEN** under `to_many_handling: "broadcast"` each region's value repeats across
  `is_p` and the broadcast warning names `is_p`, the reason that the operand grain does
  not determine it, and the `partition_by=` remedy; omitting the mode, each cell
  aggregates the city cells associated with it and the associated-cells warning names
  `is_p`; under `to_many_handling: "error"` the query fails naming `is_p`

#### Scenario: Derived column over grain members partitions like its inline spelling
- **WHEN** the model declares a derived column `city_upper` defined as `UPPER(city)` and a
  query over dimensions `[region, city_upper]` selects
  `avg(sum(amount, partition_by=[city, region]))`
- **THEN** each row carries the average of exactly its own city cells, identical to the
  query over `[region, upper(city)]`, by executed values on SQLite and DuckDB, with no warning

#### Scenario: The operand's cells never depend on the population
- **WHEN** over the seed of `queries/population` › Rows of an inferred query are the row supply, a query
  grouped by `customers.region` measures
  `count(count(returns.*, partition_by=[customers.region, customers.segment]))`, with
  `population: "orders"`, with `population: "customers"`, without a population, and without a
  population beside `orders.total_revenue`
- **THEN** each region counts the `(region, segment)` cells some return reaches: EAST 1 and WEST 0
  under `orders`; EAST 1, NORTH 1 and WEST 0 under `customers`; EAST 1 and NORTH 1 inferred alone;
  EAST 1, NORTH 1 and WEST 0 beside `orders.total_revenue` — never a population cell the
  constituent's home rows do not reach

#### Scenario: Constituents with no full-grain member take the union grain's anchor
- **WHEN** a query grouped by `region` measures
  `count(sum(a, partition_by=[city]) * sum(b, partition_by=[region]))`, where Paris and Lyon lie in
  EU and Tokyo in Asia
- **THEN** the operand cells are the `(city, region)` cells of the dimension anchor of `city` and
  `region`, EU counting 2 and Asia 1 — never every city paired with every region

### Requirement: Grain-union broadcasting
When aggregates at different grains combine in one expression, the expression's grain
SHALL be the union of its operands' grains, each operand broadcast from its own grain
to that union; combining at different grains is never, by itself, an error. Broadcast
SHALL only go from coarser to finer; consuming an aggregate at a strictly coarser
grain SHALL fail with a typed error, never collapse implicitly. At query level the population
supplies the rows — for a query omitting `population`, its row supply —; inside a second-order
aggregation's source the operand dataset supplies them (› Second-order aggregation over attached
values); in both, a cell an operand lacks contributes the operand's empty value.

#### Scenario: Different-grain arithmetic broadcasts to the union
- **WHEN** a measure divides a query-grain aggregate by a
  `partition_by=<coarser subset>` aggregate
- **THEN** by executed values the coarser operand repeats across the dimensions it
  lacks and each cell holds the ratio at the union grain

#### Scenario: Coarser consumption is a typed error
- **WHEN** a query shape would need an aggregate consumed at a strictly coarser grain
  than its own
- **THEN** the query fails with a clear typed error rather than implicitly collapsing
  or duplicating rows

### Requirement: Home dataset of a row-level aggregation source
An aggregation over a row-level source SHALL run over the rows of exactly one home dataset
(Axiom 2): the source's anchor (› An aggregation binds from its own anchor) when every
dependency of every input — each source leaf, each column-valued parameter and each
non-overridden definition default, each taken through its dependency closure, and each grain
member of every attached constituent (an aggregate or grained transform operand is opaque
and stands for its grain: its explicit `partition_by=`, else the query's dimensions; a
transform's grain is the union of its inner aggregates', where a windowed inner's grain
always includes the query's time bucket whether or not its `partition_by=` names it) — is
reachable from it over provably to-one hops, else the deepest candidate from which every
such dependency is reachable over provably to-one hops. A non-overridden definition default
SHALL be resolved as a reference from the owning model — the source anchor that declares the
aggregation — with each column reference (bare, or one inside an expression default) taken
in the owner's coordinates. A qualifier naming a dataset already on the source's path from
its reference anchor (`queries/references`) — that anchor itself included — SHALL cancel the
path back to that dataset, so the reference reads that dataset's row on the path, keeping
the path's own spelling of it, never a second join to it; a qualifier the owner cannot reach
forward otherwise SHALL instead be read as a query reference anchored at its first segment,
while an ambiguous or only partially resolvable owner reference — a first segment that
resolves, by cancellation or as a hop, followed by one that does not — SHALL fail closed
rather than silently re-anchor. An edge-name segment never cancels. Only a definition
default cancels: a query-typed path that revisits a dataset stays refused with the
circular-join error, and so does a default reference that is circular along the source's
path. (Whether a derived column whose own `DatasetField.sql` revisits is likewise refused is
out of scope here — DEV-1952.) A default is resolved once, when the query is bound, and is
thereafter the same value as its explicit spelling — the column it resolves to, or for an
expression default each of its references: it homes, is checked for input safety and renders
exactly as that spelling would, in every producer kind — plain, association, windowed and
second-order — and in every position, filters and order keys included; a default and its
identical explicit spelling in one query are computed once. So a default read as a query
reference — anchored at its first segment, even when that anchor is the query's population —
SHALL widen the home exactly as spelling that reference explicitly would, and a default
whose resolved path reaches a dataset over a fanning hop SHALL widen the home to that
dataset, exactly as spelling that column explicitly would; a definition default whose
definition crosses a fanning hop SHALL fail closed even when other inputs widen the home
away from the declaring model; and a cancelled default, or one read as a query reference, is
never rendered as a join from the owner back to the dataset it names. Candidates are the
source's anchor, the longest common prefix of the source leaves' own paths, tried first,
then the input paths — each from its own reference anchor, never a route from the query's
population — and their longest common prefix, deepest first. The aggregation is computed
over the home's rows, each counted once, never over a join product. When no candidate
determines every input the query SHALL fail with the input-safety error naming the offending
leaf and hop. The home SHALL depend on the leaves' paths alone, never on the spelling of the
expression. Every other rule — attribution and `to_many_handling` modes, explicit grain,
`window=`, filter routing, positions — applies exactly as for a single-column source rooted
at the home.

#### Scenario: Deepest determining dataset wins
- **WHEN** a query rooted at `orders` selects `sum(customers.spend - customers.regions.pop)`
  over provably to-one hops `orders → customers → regions`
- **THEN** the home is `customers`: each customer's spend and its region's population
  are counted once, however many orders the customer has, by executed values

#### Scenario: Host-side leaf pulls the home to the root
- **WHEN** a query rooted at `orders` selects `sum(amount - customers.discount)`
- **THEN** the home is `orders`: each order row carries its own customer's discount,
  by executed values

#### Scenario: Branches meet at their common ancestor
- **WHEN** a query rooted at `orders` selects `sum(customers.spend - stores.rent)`,
  both hops provably to-one
- **THEN** the home is `orders`, the aggregation runs over the order rows, and no
  warning is raised, by executed values

#### Scenario: A parameter widens the home
- **WHEN** a query rooted at `orders` selects
  `wsum(customers.spend + customers.regions.pop, weight=amount)`
- **THEN** the home is `orders` — the weight's dataset — and each order is weighted by
  its own amount, identical to the rule for a single-column source with the same
  parameter

#### Scenario: A definition default naming a root column widens the home to the root
- **WHEN** a query rooted at `orders` selects `<agg>(customers.spend)`, where `<agg>` is
  declared on `customers` and defaults its weight to the root column `orders.amount`
  (bare-qualified or inside an expression such as `orders.amount * 1`)
- **THEN** the home is `orders`, identical in value to the explicit
  `weighted_avg(customers.spend, weight=orders.amount)` — the genuine root-local
  default is retained as a home candidate rather than dropped

#### Scenario: A bare definition default stays owner-local
- **WHEN** a query rooted at `orders` selects a `customers`-declared aggregation over
  `customers.spend` whose weight defaults to the bare identifier `spend`
- **THEN** the default resolves to the owner's `customers.spend`, never a bogus
  root-local `()`, and the home is exactly the home of the source alone

#### Scenario: An owner-reachable dotted default resolves in the owner's frame
- **WHEN** a `customers`-declared aggregation defaults its weight to `regions.pop`,
  a model reachable forward from `customers`
- **THEN** the default resolves to the owner-relative `customers.regions.pop`, not to a
  root-anchored `regions`, and homes exactly as spelling `customers.regions.pop`
  explicitly would

#### Scenario: A fanning definition default fails closed even when the home widens
- **WHEN** a `regions`-declared aggregation over `customers.regions.pop` has one default
  that widens the home to `customers` and another default whose definition crosses the
  fanning `regions → region_events` hop
- **THEN** the query fails closed with the input-safety error naming the fanning hop —
  the fanning default is resolved on the declaring `regions` model and never omitted
  from safety because the home widened to `customers`

#### Scenario: A default cancels to a dataset two hops from the root
- **WHEN** a query rooted at `orders` selects `wsum_region_pop(customers.regions.countries.gdp)`,
  where `wsum_region_pop` is declared on `countries` and defaults its weight to
  `regions.pop`, `regions → countries` is provably to-one and `orders` has no direct
  join to `regions`
- **THEN** the default resolves to `customers.regions.pop` — the region the path came
  through, never a second `countries → regions` join — the home is `customers.regions`,
  and the value equals the explicit `weight=customers.regions.pop` twin: each region's
  gdp times its population once (500000 on the reference dataset), never the
  per-customer multiple (1500000), on SQLite and DuckDB

#### Scenario: A default cancels twice
- **WHEN** the `countries`-declared default is `regions.customers.spend` and the query
  selects `wsum_cust_spend2(customers.regions.countries.gdp)` from `orders`
- **THEN** the default resolves to `customers.spend`, the home is `customers`, and the
  value equals the explicit `weight=customers.spend` twin (670000 on the reference
  dataset), by executed values

#### Scenario: A default cancels and then walks forward
- **WHEN** the `countries`-declared default is `regions.customers.plans.fee` (queried as
  `wsum_plan_fee(customers.regions.countries.gdp)`), or a `regions`-declared default is
  `customers.plans.fee` (queried as `wsum_cust_plan_fee(customers.regions.pop)`)
- **THEN** each resolves to `customers.plans.fee`, homes at `customers`, and equals its
  explicit twin (100000 and 10000 on the reference dataset), by executed values

#### Scenario: Defaults in different frames resolve per reference
- **WHEN** a `countries`-declared aggregation defaults one parameter to `regions.pop`
  and another to `regions.customers.plans.fee`, queried as
  `wsum_two(customers.regions.countries.gdp)` from `orders`
- **THEN** each default resolves in its own frame (`customers.regions.pop` and
  `customers.plans.fee`), the home is `customers`, and the value equals the explicit
  two-kwarg twin (17000000 on the reference dataset), by executed values

#### Scenario: A cancelled default homes above it over a to-one reverse hop
- **WHEN** a query rooted at `orders` selects `wsum_rp(customers.regions.region_events.value)`,
  where `wsum_rp` is declared on `region_events` and defaults its weight to `regions.pop`,
  `regions → region_events` fans and its inverse is provably to-one
- **THEN** the default resolves to `customers.regions.pop`, the home is
  `customers.regions.region_events` (it determines `regions.pop` back over the to-one
  hop), and the value counts each event once (16000 on the reference dataset, never
  the 48000 of a per-customer fan), identical to the explicit
  `weight=customers.regions.pop` twin and to
  `sum(customers.regions.region_events.value * customers.regions.pop)`

#### Scenario: A cancelled default that then crosses a fanning hop fails closed
- **WHEN** the `countries`-declared default is `regions.region_events.value` and the
  query selects `wsum_fan(customers.regions.countries.gdp)` from `orders`
- **THEN** the default resolves to `customers.regions.region_events.value`, the home is
  `customers.regions.region_events` exactly as for the explicit
  `weight=customers.regions.region_events.value` twin, and the value equals that twin
  and `sum(customers.regions.countries.gdp * customers.regions.region_events.value)` —
  each event counted once — on SQLite and DuckDB, by executed values

#### Scenario: A reverse hop to a dataset not on the path stays refused
- **WHEN** a query rooted at `regions` selects `wsum_cust_spend(pop)`, whose
  `regions`-declared default is `customers.spend`
- **THEN** nothing cancels — `customers` is not on the path — and, `customers → regions`
  being to-one, the default homes on `customers`: the value equals the explicit
  `weight=customers.spend` twin and `sum(regions.pop * spend)` rooted at `customers`,
  by executed values

#### Scenario: A query-typed revisit is refused
- **WHEN** a query selects `sum(customers.regions.customers.spend)` from `orders`
- **THEN** the query is refused with the circular-join error, never silently cancelled

#### Scenario: The path's own spelling survives cancellation
- **WHEN** the query root is an inline extension of `orders` adding a named join
  `ship_region` to `regions`, and the query selects
  `wsum_region_pop(ship_region.countries.gdp)`
- **THEN** the default resolves to `ship_region.pop` — the edge-name spelling of the
  region on the path — and the value equals the explicit `weight=ship_region.pop` twin
  (500000 on the reference dataset)

#### Scenario: A cancelled expression default renders canonically in every producer kind
- **WHEN** the cancelled default is an expression (`regions.pop * 1`,
  `regions.customers.spend * 1`) and the aggregation runs as an association producer
  (`to_many_handling: associate` by the unattributable `status`), as a trailing-window
  producer (`window='1y'` over `customers.signup_at` months, home `customers`), or as
  the outer aggregation of a second-order producer (a host-declared default
  `customers.regions.pop * 1` over `sum(amount, partition_by=customers.regions.id)`)
- **THEN** each executes with values identical to its explicit-kwarg twin — the
  default is entered at the producer root in canonical coordinates, never as a reverse
  join from the owner — by executed values

#### Scenario: An owner at the root consumes its own name once
- **WHEN** a query rooted at `orders` selects `<agg>(amount)` where `<agg>` is declared
  on `orders` and defaults its weight to `orders.cost` (bare-dotted or inside
  `orders.cost * 1`)
- **THEN** the leading owner name is consumed as a self-reference exactly once, the
  default is root-local, and the value equals the explicit `weight=cost` twin

#### Scenario: A second-order default naming a non-join qualifier fails closed at typing
- **WHEN** a host-declared aggregation whose default is `nowhere.col` is used as the
  outer aggregation over `sum(amount, partition_by=status)`
- **THEN** the query fails with the unresolvable-join error naming the default, never
  a bogus key that fails later or slips through

#### Scenario: A stage query with no model host is unaffected
- **WHEN** a multi-stage query aggregates a stage's output columns in a later stage
- **THEN** it executes exactly as before — a stage has no model host, so no definition
  default is resolved for it

#### Scenario: An attached constituent's grain widens the home
- **WHEN** a query rooted at `orders` selects
  `sum(customers.discount * avg(amount, partition_by=status))`
- **THEN** the home is `orders` — the grain member `status` must be determined by the
  home — and each order row carries its customer's discount times its status's
  average amount, by executed values; with `partition_by=customers.tier` instead the
  home stays `customers`

#### Scenario: Spelling never moves the home
- **WHEN** one query selects `sum(customers.spend)` and another `sum(customers.spend + 0)`
- **THEN** both resolve the same home and return identical executed values

#### Scenario: No home fails closed
- **WHEN** a source leaf is reachable from every candidate home only across a fanning
  or unproven join hop
- **THEN** the query fails with the input-safety error naming the leaf and the hop,
  never a multiplied value

#### Scenario: A host column defined across a fanning hop is refused
- **WHEN** `orders` defines `li_qty` as `line_items.qty` over an undeclared
  one-to-many hop and a query rooted at `orders` selects `sum(li_qty)`
- **THEN** it fails with the input-safety error naming `line_items` and the
  cross-model spelling, while `sum(line_items.qty)` returns the per-line-item total

#### Scenario: A default and its explicit spelling are computed once
- **WHEN** one query rooted at `orders` selects both `wpop(amount)` — whose weight
  defaults to `customers.hr.pop` — and `wpop(amount, weight=customers.hr.pop)` under
  two names
- **THEN** both names carry the same value from a single computation (one producer in
  the generated SQL), each keeping its own public name, and the generated SQL equals
  the SQL of the same query spelling the default by model name (`customers.regions.pop`)

#### Scenario: A bare default names the owner's column even when the root shadows it
- **WHEN** both `customers` and the query root `orders` declare a column of the same
  name and a `customers`-declared aggregation defaults its weight to that bare name
- **THEN** the default reads the owner's (`customers`) column in every producer kind,
  equal by executed values on SQLite and DuckDB to the explicit
  `weight=customers.<field>` twin

#### Scenario: A mixed-frame expression default keeps a quoted column's identity
- **WHEN** an expression default combines an owner-local column whose physical name
  must be quoted (mixed-case or a reserved word) with a root-anchored column
  (`<owner field> + orders.amount`)
- **THEN** the quoted column renders with its exact identity and the executed value on
  SQLite and DuckDB equals the explicit two-reference spelling

#### Scenario: A default that revisits a dataset without cancelling is refused
- **WHEN** a `customers`-declared default returns to `customers` through edge names,
  which never cancel (`hr.back.spend`, with `hr` a named join `customers → regions` and
  `back` a named join `regions → customers`), alone or inside an expression default
  (`hr.back.spend * 1`)
- **THEN** the query is refused with the circular-join error when it is bound — never
  a reference emitted verbatim into the SQL, and never a later or different failure

## ADDED Requirements

### Requirement: An aggregation binds from its own anchor
An aggregation's source SHALL be bound from its **source anchor**, never through the query's
population (semantics Axiom 2.6): the dataset its source's references are anchored at when they
share one anchor; when they are anchored apart, the smallest dataset determining every leaf's
anchor over provably to-one hops — a dataset that determines another candidate it is not determined by back is not smaller. Tied
minimal candidates SHALL fail with the typed ambiguity error of `queries/references`, naming one
explicit single-anchor spelling per candidate, ordered by canonical dataset name, each in its
declared case; no candidate SHALL fail with the input-safety error naming the leaf and the hop.
The aggregation's parameters, definition defaults, `FILTER (WHERE …)` and ranking key SHALL be
bound from the same anchor, and its home resolved from there per › Home dataset of a row-level
aggregation source. Its producer SHALL be rooted at its home, read its grain dimensions and the
filter conjuncts restricting it from that home (`queries/references`), associate per home row
inside the home's own join tree — a keyless home's rows told apart by a per-query row number and a
list child's by its parent and element — and meet the result only on grain values, NULL-safely,
never on a key of the population. A saved metric's definition SHALL be bound from the dataset that
declares it, as a reference anchored there. No value or refusal of an aggregation SHALL depend on
the population's routes, on which dataset hosts an inferred stage, on sibling measures, or on the
order in which the query writes its entries. The time spine SHALL stay a spine query's row source,
producers only attaching to its grain.

#### Scenario: A named population with two routes to the anchor
- **WHEN** `orders → customers → regions`, `regions → managers` and `orders → managers` are
  many-to-one, and a query with `population: "regions"` groups by `regions.name` and measures
  `orders.total_revenue`, and again with the same aggregate as a filter, an order key, a computed
  dimension with `partition_by=[regions.name]`, and an attached parameter
- **THEN** each answers North 150 and South 300 in its position, the source binding from `orders`

#### Scenario: A metric written through another anchor binds from that anchor
- **WHEN** over the seed of `queries/population` › Rows of an inferred query are the row supply, a
  query without a population groups by `customers.region` and measures `sum(customers.orders.amount)`,
  then beside `returns.total_returns`, then with no dimension
- **THEN** it answers EAST 350 and WEST 300; beside the returns it adds NORTH with NULL and 10; with
  no dimension it answers one row, 650 — the home `orders` reached along the source's written path

#### Scenario: A saved metric bound through a host with two routes to its home
- **WHEN** `store_sales` and `store_returns` each relate many-to-one to `item` and to `date_dim`, and
  a query without a population groups by `item.brand` and `date_dim.year` and measures
  `store_sales.total_sales` and `store_returns.total_returns`
- **THEN** it answers Acme 2023 125/10, Acme 2024 200/NULL and Beta 2024 90/5, each saved metric
  bound from the dataset declaring it

#### Scenario: Leaves anchored apart meet at the smallest determining dataset
- **WHEN** `order_lines → orders → customers` and `order_lines → products` are many-to-one, and
  `sum(customers.discount * products.price)` is measured with `population: "customers"`, with
  `population: "products"` and without a population
- **THEN** it is homed on `order_lines`, each line counted once, with the same value per cell in all
  three queries

#### Scenario: Tied minimal datasets are refused
- **WHEN** `orders` and `tickets` each relate many-to-one to `customers` and to `managers`, and a query
  with `population: "orders"` measures `sum(customers.discount * managers.bonus)`
- **THEN** it fails with the ambiguity error naming `orders.customers.discount * orders.managers.bonus`
  and `tickets.customers.discount * tickets.managers.bonus`, and the first spelling executes

#### Scenario: Unconnected anchors are refused
- **WHEN** an aggregation's source reads `orders.amount * tickets.weight` and no join path connects
  `orders` and `tickets`
- **THEN** it fails with the input-safety error, never as a product of the two datasets

#### Scenario: Ranked and filtered aggregations bind from their anchor
- **WHEN** with the two-route schema above, a query with `population: "regions"` groups by
  `regions.name` and measures `last(orders.amount, orders.ordered_at)` and
  `sum(orders.amount) FILTER (WHERE orders.amount > 60)`
- **THEN** both bind from `orders`, the ordering key and the filter read from the home, with the
  values of the same aggregations without a population

#### Scenario: A spine query keeps its spine rows
- **WHEN** with the two-route schema, a query with `population: "regions"` groups by a monthly
  `time_spine.timestamp` bounded over a range with an empty month and by `regions.name`, and measures
  `orders.total_revenue`
- **THEN** every month appears for every region, the empty month carrying NULL, the source binding
  from `orders`

#### Scenario: The order of metrics never changes a value
- **WHEN** an inferred query measures `orders.total_revenue`, `returns.total_returns` and a
  re-aggregation, in each order
- **THEN** every order returns the same rows and values
