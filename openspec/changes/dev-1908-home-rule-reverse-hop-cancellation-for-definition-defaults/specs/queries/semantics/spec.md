## MODIFIED Requirements

### Requirement: Home dataset of a row-level aggregation source
An aggregation over a row-level source SHALL run over the rows of exactly one home
dataset (Axiom 2): the deepest join path from the query root from which every
dependency of every input — each source leaf, each column-valued parameter and each
non-overridden definition default, each taken through its dependency closure, and
each grain member of every attached constituent (an aggregate or grained transform
operand is opaque and stands for its grain: its explicit `partition_by=`, else the
query's dimensions; a transform's grain is the union of its inner aggregates', where
a windowed inner's grain always includes the query's time bucket whether or not its
`partition_by=` names it) — is
reachable over provably to-one hops. A non-overridden definition default SHALL be
resolved as a reference from the owning model — the source anchor that declares the
aggregation — with each column reference (bare, or one inside an expression default)
taken in the owner's coordinates. A qualifier naming a dataset already on the owner's
path from the query root — the root itself included — SHALL cancel the path back to
that dataset, so the reference reads that dataset's row on the path, keeping the
path's own spelling of it, never a second join to it; a qualifier the owner cannot
reach forward otherwise SHALL instead be anchored at the query root, while an
ambiguous or only partially resolvable owner reference — a first segment that
resolves, by cancellation or as a hop, followed by one that does not — SHALL fail
closed rather than silently re-anchor at the root. An edge-name segment never
cancels. Only a definition default cancels: a query-typed path or a model-SQL
fragment that revisits a dataset stays refused. A default that resolves to a genuine
host-local (root) column SHALL widen the home to the root exactly as spelling that
column explicitly would. This same owning-model resolution governs the input-safety
check and every rendering of the default in every producer kind — plain, association,
windowed and second-order — so a definition default whose definition crosses a fanning
hop SHALL fail closed even when other inputs widen the home away from the declaring
model, and a cancelled or root-anchored default is never rendered as a join from the
owner back to the dataset it names.
Candidates are the input paths and their
longest common prefix, deepest first; a tie prefers the source's anchor, the longest
common prefix of the source leaves' own paths. The aggregation is computed over the
home's rows, each counted once, never over a join product. When no candidate
determines every input the query SHALL fail with the input-safety error naming the
offending leaf and hop. The home SHALL depend on the leaves' paths alone, never on
the spelling of the expression. Every other rule — attribution and
`to_many_handling` modes, explicit grain, `window=`, filter routing, positions —
applies exactly as for a single-column source rooted at the home.

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
- **WHEN** a query rooted at `orders` selects `customers.spend:<agg>`, where `<agg>` is
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
- **WHEN** a query rooted at `orders` selects `customers.regions.countries.gdp:wsum_region_pop`,
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
  selects `customers.regions.countries.gdp:wsum_cust_spend2` from `orders`
- **THEN** the default resolves to `customers.spend`, the home is `customers`, and the
  value equals the explicit `weight=customers.spend` twin (670000 on the reference
  dataset), by executed values

#### Scenario: A default cancels and then walks forward
- **WHEN** the `countries`-declared default is `regions.customers.plans.fee` (queried as
  `customers.regions.countries.gdp:wsum_plan_fee`), or a `regions`-declared default is
  `customers.plans.fee` (queried as `customers.regions.pop:wsum_cust_plan_fee`)
- **THEN** each resolves to `customers.plans.fee`, homes at `customers`, and equals its
  explicit twin (100000 and 10000 on the reference dataset), by executed values

#### Scenario: Defaults in different frames resolve per reference
- **WHEN** a `countries`-declared aggregation defaults one parameter to `regions.pop`
  and another to `regions.customers.plans.fee`, queried as
  `customers.regions.countries.gdp:wsum_two` from `orders`
- **THEN** each default resolves in its own frame (`customers.regions.pop` and
  `customers.plans.fee`), the home is `customers`, and the value equals the explicit
  two-kwarg twin (17000000 on the reference dataset), by executed values

#### Scenario: A cancelled default homes above it over a to-one reverse hop
- **WHEN** a query rooted at `orders` selects `customers.regions.region_events.value:wsum_rp`,
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
  query selects `customers.regions.countries.gdp:wsum_fan` from `orders`
- **THEN** the query fails with the input-safety error naming `region_events`, never a
  multiplied value

#### Scenario: A reverse hop to a dataset not on the path stays refused
- **WHEN** a query rooted at `regions` selects `pop:wsum_cust_spend`, whose
  `regions`-declared default is `customers.spend`
- **THEN** nothing cancels — `customers` is not on the path — and the query fails with
  the same input-safety error naming `customers` that the explicit
  `weight=customers.spend` twin raises

#### Scenario: A query-typed revisit is refused
- **WHEN** a query selects `customers.regions.customers.spend:sum` from `orders`, or a
  model-SQL derived column's definition revisits a dataset on its own path
- **THEN** the query is refused with the circular-join error (the derived column with
  its own typed refusal), never silently cancelled

#### Scenario: The path's own spelling survives cancellation
- **WHEN** the query root is an inline extension of `orders` adding a named join
  `ship_region` to `regions`, and the query selects
  `ship_region.countries.gdp:wsum_region_pop`
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
- **WHEN** a query rooted at `orders` selects `amount:<agg>` where `<agg>` is declared
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
  one-to-many hop and a query rooted at `orders` selects `li_qty:sum`
- **THEN** it fails with the input-safety error naming `line_items` and the
  cross-model spelling, while `line_items.qty:sum` returns the per-line-item total
