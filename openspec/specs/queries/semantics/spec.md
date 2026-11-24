# queries/semantics Specification

## Purpose
States the query-wide semantic axioms every query obeys: which rows aggregations run
over, which dimensions they may vary along, what the result grain is, how different-grain
values combine, what filters do to populations, and what stays independent of what.
Producer mechanics and per-shape detail live in `queries/cross-model-aggregates`; the
complete end-target algebra (including not-yet-implemented semantics) is mapped with
status tags in `architecture/semantics.arc42.md`.

## Requirements

### Requirement: Attribution by determination
A query dimension SHALL be attributable to an aggregate iff a chain of provably
many-to-one join hops (per `models/join-cardinality` evidence) leads from the
aggregate's root model to the dimension. Attributable dimensions retain exact per-cell
values — the cells partition the root rows, and an additive aggregate's cells sum to its
total. Every other dimension —
reached across a fanning or an unproven hop alike — resolves per the query's
`to_many_handling` mode: when the mode is omitted or `"associate"`, each cell aggregates
over the distinct root entities associated with it (per `queries/attribution-modes`),
whatever its aggregation — `first`/`last` and windowed included; under
`"broadcast"` the aggregate broadcasts across it (its value repeats across that
dimension's cells); under `"error"` the query refuses. In no mode is an aggregation's
input multiplied by a join across a fanning or unproven path.

#### Scenario: Attributable dimension partitions, unattributable broadcasts
- **WHEN** a query rooted at `orders` with `to_many_handling: "broadcast"` selects
  `sum(customers.spend)` by one customer-determined dimension and one orders-level
  dimension
- **THEN** by executed values, cells vary along the determined dimension, sum to the
  total across it, and repeat unchanged across the orders-level dimension

#### Scenario: Associate mode keeps attributable dimensions exact
- **WHEN** the same query runs omitting the mode, and again with
  `to_many_handling: "associate"`
- **THEN** by executed values, in both runs cells along the customer-determined
  dimension are unchanged from the broadcast run, while cells along the orders-level
  dimension carry per-cell distinct-entity values

### Requirement: No double counting
Every aggregation — however consumed: directly selected, a composite leaf, filter-only,
order-only, inside a computed dimension, or nested in a producer, and local and
cross-model alike — SHALL run over the rows of its own root model, each root row
counted exactly once per result cell — never over the row product of a join; no join
fan-out may multiply an aggregation's inputs.

#### Scenario: Join fan-out never multiplies aggregation inputs
- **WHEN** a query rooted at `orders` selects `sum(customers.spend)` and each customer
  has several orders
- **THEN** by executed values every customer's spend is counted exactly once, however
  many orders fan the join

#### Scenario: Local aggregate over a fanning dimension never multiplies
- **WHEN** a query rooted at `customers` selects the local measure `sum(spend)` by
  `orders.status`, and one customer has two orders with the same status
- **THEN** by executed values that customer's spend is never counted twice in a cell —
  by default and under `to_many_handling: "associate"` the cell carries the
  distinct-entity value; under `"broadcast"` the metric broadcasts with a warning

#### Scenario: Fanned local aggregates are caught in every consumer context
- **WHEN** a local aggregate over a fanning dimension is consumed inside an arithmetic
  composite, an ORDER BY entry, an aggregate-phase filter, or a computed dimension
- **THEN** each context resolves it per the mode exactly as a directly selected
  measure — never through the silently multiplied join product

### Requirement: Grain guarantee
For a query with at least one measure, and for a measure-less query with distinct
dimension values enabled (the default), the result SHALL have exactly one row per
combination of dimension values present among the row-filtered population's rows — for a query
omitting `population`, among its row supply (`queries/population` › Rows of an inferred query
are the row supply), never one row twice. Raw-row mode (`distinct_dimension_values=false`) is the documented exception and
returns one row per population row. A row-level filter that reaches the population root
only across a non-determining path SHALL NOT multiply the population's rows, whatever the
conjunct's boolean shape: raw-row mode returns each population row passing the restriction
exactly once. A value the query evaluates only for a filter, an order key or an attached
aggregate's join SHALL NOT change the result's grain: it never adds a row, splits a
dimension combination, or repeats one.

#### Scenario: One row per dimension combination
- **WHEN** an aggregating query groups by dimensions whose value combinations repeat
  across many population rows
- **THEN** the result contains each present combination exactly once

#### Scenario: Raw rows are never multiplied by a population filter's join
- **WHEN** a query rooted at `customers` selects `dimensions: ["tier"]` with
  `distinct_dimension_values: false` and `filter: ["orders.status = 'ok'"]`, and one
  customer has two `ok` orders
- **THEN** by executed values the result has one row per customer with at least one `ok`
  order (five rows on the reference dataset), never one row per matching order

#### Scenario: A filter on a computed dimension's partition key keeps the dimension's grain
- **WHEN** a query rooted at `orders` selects the computed dimension
  `case when sum(amount, partition_by=customer_id) > 50 then 'high' else 'other' end`
  and the measure `sum(amount)`, with a row filter that reads `customer_id`
  (`customer_id is not null`, or `customer_id + 0 > 0`), over customers whose order totals
  are 90, 70 and 50
- **THEN** by executed values the result has exactly one row per band (`high` 160,
  `other` 50), the filter removing rows before every aggregation, never one row per customer

#### Scenario: A filter on a joined partition key keeps the dimension's grain
- **WHEN** the computed dimension partitions by a joined column (`customers.tier` or
  `customers.regions.name`), a row filter reads that column, and one band covers rows of
  two distinct values of it
- **THEN** by executed values the band appears in exactly one row, never once per value of
  the partition key

#### Scenario: An order key beside a ranked measure keeps the query's grain
- **WHEN** a grouped query declares a `first` or `last` measure (e.g.
  `last(amount, ordered_at)`) and orders by a column that is not a dimension, local
  (`city`) or joined (`customers.regions.name`)
- **THEN** by executed values the result has exactly one row per dimension combination
  (one row when the query has no dimensions), never one row per value of the order key

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

### Requirement: Filters restrict by association or fail loudly
A row-level filter conjunct that reaches a dataset's root only across non-determining
paths SHALL restrict that dataset's population by association — the dataset computes
over exactly the root rows related to at least one surviving row combination, each
counted once; a conjunct naming a reference with no resolvable join path SHALL fail at
resolution with a typed error in every `to_many_handling` mode, never be dropped (spec:
`queries/cross-model-aggregates`). This applies to the query population itself exactly as
to an aggregate's root: a conjunct reaching the population root only across a
non-determining path restricts the population by association, every aggregate evaluated
over the population rows — inline or in a producer rooted at the population — counts each
population row once, the filtered model is not joined into the base query for that
conjunct, and the restriction is reported through the semi-join informational entry with
no aggregate named — never an error, in any `to_many_handling` mode, whatever the query
selects. A conjunct whose non-determining paths a consumer's grain already materialises (a
projected dimension or time dimension on the same join branch) applies to that consumer on
the same related row instead, so filtering and grouping on one branch bind to one row; when
the grain materialises only some of a conjunct's branches, the conjunct's references on a
materialised branch bind to the grouped row and only the remaining branches are quantified.
A stated restriction SHALL never be silently ignored and SHALL never fan out an
aggregation's inputs. Whether a conjunct crosses a non-determining path is judged on its
dependency closure — a reference to a derived column whose definition crosses such a path
crosses it too; a conjunct whose closure cannot be analysed SHALL fail at plan time with a
typed error naming the filter and the column, in every mode, whether or not the query
aggregates. Restriction by association is defined on the root row's join product over the
branches the conjunct references, built as the inline path would join them (each hop with
its declared join type, LEFT by default, so a hop with no related row contributes NULL
columns): the root row survives iff the conjunct holds on at least one row of that product,
root-local references taking the root row's values, conjuncts sharing a branch judged on
one product row, SQL three-valued logic applying inside and the restriction itself never
unknown. This rule is total over the conjunct's boolean shape — a root-local and a cross-path
reference mixed under `OR`/`NOT`, cross-path references spanning several join branches, and
an atom comparing columns of two branches all restrict by association, never dropped and
never an error in any mode; negation keeps the existential reading (`NOT B` holds when some
related row fails `B`), and a null-test on a related column holds for a root row with no
related row.

#### Scenario: Cross-path filter restricts the population by association
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects
  `sum(customers.spend)`
- **THEN** by executed values the metric counts exactly the customers with at least one
  order passing the predicate, each once

#### Scenario: A restriction is never silently ignored
- **WHEN** a filter conjunct names a reference with no resolvable join path from the
  query root
- **THEN** the query fails at resolution with a typed error in every `to_many_handling`
  mode — never an unrestricted value presented as restricted

#### Scenario: Population filter across a fanning hop restricts the population
- **WHEN** a query rooted at `customers` filters on `orders.status = 'ok'` and selects the
  local `sum(spend)`, in any `to_many_handling` mode, and one customer has two `ok` orders
- **THEN** by executed values the metric equals the spend of the distinct customers with at
  least one `ok` order, each once (420 on the reference dataset, never 520); the base query
  restricts `customers` by a correlated semi-join and does not join `orders`; the response
  carries a `semi_join_pushed` entry naming the filter with no aggregate; no Python-level
  warning is emitted

#### Scenario: Derived population filter across a fanning hop restricts the population
- **WHEN** a query rooted at `orders` filters on `customers.regions.bad_pop > 0` (a derived
  column whose definition crosses the one-to-many `regions → region_events` hop) and
  selects the local `sum(amount)`, in any `to_many_handling` mode
- **THEN** by executed values the metric equals the amount of the orders whose region
  passes the predicate, each order once (120 on the reference dataset, never 220)

#### Scenario: Population filter over a provably to-one path stays inline
- **WHEN** a query rooted at `orders` filters on `customers.tier = 'gold'` and selects the
  local `sum(amount)`
- **THEN** the query executes with the filter applied as a plain row restriction, by
  executed values and generated SQL unchanged from today

#### Scenario: Filter and dimension on one branch bind to the same row
- **WHEN** a query rooted at `customers` selects `dimensions: ["orders.status"]`, the
  local `sum(spend)`, and `filter: ["orders.amount = 20"]`, where the only order of
  amount 20 has status `new` and its customer also has `ok` orders
- **THEN** the result has exactly one cell, `new`, by executed values — the filter and
  the dimension are evaluated on the same joined row, never on the customer's other orders

#### Scenario: Two branches restrict the population independently
- **WHEN** a query rooted at `customers` selects `sum(spend)` with
  `filter: ["orders.status = 'ok'", "regions.region_events.value >= 50"]`, where
  `customers → orders` and `regions → region_events` are one-to-many
- **THEN** by executed values the metric counts exactly the customers with at least one
  `ok` order whose region has at least one event of value 50 or more, each once (280 on the
  reference dataset), and the response carries one `semi_join_pushed` entry per filter

#### Scenario: Unanalyzable filter dependency fails closed
- **WHEN** a query filters on `customers.regions.unparseable > 0`, a derived column no
  supported dialect can parse, with or without a measure, in any `to_many_handling` mode
- **THEN** the query fails at plan time with a typed error naming the filter and the column,
  containing no issue reference — never SQL routed as if the column crossed nothing

#### Scenario: Population filter across a fanning hop with an inline aggregate fails closed
- **WHEN** a query rooted at `customers` filters on
  `tier = 'bronze' or orders.status = 'ok'` (a root-local and a cross-path reference under
  `OR`, the cross-path leg reaching only across the fanning `orders` hop) and selects the
  local `sum(spend)`, in any `to_many_handling` mode
- **THEN** the query no longer fails closed: by executed values the metric equals the spend of
  the distinct bronze customers and customers with at least one `ok` order, each once (460 on
  the reference dataset, never the join-multiplied 560); the base query does not join
  `orders`; the response carries a `semi_join_pushed` entry naming the filter with no
  aggregate and no dropped-filter warning; no Python-level warning is emitted

#### Scenario: A root row with no related row is judged with NULL related columns
- **WHEN** a query rooted at `customers` filters on `tier = 'gold' or orders.status = 'ok'`
  and selects `sum(spend)`, where one gold customer has no orders at all
- **THEN** by executed values that customer is counted — the disjunction holds on the
  customer's null-extended row — so the metric is 475 on the reference dataset, never 420
  (the customer wrongly dropped) and never the join-multiplied 675

#### Scenario: Negation keeps the existential reading
- **WHEN** a query rooted at `customers` filters on
  `not (tier = 'gold' and orders.status = 'ok')` and selects `sum(spend)`
- **THEN** by executed values the metric counts the non-gold customers together with the
  gold customers having at least one order that is not `ok`, each once (370 on the reference
  dataset); a gold customer with no orders is not counted, and the value is never the
  join-multiplied 520

#### Scenario: A null-test on a related column reads as absence
- **WHEN** a query rooted at `customers` filters on `orders.id is null` and selects
  `sum(spend)`
- **THEN** by executed values the metric counts exactly the customers with no orders (55 on
  the reference dataset), and the response carries the `semi_join_pushed` entry

#### Scenario: Branches under a disjunction restrict independently
- **WHEN** a query rooted at `customers` filters on
  `orders.status = 'new' or regions.region_events.value >= 50` and selects `sum(spend)`
- **THEN** by executed values the metric counts the customers with at least one `new` order
  together with those whose region has an event of value 50 or more, each once (320 on the
  reference dataset), with one `semi_join_pushed` entry for the filter

#### Scenario: An atom spanning two branches is judged on their product
- **WHEN** a query rooted at `customers` filters on
  `orders.amount < regions.region_events.value` and selects `sum(spend)`
- **THEN** by executed values the metric counts the customers having some order and some
  region event with the order's amount below the event's value, each once (420 on the
  reference dataset), never a dropped-filter warning

#### Scenario: A materialised branch binds to the grouped row inside a multi-branch conjunct
- **WHEN** a query rooted at `customers` selects `dimensions: ["orders.id"]` and no measures
  with `filter: ["orders.status = 'ok' or regions.name = 'South'"]`
- **THEN** by executed values the result has one cell per order that is itself `ok` or whose
  customer's region is `South` (orders 1, 3, 5, 7, 9 and 10 on the reference dataset), plus the
  null-order cell for the orderless customer whose region is `South` (its LEFT-extended row
  satisfies the region leg: `NULL = 'ok' OR 'South' = 'South'` is TRUE) — never a cell for a
  customer's other order admitted because a sibling order is `ok`

#### Scenario: Out-of-scope conjunct without an inline aggregate keeps applying
- **WHEN** a query rooted at `customers` selects `sum(spend, partition_by=tier)` by `tier`
  with `filter: ["tier = 'bronze' or orders.status = 'ok'"]`, in any `to_many_handling`
  mode
- **THEN** the conjunct keeps applying, now by association in the producer too: by executed
  values each tier cell equals the spend of that tier's distinct customers passing the
  disjunction, each once (gold 190, silver 230, bronze 40 on the reference dataset), the
  response carries the producer's and the population's
  `semi_join_pushed` entries and no dropped-filter warning, and `to_many_handling: "error"`
  does not error

#### Scenario: Out-of-scope conjunct in raw-row mode fails closed
- **WHEN** a query rooted at `customers` selects `dimensions: ["tier"]` with
  `distinct_dimension_values: false` and `filter: ["tier = 'bronze' or orders.status = 'ok'"]`,
  and one customer has two `ok` orders
- **THEN** the query no longer fails closed: by executed values the result has one row per
  bronze customer or customer with at least one `ok` order (six rows on the reference
  dataset), never one row per matching join row (seven) — the raw-row grain guarantee holds by
  association

#### Scenario: Dimension-only and producer-only queries are unaffected by the guard
- **WHEN** a query rooted at `customers` filters on `orders.status = 'ok'` and selects no
  measures, or selects only aggregates that compute in their own producers (cross-model,
  partitioned, or windowed)
- **THEN** the retired guard blocks nothing: the query executes with values unchanged from
  today, the filter applied to the population and to each producer by association

#### Scenario: Population filter across a fanning hop restricts a mixed re-aggregation constituent
- **WHEN** a customers query over `[tier]` selects
  `sum(spend * avg(sum(spend, partition_by=[tier, plan_code]), partition_by=tier))` with
  the filter `orders.status = 'ok'`
- **THEN** the constituent's operand cells and its per-tier average are computed over
  the restricted population only (gold 18050 = 190 × 95, silver 26450 = 230 × 115),
  never over rows multiplied by the fanning join; every producer relation carries the
  semi-join and none joins `orders`; the semi-join informational entries name the
  selected measure; and selecting the same re-aggregation standalone beside it shares
  one producer relation and reports under its own name

#### Scenario: Re-aggregation producers report a dropped out-of-scope conjunct
- **WHEN** a customers query over `[tier]` selects
  `min(sum(spend, partition_by=[tier, plan_code]), partition_by=tier)` with the filter
  `tier = 'bronze' or orders.status = 'ok'`, in any `to_many_handling` mode
- **THEN** nothing is dropped: the conjunct restricts the re-aggregation's carrier and
  outer producer by association exactly as it does a plain partitioned producer — by
  executed values each tier cell is the smallest per-plan spend among that tier's
  customers passing the disjunction (gold 30, silver 80, bronze 40 on the reference
  dataset), every producer relation carries the semi-join, and the response carries a
  `semi_join_pushed` entry naming the conjunct under the selected measure and no
  dropped-filter warning

### Requirement: Compositionality
Each result cell's values SHALL depend only on the evaluated expression, the
cell, and the row-level filters — never on which other measures, measure-typed
filters, or order entries the query contains. In a query naming `population`, adding or
removing a projected measure SHALL NOT change the row set or any other column's values. In a
query omitting it with at least one measure, adding a measure SHALL NOT remove a row or change any value in an existing
row, a row it adds carrying each other measure's value on its own grain there (its empty value
where it has no cell). Transforms obey both guarantees: each evaluates over its operand's own
cells (`queries/transforms` › Transforms evaluate over their operand's domain). A measure-typed
filter masks result cells without changing any surviving cell's values; ORDER BY and LIMIT
select and order rows without changing any cell's values. A multi-measure query SHALL
return, cell by cell, the same values as its single-measure splits in each split's rows, and
in the rows only its siblings supply, its value on its own grain there (its empty value where it
has no cell). Selecting an
expression as a measure SHALL NOT make any other measure, filter or order entry that
contains it fail.

#### Scenario: Adding a measure changes nothing else
- **WHEN** any supported query naming `population` runs with and without one additional measure
- **THEN** both runs return the same rows and identical values in all shared columns

#### Scenario: Adding a measure to an inferred query only adds rows
- **WHEN** a query without `population` grouping by `customers.region` and measuring
  `orders.total_revenue` adds `returns.total_returns`
- **THEN** EAST 350 and WEST 300 keep their values, and the added NORTH row carries NULL revenue

#### Scenario: Measure-typed filter masks without altering values
- **WHEN** a query filters on an aggregate predicate
- **THEN** surviving cells carry exactly the values the unfiltered query gave them

#### Scenario: A query equals its single-measure splits
- **WHEN** a two-measure query and its two single-measure counterparts run
- **THEN** each measure's values match cell by cell across the runs

#### Scenario: A saved ratio measure alongside a transform over it
- **WHEN** a query rooted at `orders` with a month time dimension (with a `date_range`,
  and again with a plain date filter instead) selects the saved measure
  `aov = sum(amount) / count(*)` and `cumsum(aov)`, and again with the inline formulas
  `sum(amount) / count(*)` and `cumsum(sum(amount) / count(*))`
- **THEN** every run returns the monthly ratio and its running sum, by hand-computed
  executed values on SQLite and DuckDB, equal cell by cell to the two single-measure runs

#### Scenario: A selected composite alongside any consumer that contains it
- **WHEN** a query selects a composite measure (a ratio of local aggregates, and a ratio
  with a cross-model aggregate operand) together with one consumer containing it — each of
  `cumsum`, `lag`, `lead`, `change`, `change_pct`, `consecutive_periods` and a rank-family
  transform over it; an arithmetic composite combining it with a transform; a transform
  over a larger expression containing it; a measure-typed filter on a transform of it; an
  ORDER BY on a transform of it
- **THEN** the query executes on SQLite and DuckDB with no internal error, every measure
  equals its single-measure run cell by cell, the filter and the order entry select and
  order exactly the rows they do without the composite selected, and no internal
  placeholder appears in the generated SQL

#### Scenario: Generated SQL for a reused composite is pinned across dialects
- **WHEN** a selected composite is reused by a measure-typed filter, and by an order-only
  composite, and the queries are rendered for PostgreSQL and T-SQL
- **THEN** the generated SQL matches recorded golden baselines

#### Scenario: A transform keeps its values when a sibling adds rows
- **WHEN** a query without `population` grouping by `customers.region` selects
  `percent_rank(count(orders.*))`, and again with `returns.total_returns` added
- **THEN** EAST 1 and WEST 0 keep their values in both runs, and the added NORTH row carries NULL

### Requirement: Loud degradation
Whenever partition semantics is unavailable — an implicit attribution-loss broadcast,
or a filter excluded from a producer — the response SHALL carry a machine-readable
warning naming the affected aggregate and the cause, and `to_many_handling: "error"`
SHALL turn it into an error (per the metadata and mode requirements in
`queries/cross-model-aggregates` and `queries/attribution-modes`). A broadcast under
explicit `"broadcast"` SHALL carry the dice–slice hint: filtering on the dimension
restricts the metric by association while slicing broadcasts, and omitting the mode (or
`"associate"`) reconciles the two. Omitting the mode never broadcasts. Associated resolution SHALL warn that the cells' entity populations may overlap across
the named dimensions and are not additive. Explicit `partition_by=` is requested grain
and SHALL NOT warn — broadcasting and associating alike. Warnings and errors SHALL name
a dimension by its query name — a named dimension's `name`, else its canonical dotted
path — never an internal alias.

#### Scenario: Implicit broadcast warns, explicit grain does not
- **WHEN** one query under `to_many_handling: "broadcast"` broadcasts a metric over an
  unattributable dimension and another declares the same coarser grain via
  `partition_by=`
- **THEN** the first response carries the broadcast warning and the second carries none

#### Scenario: Broadcast warning carries the dice–slice hint
- **WHEN** any query under `to_many_handling: "broadcast"` broadcasts a metric over an
  unattributable dimension
- **THEN** the broadcast warning entry names omitting the mode (or `"associate"`) as the
  reconciliation, whether or not the query contains a filter

#### Scenario: A fallback broadcast carries its reason instead
- **WHEN** a query omitting the mode selects a `first`/`last` aggregate that needs
  association
- **THEN** there is no fallback broadcast: the response carries the associated-cells
  warning and no broadcast warning

#### Scenario: Associate warns about overlapping populations
- **WHEN** a query omitting the mode, or under `"associate"`, resolves a metric over an
  unattributable dimension implicitly (no explicit `partition_by=`)
- **THEN** the response carries a warning naming the metric and the dimensions whose
  cells are not additive, and the same event surfaces as a Python-level warning

#### Scenario: Diagnostics name dimensions by their query name
- **WHEN** a re-aggregation broadcasts over the joined dimension `customers.regions.name`
  (e.g. `avg(sum(amount, partition_by=amount))` rooted at `corders`), and another over a
  named computed dimension `is_p` defined as `product = 'P'`, each under
  `to_many_handling: "broadcast"` and again under `"error"`
- **THEN** the broadcast warnings and the errors name `customers.regions.name` and
  `is_p` — never a `__`-joined path or a generated `grain_…` alias

### Requirement: Dice–slice correspondence under associate
When the mode is omitted or `to_many_handling: "associate"`, filtering the population to
`d = v` SHALL yield the same value for every aggregate — `first`/`last` and windowed ones
included — as slicing the `v` cell of the same query grouped by `d`, by executed values,
for any dimension `d` reachable from the population. Explicit `"broadcast"` does not satisfy this
correspondence; its warning hint (per Loud degradation) is the required disclosure.

#### Scenario: Filtered value equals the sliced cell
- **WHEN** a query omitting the mode, and again under `"associate"`, filters
  `status = 'ok'` with `sum(customers.spend)` rooted at `orders`, and its counterpart
  groups by `status` instead
- **THEN** the filtered run's value equals the `ok` cell of the grouped run, by
  executed values, across a generated family of query shapes

#### Scenario: Filtered first/last and window equal the sliced cell
- **WHEN** the same pair of queries selects `last(customers.spend, customers.signup_at)`,
  and another pair selects `sum(customers.spend, window='1y')` by the month of `ordered_at`
- **THEN** each filtered run equals the `ok` cell of its grouped run, by executed values

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

### Requirement: Aggregation parameters are typed by the home dataset's grain
Every parameter of an aggregation — a keyword or positional parameter (`weight=`,
`other=`, a custom aggregation's declared parameters) and a parameter supplied by the
aggregation definition's default — SHALL be typed against the dataset the aggregation
runs over: with `D` that dataset and `G` its grain, a parameter `P` is legal iff `G`
determines `P` — `P` is a grain member, an aggregate each of whose `partition_by=`
members `G` determines (a cell value of the same dataset), or a column reached from a
grain member over provably to-one join hops (per Axiom 1, Determination), determination
being closed under row-level combination. A parameter naming a
derived column is determined iff `G` determines every dependency of that column's
definition, recursively — a derived parameter whose definition crosses a hop `G` does
not pin is not determined, however its own path is reached. A legal parameter is evaluated once per
cell of `D` and the aggregation reads that value; the origin of `D`'s rows — a model's
rows deduplicated per entity, or another aggregate's cells — MUST NOT affect the rule.
A parameter `G` does not determine SHALL fail at plan time with a typed error naming
the parameter, the grain, and the remedy (aggregate the parameter to that grain, or add
its determining keys to the operand's `partition_by=`) — never invalid SQL or a silently
arbitrary value. The rule applies identically in every `to_many_handling` mode and in
every consumer position.

#### Scenario: Parameter determined by the entity key under association
- **WHEN** a query rooted at `orders` with `to_many_handling: "associate"` selects
  `weighted_avg(customers.spend, weight=customers.spend)` by the orders-level dimension
  `status`
- **THEN** each status cell equals the spend-weighted average over the distinct customers
  associated with it, by hand-computed executed values on SQLite and DuckDB — each
  customer weighted once, never the join-multiplied figure — with unchanged result grain

#### Scenario: Definition-default parameter follows the same rule
- **WHEN** a model declares a custom aggregation whose parameter defaults to a column of
  the aggregate's root (e.g. `wsum` with `weight` defaulting to `spend`) and an
  associate-mode query selects `wsum(customers.spend)` by an unattributable dimension
- **THEN** it executes with the default applied once per distinct entity, by executed
  values, identical to spelling the parameter explicitly

#### Scenario: Parameter determined over a to-one chain from the entity key
- **WHEN** an associate-mode aggregate's parameter is a column reached from the
  aggregate's root over a provably to-one join (e.g. `weight=customers.regions.pop`)
- **THEN** the query executes with the parameter read once per associated entity, by
  executed values

#### Scenario: Parameter as a cell of the operand dataset
- **WHEN** a query over `[region]` selects
  `weighted_avg(sum(amount, partition_by=[city, region]), weight=count(id, partition_by=[city, region]))`
- **THEN** each region row carries its city totals averaged with each city's row count as
  the weight, by hand-computed executed values on SQLite and DuckDB, equal to the manual
  two-stage encoding of the same computation

#### Scenario: Parameter determined over a to-one chain from the operand grain key
- **WHEN** the operand grain is an entity key (e.g. `sum(amount, partition_by=customer_id)`
  rooted at `corders`) and the parameter is a column reached from it over a provably
  to-one join (`weight=customers.region_id`)
- **THEN** the query executes with the parameter read once per cell, by executed values

#### Scenario: Parameter not determined by the grain fails closed
- **WHEN** the outer aggregation's parameter is a population-row column against a coarser
  cell grain (`weighted_avg(sum(amount, partition_by=[city, region]), weight=id)`), a
  definition default naming such a column, or an aggregate grained outside the operand
  grain (`weight=sum(amount, partition_by=product)` over a `[city, region]` operand)
- **THEN** the query fails at plan time with a typed error naming the parameter, the
  grain, and the remedy, containing no issue reference

#### Scenario: Derived re-aggregation parameter over an unpinned fanning hop fails closed
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(sum(amount, partition_by=customers.regions.id), weight=customers.regions.bad_pop)`,
  where `regions.bad_pop` is defined as `pop + region_events.value` over the one-to-many
  `regions → region_events` hop the operand grain does not pin
- **THEN** the query fails at plan time with the typed parameter error naming `weight`,
  the grain, and the `partition_by=` remedy — never the multiplying join

#### Scenario: Derived re-aggregation parameter with local-only dependencies executes
- **WHEN** the same query's weight is `customers.regions.derived_pop`, a derived column
  defined as `pop * 2` on `regions`, and the operand grain pins `regions` by its entity key
- **THEN** the query executes with each region cell weighted by twice its population, by
  hand-computed executed values on SQLite and DuckDB

#### Scenario: Derived parameter over grain members is determined
- **WHEN** a query over `[region]` selects
  `weighted_avg(sum(amount, partition_by=[city, region]), weight=city_len)`, with
  `city_len` a derived column defined as `LENGTH(city)`
- **THEN** it executes with each city cell weighted by its name length, by hand-computed
  executed values on SQLite and DuckDB; the same query with `weight=prod_flag` (a derived
  column reading `product`) fails with the typed parameter error

#### Scenario: NULL parameter values follow SQL aggregate semantics
- **WHEN** a legal parameter is NULL for some cells (e.g. a to-one lookup with no match)
- **THEN** those cells contribute exactly as the underlying SQL aggregate treats NULL
  inputs (a NULL weight contributes nothing to `weighted_avg`), by executed values on
  SQLite and DuckDB

#### Scenario: Attached parameter grained by an expression over home-determined columns
- **WHEN** a cross-model aggregate rooted at `corders` and homed on `customers` carries an
  attached parameter whose `partition_by=` names a computed dimension `rid10` defined as
  `customers.region_id * 10`
- **THEN** the query executes, by executed values on SQLite and DuckDB, with values
  identical to the same query with `partition_by=[customers.region_id]` — never a
  parameter-grain error

### Requirement: Row-grain aggregation sources
An aggregation whose source operand combines row-level column references with
attached values — partitioned aggregates or explicitly grained transforms, directly
or through arithmetic and scalar functions — SHALL aggregate over the row-filtered
population rows of its home dataset, never over the attached operands' cells. The
operand types at row grain: the union of a row leaf's grain with any attached
constituent's grain is row grain, the finest. Each attached constituent is computed
at its own declared grain (an aggregate at its `partition_by=` grain, a transform at
the union of its inner aggregates' grains, minus its time axis for `first`/`last`;
a constituent with no declared grain is typed at the query's dimensions) and its
value is broadcast onto each population
row null-safely: a row whose constituent lacks a value carries NULL for that
constituent, and the attachment never adds or removes rows. Per-row weighting is
the defined meaning of the shape and the broadcast SHALL NOT warn. The outer
aggregation evaluates at its consumer grain exactly as over any row-level
expression. Adding such a measure MUST NOT change the result row count or any
other column's values.

#### Scenario: Row-weighted value distinguishable from pure re-aggregation
- **WHEN** a query over dimensions `[region]` selects the measure
  `sum(quantity * avg(unit_price, partition_by=product))`
- **THEN** each region row carries the sum, over that region's base rows, of the
  row's `quantity` times its product's average unit price, by executed values,
  distinguishable from the pure re-aggregation
  `sum(avg(unit_price, partition_by=product))` and from
  `sum(quantity * unit_price)`

#### Scenario: Transform constituent inside a mixed source
- **WHEN** a query over dimensions `[region]` selects the measure
  `sum(quantity * rank(avg(unit_price, partition_by=product), direction='desc'))`
- **THEN** each region row carries the sum over its base rows of `quantity` times
  the rank of the row's product among products by average unit price, by executed
  values, with unchanged cardinality

#### Scenario: Missing constituent value is NULL on a surviving row
- **WHEN** some base row's cell has no value for an attached constituent and the
  operand restores it through a NULL-restoring composite (e.g.
  `coalesce(avg(unit_price, partition_by=product), 0) * quantity`)
- **THEN** that row still contributes to the outer aggregation with the restored
  value — the population row exists and only its constituent value was NULL,
  unlike a fully-attached source, whose carrier excludes the absent cell

#### Scenario: Ungrained inner constituent types at the query's dimensions
- **WHEN** a query selects `sum(quantity * avg(unit_price))` with no
  `partition_by=` on the inner aggregate
- **THEN** the inner value is computed at the query's dimensions, broadcast onto
  each row, and weighted per row, with no degenerate-re-aggregation warning

#### Scenario: Row filters bound both the population and the constituents
- **WHEN** the query carries a row-level filter conjunct
- **THEN** it restricts both the population rows the outer aggregation consumes
  and each attached constituent's producer, and the executed value reflects both

### Requirement: Ungrained aggregate parameters type at the query grain
An aggregate-valued parameter with no declared `partition_by=` SHALL be typed at
the query's dimensions — exactly as an ungrained aggregation source constituent
is — before the parameter determination rule is applied: it is legal iff the
aggregation's operating grain determines the query's dimensions, and it is then
evaluated once per query-grain cell and attached into the aggregation's input
relation (a row-level source) or carried as a constituent of the operand dataset
(a re-aggregation), broadcast onto that dataset's cells. The rule SHALL apply
identically in every kernel and in every `to_many_handling` mode; rejecting the
ungrained form on construction grounds is a closure violation.

#### Scenario: Ungrained parameter on a row-level source
- **WHEN** a locally-rooted query over `[region]` selects the model-defined
  `wsum(amount, weight=sum(amount))` (`SUM({value} * {weight})`)
- **THEN** each region row equals the region's row sum times the region total —
  the query-grain value row-attached as the weight — by executed values

#### Scenario: Ungrained parameter under association
- **WHEN** an associate-mode query rooted at `orders` over `[status]` selects
  `wsum(customers.spend, weight=sum(amount))`
- **THEN** each status cell equals the sum of its distinct associated customers'
  spend times that status cell's order total, by executed values

#### Scenario: Ungrained parameter on a re-aggregation
- **WHEN** a query over `[region]` selects
  `wsum(sum(amount, partition_by=[city, region]), weight=count(id))`
- **THEN** the parameter is typed at `[region]`, carried as a constituent of the
  `[city, region]` operand dataset and broadcast onto its cells, and the
  executed value equals the manual encoding with
  `count(id, partition_by=region)` — never the not-determined rejection

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

### Requirement: Aggregates are virtual models
An aggregate SHALL behave as a virtual model keyed by its grain: its rows are computed
from its source by the same joins as any query — a to-one hop that finds no related row
null-extends, never drops the row — and by the query's row filters, whatever consumes
it. In an expression its value SHALL be read as a field of that model, joined
one-to-one on the grain, NULL being a grain value like any other: a NULL grain cell
receives the virtual model's NULL row, and a virtual-model row with no matching result
cell contributes nothing. Consequently a source row whose path to a grain dimension is
broken (an orphan or a dangling reference) counts in the NULL cell of that dimension,
exactly as it does when the same aggregate is materialised on its own; and the value
never depends on which dataset roots the query.

#### Scenario: Orphan child rows count in the NULL grain cell
- **WHEN** a query rooted at `customers` selects `sum(orders.amount)` and
  `count(orders.id)` by `regions.name`, one customer has no region, and one order has no
  customer
- **THEN** the NULL-region cell holds the region-less customer's orders plus the
  customerless order (47 / 2 on the reference dataset), identically under `broadcast`,
  `associate` and `error` and with the population inferred, by executed values on
  SQLite and DuckDB

#### Scenario: Attached value equals the materialised aggregate
- **WHEN** the same aggregate is saved as a query-backed model grouped by the same grain
- **THEN** every result cell of the attached aggregate, the NULL cell included, equals
  that model's row for the cell's grain value, for the query rooted at the population
  and at the aggregate's source alike, by executed values

#### Scenario: A dangling or partial reference counts like an orphan
- **WHEN** a child row's foreign key names no existing parent, or a composite foreign
  key is partially NULL, and the aggregate is grouped by a parent-level dimension
- **THEN** the row counts in the NULL cell of that dimension exactly as in the
  materialised aggregate, by executed values

#### Scenario: Multi-hop grain through a broken hop
- **WHEN** a query rooted at `regions` selects `sum(customers.orders.amount)` by `name`,
  and one region's name is NULL
- **THEN** the NULL-name cell equals the materialised aggregate's NULL row — that
  region's orders together with every order whose path to a region is broken — by
  executed values

#### Scenario: A row filter narrows the virtual model's rows
- **WHEN** a query rooted at `orders` under `associate` selects `sum(customers.spend)` by
  `status` with the row filter `channel = 'app'`, and one customer has no orders
- **THEN** the orderless customer fails the filter on its null-extended row and counts
  in no cell, and every cell equals the hand-computed aggregate over the customers
  associated with app orders, by executed values

### Requirement: Division by zero is NULL

A division whose divisor evaluates to 0 SHALL be NULL on every backend and in every position (measure, filter, order key, computed dimension) — never an error, an infinity or NaN.

#### Scenario: A ratio over an empty population

- **WHEN** a query measures `sum(amount) / count(*)` and its filters keep no population row
- **THEN** one row is returned whose ratio is NULL on SQLite and DuckDB, and the SQL emitted for every dialect guards the divisor with `NULLIF(…, 0)`

### Requirement: The home, not the query root, decides where an aggregation is computed

An aggregation over a row-level source whose home (per "Home dataset of a row-level aggregation source") is a dataset below the query root SHALL be computed over that home's rows exactly as when the query is rooted on any other dataset whose join paths reach the same home: the same value per cell it can attribute, the same input-safety outcome, the same refusal when it is refused. The longest common prefix of the source's own leaves (its anchor) SHALL NOT decide where it is computed. An aggregation homed on the query root, a second-order aggregation (homed on its operand dataset) and an aggregation in a query whose root is not a model SHALL keep their own evaluation.

#### Scenario: A root column and a list element home on the list child
- **WHEN** a query on `contractors` measures `sum(rate * length(skills.value))`, and a query on `assignments` measures `sum(contractors.rate * length(contractors.skills.value))`
- **THEN** both return 230 — each element counted once with its contractor's rate — by executed values; grouped by `team` (by `contractors.team` from `assignments`) both return A 130 and B 100

#### Scenario: A list-element definition default reads like from its home
- **WHEN** a custom aggregation `w` declared on `contractors` as `SUM({value} * length({weight}))` defaults `weight` to `skills.value`, and `w(rate)` is measured from `contractors` and `w(contractors.rate)` from `assignments`
- **THEN** both return 230, by executed values

#### Scenario: A root column and a reverse to-many column home on the child dataset
- **WHEN** a query on `contractors` measures `sum(rate * assignments.id)`, and a query on `assignments` measures `sum(contractors.rate * id)`
- **THEN** both return 460, by executed values

#### Scenario: Every producer kind is independent of the query root
- **WHEN** an aggregation homed below the query root on `contractors` is a `FILTER` aggregation, a ranked or `first` / `last` aggregation, a windowed aggregation, an aggregation with an attached constituent, or an explicit `partition_by=` aggregation, and the same aggregation is spelled from `assignments`
- **THEN** each returns the same values from both populations, or both populations refuse it with the same typed error

#### Scenario: Undetermined inputs are refused from every population
- **WHEN** an aggregation's inputs are determined by no single dataset over provably to-one hops
- **THEN** it is refused with the same input-safety error from every population that reaches its inputs

#### Scenario: A to-one input keeps the aggregation on its anchor
- **WHEN** `orders` and `customers` are linked one-to-one by their primary keys, and a query on `orders` measures `scaled(price, scale='customers.weight')` with `scaled` defined as `SUM({value}) / MAX({scale})`
- **THEN** it is computed over the rows of `orders`: an order with no matching customer still counts in `SUM(price)`

#### Scenario: A home-changing default after a redundant partition still homes below the root
- **WHEN** an aggregation homed below the query root also carries a redundant `partition_by=` that the query's grain already implies and a definition default that moves its home
- **THEN** it is computed at the same home and returns the same value as without the redundant `partition_by=`

### Requirement: Filters restrict every home of an inferred query
In a query omitting `population`, a field-typed filter conjunct SHALL restrict the rows of every
aggregation's home, and of the dimension anchor where its cells are supplied, before any tuple is
taken: inlined where the dataset determines every reference of the conjunct, otherwise by
association, exactly as a conjunct restricts a named population (each restricted row counted
once, the restriction reported through the semi-join informational entry naming the aggregate).
Each conjunct's references SHALL be routed from the dataset it restricts (`queries/references`).
A query-declared INNER join SHALL restrict each dataset as the field filter "some row is reached
along the route from that dataset to the join's declaring dataset". A conjunct reading several
homes SHALL restrict each of them by its own disposition. A conjunct on a dataset that supplies no
rows SHALL still restrict every home it reaches. No conjunct SHALL choose the dimension anchor or
make an aggregation supply rows.

#### Scenario: One filter, two homes
- **WHEN** a query without `population` groups by `customers.region`, measures
  `orders.total_revenue` and `returns.total_returns`, and filters `orders.amount > 100`, over the
  seed of `queries/population` › Rows of an inferred query are the row supply
- **THEN** revenue is restricted inline and returns by association: EAST (200, 25), WEST
  (300, NULL)

#### Scenario: A filter on a dataset no metric is homed on
- **WHEN** the same query measures only `orders.total_revenue` and filters `visits.visit_id is
  not null`
- **THEN** the orders are restricted to customers with a visit: the only row is WEST 300

#### Scenario: A query-declared INNER join restricts each home
- **WHEN** a query without `population` declares an `inner` relationship from `customers` to a
  dataset listing C1 and C4, groups by `customers.region` and measures `orders.total_revenue` and
  `returns.total_returns`
- **THEN** the rows are EAST (300, 25) and NORTH (NULL, 10)

### Requirement: Row laws of inferred queries
Before measure-typed filters, `order` and `limit`, every query omitting `population` SHALL obey:
**measure monotonicity** — adding a measure to a query with at least one measure never removes a
row and never changes a value in an existing row, transforms included; **home-row preservation** —
every home row of a full-grain aggregate surviving the filters lands in a row, exactly one when
the home determines the dimensions and the data respects declared cardinality; **filter
non-expansion** — adding a field filter never adds a row and never changes a result key, except
the value of a computed dimension read from an aggregate over the filtered rows; and the
**unit** — a query with measures and no dimensions returns exactly one row. Filter non-expansion
and the unit SHALL hold for a query naming `population` too.

#### Scenario: Monotonicity over generated queries
- **WHEN** the law harness generates root-less queries, transforms among their measures, and adds a
  measure to each
- **THEN** every row of the smaller query is a row of the larger one with the same values

#### Scenario: Home rows are preserved
- **WHEN** the law harness generates root-less queries over data with orphan home rows
- **THEN** the sum of an additive full-grain measure over the result equals its total over the
  home's filtered rows

#### Scenario: Filters never expand
- **WHEN** the law harness adds a field filter to generated queries, with and without `population`
- **THEN** the filtered result's keys are a subset of the unfiltered result's keys, apart from the
  value of a computed dimension read from an aggregate over the filtered rows

#### Scenario: The unit
- **WHEN** a query with measures and no dimensions runs, with and without `population`, with and
  without a filter no row passes
- **THEN** it returns exactly one row

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

### Requirement: Row-level expressions are homed or refused
A row-level expression in a value position — a dimension (plain or computed), a time dimension, or a
row-level order key — SHALL be homed on the deepest dataset that determines every operand over
provably to-one hops (Axiom 2.2). Each row-level leaf is an operand at the dataset it is declared on,
or, when its definition reads across a provably fanning hop, at each dataset that definition reads a field on;
a definition reading across an unproven hop is refused, and one that cannot be analysed keeps its
resolution error; an attached aggregate or transform operand stands
for its grain members (Axiom 2.3); a literal constrains nothing, so an expression with one operand
is always homed. When no dataset determines every operand, the query SHALL fail before any SQL is
generated with the typed input-safety error, naming the position, two operands that no dataset
determines together, and the fanning or unproven hop each crosses from their common dataset, with
the remedy of aggregating one operand to a grain the other's dataset determines or of grouping by
each field as its own dimension. The refusal SHALL hold in every `to_many_handling` mode, in every
stage of a query list, under a named or an inferred population, in raw-row mode, and with or without
metrics. A row-level order key SHALL be judged before any host-grain wrap is built from it. A field
filter is not a value position: a conjunct whose operands no dataset determines keeps restricting
by association (*Filters restrict by association or fail loudly*). An operand on a dataset no join
path reaches from the population SHALL keep the product-hop type error of `queries/unrelated-datasets`,
reported ahead of this refusal. An expression whose operands a dataset determines SHALL take the
values of a column of its home, a population row with no home row reading NULL; a population that
does not determine its home resolves it per the `to_many_handling` mode, as for a column of that home.

Scenarios below run on the seed of `queries/population` › Rows of an inferred query are the row
supply: `orders` and `returns` are sibling many-to-one children of `customers`; `tickets` is
reached by no relationship.

#### Scenario: Sibling operands in a dimension are refused
- **WHEN** a query with `population: "customers"` groups by `{"field": "orders.amount * returns.amount", "alias": "x"}` and measures `count(customers.*)`, with `to_many_handling` omitted, `"associate"`, `"error"` and `"broadcast"`
- **THEN** each run fails with the input-safety error naming the dimension `x`, `orders.amount` and `returns.amount`, and the hops to `orders` and to `returns` from `customers`, and its remedy

#### Scenario: Refused whatever the query's other parts
- **WHEN** the same dimension is queried without `population`, with no metrics, with `distinct_dimension_values: false`, or in the second stage of a two-stage query list
- **THEN** each query fails with the same error

#### Scenario: Sibling operands in an order key are refused before the wrap
- **WHEN** a query with `population: "customers"` groups by `customers.region`, measures `count(customers.*)` and orders by `orders.amount * returns.amount`
- **THEN** it fails with the input-safety error naming the order key and both operands — never a `min` or `max` wrap over the pairs

#### Scenario: A constituent grained on a sibling is refused
- **WHEN** a query with `population: "customers"` groups by `orders.amount * sum(returns.amount, partition_by=[returns.return_id])`, or by `orders.amount * rank(sum(returns.amount, partition_by=[returns.return_id]), direction='desc')`
- **THEN** each query fails with the input-safety error naming `orders.amount` and the constituent's grain member `returns.return_id`

#### Scenario: The remedy is accepted
- **WHEN** the query instead groups by `{"field": "orders.amount * sum(returns.amount, partition_by=[customers.customer_id])", "alias": "x"}` and measures `count(customers.*)`
- **THEN** by executed values the cells are `x` = 2500 → 1, 5000 → 1, NULL → 3, with the `associated` warning

#### Scenario: Parent and child operands are homed on the child
- **WHEN** a query with `population: "customers"` measuring `count(customers.*)` groups by `{"field": "orders.amount * (customers.region = 'EAST')", "alias": "x"}`; or buckets `case when customers.region = 'EAST' then orders.ordered_at end` at `month`; or groups by `customers.region` and orders by `orders.amount * (customers.region = 'EAST')` descending, NULLs last
- **THEN** by executed values the dimension cells are 0 → 1, 50 → 1, 100 → 1, 200 → 1, NULL → 1; the time-dimension cells are 2025-01 → 2, 2025-02 → 1, NULL → 2, each with the `associated` warning; and the regions are ordered EAST, WEST, NORTH by the per-region `max` of the key

#### Scenario: A parent and child expression reads NULL without a home row
- **WHEN** a query with `population: "customers"` measuring `count(customers.*)` groups by `{"field": "coalesce(orders.shipped_at, customers.signup_ts)", "alias": "t"}`
- **THEN** by executed values the cells equal those of grouping by `orders.eff_col`: 2024-11-01 → 1, 2025-01-12 → 1, 2025-01-25 → 1, 2025-03-01 → 1, NULL → 1 (the customer without orders), with the `associated` warning

#### Scenario: A derived operand is judged by its closure
- **WHEN** `orders` declares `amount_x2` as `amount * 2` and a query with `population: "customers"` groups by `orders.amount_x2 * returns.amount`
- **THEN** it fails with the input-safety error naming `amount_x2` and `returns.amount`

#### Scenario: A derived operand reading its own dataset and across a non-determining hop is refused
- **WHEN** `customers` relates many-to-many to `tags` on the non-unique `segment` and declares `mix` as `region || tags.tag`, and a query with `population: "customers"` groups by `customers.mix`
- **THEN** it fails with the input-safety error naming `mix` and the hop to `tags`; a field defined as `tags.tag` alone is homed on `tags` and associates

#### Scenario: A derived operand reading across an unproven hop is refused
- **WHEN** `customers` declares a derived field reading `orders.amount` across an `orders → customers` relationship of unknown cardinality, and a query with `population: "customers"` groups by that field alone, or by `customers.region || <that field>`
- **THEN** each query fails with the input-safety error naming the field and the hop to `orders`

#### Scenario: An unproven hop determines nothing
- **WHEN** the `orders → customers` relationship declares no cardinality and `customers` declares no key, and a query with `population: "customers"` groups by `orders.amount * (customers.region = 'EAST')`
- **THEN** it fails with the input-safety error naming `orders.amount`, `customers.region` and the hop to `orders`

#### Scenario: A field filter with sibling operands still restricts by association
- **WHEN** a query with `population: "customers"` groups by `customers.region`, measures `count(customers.*)` and filters `orders.amount * returns.amount > 100`
- **THEN** by executed values the only row is EAST 1, with the semi-join informational entry

#### Scenario: An unrelated operand keeps the product-hop error
- **WHEN** a query with `population: "customers"` groups by `orders.amount * length(tickets.priority)`, buckets an expression reading `tickets` at `month`, orders by an expression reading `tickets`, or groups by an `orders` derived field whose definition reads `tickets`
- **THEN** each query fails with the product-hop type error of `queries/unrelated-datasets` naming `tickets` and `customers`, never the input-safety error

### Requirement: Declared cardinality never changes a value
Every aggregation SHALL count each home row once in each output group it matches after the
filters, whatever the data, unless every hop it is evaluated across is trusted
(`models/join-cardinality` › Trusted hops). Declared cardinality and declared keys decide
which paths exist and what a dataset determines, never how often a home row counts. An
untrusted provably to-one hop SHALL keep its typing: it determines, raises no associated-grain
warning, is accepted under `to_many_handling: "error"`, and admits `partition_by=` keys
across it. A dimension computed from fields beyond such a hop SHALL count a home row once
per distinct computed value, never once per matching row. With data satisfying every
declared cardinality, results SHALL be identical whether the hops are trusted or not.

#### Scenario: A duplicated dimension row counts once per group
- **WHEN** on the Appendix B seed with customer C1 appearing twice, both in `EAST`, and the
  `orders → customers` relationship untrusted, a query selects dimension
  `customers.region` and metric `orders.total_revenue`
- **THEN** EAST is 350 and WEST 300, on SQLite and DuckDB, with no warning

#### Scenario: A filter across a duplicated row counts once
- **WHEN** the same seed and query run with no dimensions and filter
  `customers.region = 'EAST'`
- **THEN** the result is 350

#### Scenario: The duplicated value is reachable only by trusting
- **WHEN** the same relationship declares `trusted: true`
- **THEN** EAST is 650

#### Scenario: A row in two groups counts in each
- **WHEN** customer C3 appears once in `SOUTH` and once in `WEST`, the relationship untrusted
- **THEN** `orders.total_revenue` is 300 in SOUTH and 300 in WEST

#### Scenario: Clean data does not depend on trust
- **WHEN** the unmodified Appendix B seed is queried by `customers.region` with the
  relationship trusted, and again untrusted
- **THEN** both runs return identical rows

#### Scenario: A computed dimension counts each distinct value once
- **WHEN** C1 appears once in `EAST` and once in `WEST`, untrusted, and the query groups by
  `CASE WHEN customers.region IN ('EAST', 'WEST') THEN 'coast' END`
- **THEN** each of C1's orders counts once in `coast`

#### Scenario: A filter and a dimension on one dataset bind the same row
- **WHEN** C1 appears once as (`EAST`, `gold`) and once as (`WEST`, `silver`), untrusted, and
  the query groups by `customers.region` with filter `customers.tier = 'gold'`
- **THEN** C1's orders count in EAST only

#### Scenario: Error mode accepts an untrusted declared dimension
- **WHEN** the duplicated-C1 query runs under `to_many_handling: "error"`
- **THEN** it returns EAST 350 instead of refusing

#### Scenario: Every aggregation family counts the home row once
- **WHEN** on the duplicated-C1 seed, untrusted, the query groups by `customers.region` and
  selects `count(*)`, `avg(orders.amount)`, `count_distinct(orders.order_id)`,
  `last(orders.amount)`, `collect(orders.order_id)`, a windowed and a ranked aggregate over
  `orders`, and `sum(orders.amount)` with an aggregate filter on `orders`
- **THEN** each value equals the same query over the de-duplicated seed

### Requirement: An aggregate input across an untrusted hop counts its home row once
An aggregation whose input, parameter, ranking key or aggregate-filter operand reads a
field across an untrusted provably to-one hop SHALL count each home row once, reading one
of the home row's matching values when the data holds several; which one is unspecified.
Under `to_many_handling: "error"` such an aggregation SHALL be refused with a typed error
naming the hop and the remedies: mark the relationship `trusted`, or record an enforced key.
Across trusted hops the input is read as today.

#### Scenario: A weighted sum reads one weight per order
- **WHEN** C1 appears twice with weights 1 and 3, untrusted, and the query selects
  `wsum(orders.amount, weight=customers.weight)` by `customers.region`
- **THEN** each of C1's orders contributes once, weighted by 1 or by 3

#### Scenario: Error mode refuses an input across an untrusted hop
- **WHEN** the same query runs under `to_many_handling: "error"`
- **THEN** it fails with the typed error naming `orders → customers` and both remedies

#### Scenario: A trusted hop reads the input as before
- **WHEN** the relationship declares `trusted: true` and the data holds no duplicates
- **THEN** the weighted sum equals today's value under every mode
