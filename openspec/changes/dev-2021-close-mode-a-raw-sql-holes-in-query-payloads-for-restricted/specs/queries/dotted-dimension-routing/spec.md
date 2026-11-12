## ADDED Requirements

### Requirement: A joined stage's flattened columns are read by their flat names from any root
A stage's output column keeps its flat name when another query object joins the stage: a stage dimension
`customers.name` (flat name `customers__name`) SHALL be read through a join onto stage `s` as `s.customers__name`,
from a stored or a sibling-stage root alike. The dotted spelling of a flattened column SHALL NOT bind to
it.

#### Scenario: A stored root reads a joined stage's flat column
- **WHEN** stage `s` over `orders` selects `customers.name` and `n = count(*)`, and a query rooted at stored
  `addresses` (or stored `regions`) joins `s` through a query extension and selects `s.customers__name`
- **THEN** it executes on SQLite and DuckDB and returns stage `s`'s `customers.name` values under the result key
  `<root>.s.customers__name`

#### Scenario: A root named like the flattened path reads a joined stage's flat column
- **WHEN** a query rooted at stored `customers` joins `s` through a query extension and selects `s.customers__name`
- **THEN** it executes and reads `s`'s column, never a hop back to the root

## MODIFIED Requirements

### Requirement: Non-routing resolution is unchanged
A directly-joined dotted path, a self-prefixed root reference, and a full explicit path SHALL resolve
exactly as before, with byte-identical result keys whenever the typed path is already canonical (every
unnamed edge, and every named edge spelled by its name). A fully valid path whose terminal column does not
exist SHALL keep failing as an unknown reference, not as a routing failure.

A reference whose leading qualifier is the query root's own name — a model's name, an extension's base
model, a stage's name in its own query list, or a spliced query-backed model's name
— SHALL resolve as the rest of the reference from that root (`orders.amount` ≡ `amount` from root
`orders`), in every Mode-B position (dimensions, time dimensions, measures and aggregation arguments,
filters, order, `main_time_dimension`, computed-dimension and expression time-dimension expressions) and
with the same SQL, values and result keys as the unqualified spelling, except that an auto-named
computed dimension or expression time dimension keeps the written qualifier in its name. The root
qualifier SHALL be read before any join hop or query-level alias, so it wins over an edge spelled the same.

#### Scenario: Existing full and self-prefixed paths are unchanged
- **WHEN** a query uses a full explicit join path or a self-prefixed root column, spelled canonically
- **THEN** resolution and result keys are identical to prior behaviour

#### Scenario: A root-qualified reference equals the bare one in every position
- **WHEN** a query rooted at `orders` selects dimension `orders.status`, time dimension
  `date_trunc('month', orders.created_at)`, measure `sum(orders.amount)`, filter `orders.amount > 5` and order
  `orders.status`, and a second query spells all of them without the `orders.` qualifier
- **THEN** both produce the same SQL, the same rows and the same result keys
  (`orders.status`, `orders.created_at`, `orders.amount_sum`)

#### Scenario: An auto-name keeps the written qualifier
- **WHEN** a query rooted at `orders` selects the unnamed computed dimension `orders.amount * 2`
- **THEN** its result key is `orders.orders_amount_2`

#### Scenario: A stage root's own name qualifies its columns
- **WHEN** a query list's stage `s1` projects `status` and `sum(amount)` named `tot`, and the final query
  over `s1` selects the computed dimension `s1.tot * 2`, the measure `sum(s1.tot)` and `count(s1.*)`
- **THEN** each equals its unqualified spelling (`tot * 2`, `sum(tot)`, `count(*)`)

#### Scenario: Every root kind takes its own name as qualifier
- **WHEN** a query rooted at `orders` that extends `orders`, or at a query-backed model `qb`, selects
  `orders.status` or `qb.status` respectively
- **THEN** each equals its query selecting `status`

#### Scenario: The root wins over an edge named like it
- **WHEN** model `orders` declares a join to `customers` named `orders`, and a query rooted at `orders`
  selects `orders.status`
- **THEN** it reads `orders`' own `status`, and the joined `customers` column is reached as
  `orders.orders.<column>`

#### Scenario: A root-qualified filter matches a selected dotted measure
- **WHEN** a query rooted at `orders` selects the saved measure `customers.aov` and filters on
  `orders.customers.aov > 10`
- **THEN** the filter reads that selected measure, exactly as the filter `customers.aov > 10` does

### Requirement: A dotted spelling of a flattened column is refused with its flat name
When a dotted reference's join walk fails — a hop token resolves from no preceding model in a chain of two or more
hops, or a hop revisits a model already on the path — and, for some model on the walked prefix (the root included),
the reference's remaining tokens joined with `__` and ending in the leaf name exactly one column of that model, the
reference SHALL fail as an unknown reference that names that model as the user spells it, lists its columns, and
suggests the flat spelling (the written prefix up to that model, then the column's declared name). The model nearest
the root wins (the longest flat name). The rule applies to every model, stage-derived or authored. Matching follows
the reference's quoting: a remaining segment written double-quoted matches exactly, and the suggestion double-quotes
the flat name in that case. A stale respelling of a column SHALL match that column and the suggestion SHALL name its
canonical name, without recording a stale-spelling warning; a name matching several columns' respellings SHALL give
no hint. A single-hop short form that routes is not a failure and is never pre-empted.

In a downstream stage's flat scope, a dotted reference whose tokens joined with `__` name a stage column SHALL keep
failing as an illegal-scope reference, its reason naming that flat column. When a row-level read of an unrelated
dataset is refused and the reference's tokens joined with `__` name a column of the population, the suggestion SHALL
name that column first, ahead of the aggregate alternative.

#### Scenario: A stored root's dotted spelling suggests the flat column
- **WHEN** a query rooted at stored `addresses` joins stage `s` and selects `s.customers.name`
- **THEN** it fails as an unknown reference naming `s`, listing `s`'s columns, and suggesting `s.customers__name`

#### Scenario: A routable target does not override the flat column
- **WHEN** a query rooted at stored `regions`, from which `customers` is routable, joins `s` and selects
  `s.customers.name`
- **THEN** the error suggests `s.customers__name` and does not suggest `customers.name`

#### Scenario: A hop back to the root is not reported as a circular join
- **WHEN** a query rooted at stored `customers` joins `s` through a query extension and selects `s.customers.name`
- **THEN** it fails as an unknown reference suggesting `s.customers__name`, not as a circular join

#### Scenario: The longest flat name wins
- **WHEN** a failed walk passes two models that each have a column spelled by the remaining tokens from that model
- **THEN** the suggestion names the column of the model nearer the root

#### Scenario: An authored model's literal flat column is suggested
- **WHEN** an authored model `m`, joined from the root, has a column literally named `a__b` and no join `a`, and a
  query selects `m.a.b`
- **THEN** it fails as an unknown reference naming `m` and suggesting `m.a__b`

#### Scenario: A stale respelling suggests the canonical column
- **WHEN** the dotted spelling joined with `__` equals a stale respelling of exactly one stage column
- **THEN** the suggestion names the canonical column and the response records no stale-spelling warning

#### Scenario: An ambiguous respelling gives no hint
- **WHEN** the dotted spelling joined with `__` equals a respelling of two stage columns
- **THEN** the reference fails with the walk's own error and no flat suggestion

#### Scenario: A downstream stage's dotted spelling names the flat column
- **WHEN** a stage over `s` selects `s.customers.name`
- **THEN** it fails as an illegal-scope reference whose reason names `customers__name`

#### Scenario: An unrelated short form suggests the population's flat column
- **WHEN** a stage over `s` selects the dimension `customers.name`, which routes to the unrelated stored `customers`
- **THEN** it fails as an unrelated-dataset read whose suggestion names `customers__name` before the aggregate form

#### Scenario: A genuine join through a stage is unchanged
- **WHEN** a stage joins a model through a query extension and a root that joins the stage reads a column through
  that join with a dotted path
- **THEN** the path resolves as join hops and the query executes

## REMOVED Requirements

### Requirement: A joined stage's flattened columns are read by their flat names
**Reason**: Its body and a scenario name an inline root, which this change removes; replaced by "A joined stage's flattened columns are read by their flat names from any root".
**Migration**: Root the query at a stored model or a stage.
