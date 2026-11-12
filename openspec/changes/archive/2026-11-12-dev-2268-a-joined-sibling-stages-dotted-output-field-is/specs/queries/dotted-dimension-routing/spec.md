## ADDED Requirements

### Requirement: A joined stage's flattened columns are read by their flat names
A stage's output column keeps its flat name when another query object joins the stage: a stage dimension
`customers.name` (flat name `customers__name`) SHALL be read through a join onto stage `s` as `s.customers__name`,
from a stored, an inline, or a sibling-stage root alike. The dotted spelling of a flattened column SHALL NOT bind to
it.

#### Scenario: A stored root reads a joined stage's flat column
- **WHEN** stage `s` over `orders` selects `customers.name` and `n = count(*)`, and a query rooted at stored
  `addresses` (or stored `regions`) joins `s` through a query extension and selects `s.customers__name`
- **THEN** it executes on SQLite and DuckDB and returns stage `s`'s `customers.name` values under the result key
  `<root>.s.customers__name`

#### Scenario: An inline root named like a stored model reads a joined stage's flat column
- **WHEN** an inline source named `customers` joins `s` through a query extension and selects `s.customers__name`
- **THEN** it executes and reads `s`'s column, never a hop back to the root

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
- **WHEN** an inline source named `customers` joins `s` through a query extension and selects `s.customers.name`
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

### Requirement: Reference errors name models as the user spells them
An error raised while resolving a bare or dotted reference SHALL name every model by the spelling the user writes
for it — a stage by its name in its query list — and SHALL NOT expose a minted stage identity.

#### Scenario: An unknown field of a joined stage names the stage
- **WHEN** a query rooted at stored `addresses` joins stage `s` and selects `s.nope`
- **THEN** the error names `s` and lists its columns, and its text contains no `__slayer_` identity

#### Scenario: An unknown field of a stage source names the stage
- **WHEN** a stage over `s` selects `nope`
- **THEN** the error names `s` and its text contains no `__slayer_` identity

## MODIFIED Requirements

### Requirement: Broken explicit chains are never auto-fixed
A dotted reference of two or more hops in which some hop token does not resolve from the preceding
model SHALL be rejected and never silently repaired into a working route. When the reference spells a flat column
of a model on its walked prefix, the flat-spelling refusal applies instead. Otherwise the error SHALL name the model
the failing hop leaves, as the user spells it, and list its columns, and SHALL suggest the short form `Target.leaf`
when that short form is itself routable, and otherwise offer no suggestion.

#### Scenario: Broken chain suggests the short form
- WHEN a query rooted at `Invoice` (no direct `Customer` join) selects `Customer.Consumer.name` and `Consumer` is uniquely routable
- THEN the reference is rejected and the error suggests `Consumer.name`

#### Scenario: A broken chain through a joined stage names the stage's columns
- **WHEN** a query rooted at stored `addresses` joins stage `s` and selects `s.customers.nope`
- **THEN** the reference is rejected and the error names `s` and lists its columns
