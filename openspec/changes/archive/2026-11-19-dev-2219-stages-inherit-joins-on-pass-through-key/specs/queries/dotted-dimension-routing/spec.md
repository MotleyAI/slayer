## MODIFIED Requirements

### Requirement: Datasource-scoped and deferred past stage boundaries
Route candidates SHALL be drawn only from the query root's datasource; a target in another datasource
is never a candidate. Routing SHALL apply on every dataset scope, a later stage reading a stage included:
its candidates are the stage's edges (`queries/multi-stage` › Stages inherit edges on pass-through keys)
and the edges onward from their targets, and never a sibling stage the routing stage does not name.

#### Scenario: Cross-datasource target is not a candidate
- WHEN a same-named model exists in another datasource
- THEN it is not considered a route candidate for the root's short forms

#### Scenario: Downstream stage dotted reference stays illegal
- WHEN a downstream stage references a dotted path across a stage boundary that no edge of the upstream stage reaches
- THEN it fails as the same reference fails from a stored root, not as a routed dimension

#### Scenario: Routing through an inherited edge
- WHEN a stage reads a stage grouped by `orders.customer_id`, and `customers` relates to `regions`, and it selects the short form `regions.name`
- THEN it resolves to `customers.regions.name` through the inherited edge, and the result key is that full path


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

A later stage reading a stage SHALL follow the same rules as a stored root (`queries/multi-stage` › A stage is read as
the dataset its schema defines): a dotted reference that no edge or route resolves fails as it would from a stored root,
its suggestion naming the stage's flat column when the tokens joined with `__` name one. When a row-level read of an unrelated
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
- **WHEN** a stage over `s`, which projects `customers.name` but no key of `customers`, selects `s.customers.name`
- **THEN** it fails as it would from a stored root, its suggestion naming `customers__name`; when `s` also projects `customers.customer_id`, the reference resolves through the inherited edge instead

#### Scenario: An unrelated short form suggests the population's flat column
- **WHEN** a stage over `s` selects the dimension `customers.name`, which routes to the unrelated stored `customers`
- **THEN** it fails as an unrelated-dataset read whose suggestion names `customers__name` before the aggregate form

#### Scenario: A genuine join through a stage is unchanged
- **WHEN** a stage joins a model through a query extension and a root that joins the stage reads a column through
  that join with a dotted path
- **THEN** the path resolves as join hops and the query executes
