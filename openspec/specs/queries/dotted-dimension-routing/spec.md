# queries/dotted-dimension-routing Specification

## Purpose
Auto-resolves a short-form Mode-B dotted reference (target model plus column, first hop resolving
to no incident edge) to its full datasource-scoped join path when that path is determinable, and
rejects it with a route-aware error otherwise, uniformly across every query surface.

## Requirements

### Requirement: Unique-route resolution
A dotted reference whose anchor (its first segment, `queries/references`) is a dataset other than the
query's population SHALL resolve to the full join path from the population to the anchor, followed by the
reference's own explicit route, when that path is determinable by the rule of `queries/references` › The
anchor is reached by its only to-one path. Route candidates are drawn from the population's bidirectional
datasource-scoped join graph: reverse-orientation hops are route candidates, parallel edges are distinct
routes, and a relationship incident to the population is a route like any other, with no precedence. The
reference MUST behave bound-tree-identically to the full explicit path from the population. Its result key
SHALL follow `queries/references` › Result keys never depend on the population: the anchor as written,
never the routed path.

#### Scenario: Short form with one route resolves to the full path
- WHEN a query with population `Invoice` (chain `Invoice → Subscription → Customer → Consumer`, each a single route) selects the dimension `Consumer.name`
- THEN it produces the same SQL and values as selecting `Invoice.Subscription.Customer.Consumer.name`, and the result column key is `Consumer.name`

#### Scenario: A unique route that fans out still resolves
- WHEN the sole route to the anchor crosses a one-to-many hop
- THEN the reference resolves to that route (the fan-out tie-break applies only to ambiguity)

#### Scenario: A unique route through a reverse hop resolves
- WHEN the only stored edges are `Payment → Invoice` and `Payment → Method`, and a query with population `Invoice` selects `Method.kind`
- THEN it resolves through the reverse `Payment` hop, equivalent to the explicit `Invoice.Payment.Method.kind`

#### Scenario: A direct relationship has no precedence
- WHEN `Invoice` joins `Customer` directly and also through `Subscription`, every hop many-to-one, and a query with population `Invoice` selects `Customer.name`
- THEN it fails as ambiguous, suggesting `Invoice.Customer.name`

### Requirement: Fan-out-free tie-break among ambiguous routes
When an anchor is reachable from the population by two or more routes, the reference SHALL resolve if and
only if exactly one of those routes is fan-out-free — every hop provably many-to-one or one-to-one on its
traversal orientation (declared or inverted cardinality, or the hop's traversal-target join columns
cover a unique key on the target). A hop whose oriented cardinality is undetermined and not proven
by a covering unique key is not fan-out-free; a reverse hop over a to-one declaration fans out. A route
consisting of one relationship incident to the population is judged like every other route.

#### Scenario: Exactly one fan-out-free route resolves
- WHEN two routes reach the anchor and only one has all-to-one hops
- THEN the reference resolves to the fan-out-free route

#### Scenario: Two fan-out-free routes stay ambiguous
- WHEN two or more routes reach the anchor and two of them are fan-out-free
- THEN the reference is rejected as ambiguous

#### Scenario: Named parallel edges tie-break by oriented safety
- WHEN the only route crosses a pair joined by two named edges, one fan-out-free and one fanning out
- THEN the reference resolves through the fan-out-free edge

### Requirement: Routed paths are executable
The effective routed path and any `suggested_path` SHALL be canonical token sequences per the join-traversal
rules, starting at the population: each hop spelled by its edge name, else its target model, and resolving to
exactly the edge traversed. A route crossing an unaddressable edge (an unnamed edge in a parallel set) is not
executable and SHALL NOT be routed or suggested; when such a pair blocks every route, the rejection SHALL name
the pair and the remedy to name its edges. An anchor directly adjacent to the population via two or more edges
SHALL be judged by the routing rule like any other set of routes.

#### Scenario: Unnamed parallel pair on the only route is unroutable
- WHEN the only route to the anchor crosses a pair joined by two unnamed edges
- THEN the reference is rejected with no suggested path, and the error names that pair and the remedy to name its edges

#### Scenario: Adjacent parallel edges stay an ambiguous hop
- WHEN the population has two named many-to-one edges to the anchor and a reference names `Target.field` (dimension or saved metric)
- THEN it fails as ambiguous, suggesting the spelling through each edge name, and schema-drift attributes the reference to nothing

#### Scenario: A suggestion through a single named edge uses the edge name
- WHEN a suggested path crosses `customers`→`regions`, a single edge named `hr`
- THEN the suggestion spells that hop `hr`, never `regions`

### Requirement: Route-aware rejection of ambiguous and unreachable targets
A reference whose anchor is ambiguous (two or more routes, not uniquely fan-out-free) SHALL be rejected with
`UnresolvableDimensionRelationshipError`; an anchor with no route is an unrelated dataset (`queries/unrelated-datasets`).
An ambiguous rejection SHALL carry a `suggested_path` when an executable full path exists (the shortest
fan-out-free one if any, else the shortest executable full path), spelled from the population; among
equal-length candidates the suggestion SHALL be the one with fewer hops that are not provably to-one, then the
lexicographically-smallest canonical token sequence, so it is deterministic and independent of model load
order. An ambiguous anchor whose only routes cross an unnamed parallel pair MUST carry no suggestion.

#### Scenario: Ambiguous target suggests a full path
- WHEN an anchor is reachable by two routes with no unique fan-out-free choice
- THEN the error names the reference and the population and suggests a full path starting at the population that the caller can type

#### Scenario: Unreachable target has no suggestion
- WHEN an anchor has no route from the population and the reference reads a row-level field of it
- THEN it fails as an unrelated-dataset read (`queries/unrelated-datasets`) with no suggested path

#### Scenario: An equal-length suggestion prefers fewer fanning hops
- WHEN no fan-out-free route reaches an ambiguous anchor and two shortest routes of equal length differ in how many of their hops fan out
- THEN the suggestion is the route with fewer fanning hops

### Requirement: Broken explicit chains are never auto-fixed
A dotted reference whose explicit route after its anchor has a hop token that does not resolve from the
preceding model SHALL be rejected and never silently repaired into a working route. When the reference spells
a flat column of a model on its walked prefix, the flat-spelling refusal applies instead. Otherwise the error
SHALL name the model the failing hop leaves, as the user spells it, and list its columns, and SHALL suggest
`Target.leaf` when that reference resolves by the routing rule, and otherwise offer no suggestion.

#### Scenario: Broken chain suggests the short form
- WHEN `Invoice` has no direct `Customer` join and a query with population `Invoice` selects `Invoice.Customer.Consumer.name`, and `Consumer` is uniquely routable
- THEN the reference is rejected and the error suggests `Consumer.name`

#### Scenario: A broken chain through a joined stage names the stage's columns
- **WHEN** a query with population stored `addresses` joins stage `s` and selects `s.customers.nope`
- **THEN** the reference is rejected and the error names `s` and lists its columns

### Requirement: Uniform application across query surfaces
Anchor routing SHALL apply identically wherever a Mode-B dotted reference appears: dimensions, time
dimensions, metrics and aggregations, star aggregations, query filters, and ORDER BY. An ORDER BY entry MUST
bind to the same routed value as the matching selected dimension. Population inference SHALL apply the same
routing rule per candidate population, with one inference-specific constraint: a route only supports
determination when every oriented hop is provably to-one. A relationship incident to a candidate SHALL have no
precedence over other routes in inference either.

#### Scenario: Routing applies to a cross-model aggregation and star aggregation
- WHEN a query measures `sum(Consumer.amount)` and `count(Consumer.*)` with `Consumer` uniquely routable from the population
- THEN both resolve through the routed path and emit correct SQL

#### Scenario: Short-form ORDER BY matches its routed dimension
- WHEN a query selects the dimension `Consumer.name` and orders by `Consumer.name`
- THEN the order clause references the same column as the selected dimension

#### Scenario: Inference and binding agree on a routed short form
- WHEN a query without a population uses a dimension that inference resolved via a route from the chosen population
- THEN binding resolves the same reference through the same route, so the executed query matches the inference decision

### Requirement: Datasource-scoped and deferred past stage boundaries
Route candidates and anchors SHALL be drawn only from the population's datasource; a dataset in another
datasource is never an anchor or a route candidate. Routing SHALL apply on every dataset scope, a later stage
reading a stage included: the stage's name SHALL anchor its flat columns (`s1.tot`), its candidates are the
stage's edges (`queries/multi-stage` › Stages inherit edges on pass-through keys) and the edges onward from their
targets, and never a sibling stage the routing stage does not name.

#### Scenario: Cross-datasource target is not a candidate
- WHEN a same-named model exists in another datasource
- THEN it is not considered an anchor or a route candidate

#### Scenario: A stage's name anchors its columns
- WHEN a later stage over stage `s1` selects `s1.tot`
- THEN it reads `s1`'s column `tot`

#### Scenario: Downstream stage dotted reference stays illegal
- WHEN a downstream stage references a dotted path across a stage boundary that no edge of the upstream stage reaches
- THEN it fails as the same reference fails from a stored root, not as a routed dimension

#### Scenario: Routing through an inherited edge
- WHEN a stage reads a stage grouped by `orders.customer_id`, and `customers` relates to `regions`, and it selects `regions.name`
- THEN it reads `regions` through the inherited edge to `customers`, and the result key is `regions.name`

### Requirement: Saved-measure short forms surface under their routed name and type
A dataset-metric reference whose anchor is reached by routing SHALL resolve, surface (when unaliased) under the
key `<anchor>.<metric>`, and inherit the saved metric's declared type with the usual precedence.

#### Scenario: Routed saved measure keeps type and full-path name
- WHEN a query with population `Invoice` selects `Consumer.aov` (a typed saved metric on the uniquely-routable `Consumer`) without an alias
- THEN the result column key is `Consumer.aov` and the column carries the saved metric's declared type

### Requirement: Schema-drift tracks routed references
Schema-drift attribution SHALL resolve a persisted short-form reference through the same
datasource-scoped routing, so a change to the routed terminal's column or the terminal-reaching join
cascades to the stage that uses the short form, mirroring the equivalent full path exactly. An
ambiguous or unreachable short form attributes to nothing and never raises inside drift analysis.

#### Scenario: Dropping a routed column cascades
- WHEN a persisted stage uses the short form `Consumer.email` routed through `Consumer`, and `Consumer.email` is dropped
- THEN the drift cascade attributes the reference to `Consumer` and updates that stage

### Requirement: Stale path spellings resolve across stage boundaries
A flat name referenced across a stage boundary — by a downstream stage of a multistage query, by a query
against a query-backed model, or by another model's SQL referencing a query-backed model's columns — that
matches no column exactly SHALL bind to the upstream column whose name it would be under a non-canonical
spelling of that column's path (a named hop spelled by its target model), provided exactly one column
matches. Each such binding SHALL surface one typed `STALE_PATH_SPELLING` normalization warning per
referencing position, naming the stale and the canonical name. An exact name always wins; a name matching
several columns' respellings SHALL fail as an unknown reference.

#### Scenario: A downstream stage written against the old spelling keeps working
- **WHEN** a multistage query's first stage selects `customers.regions.rname` (now answered as
  `customers__hr__rname` downstream) and its second stage references `customers__regions__rname`
- **THEN** the reference binds to `customers__hr__rname`, the results equal the canonical spelling's, and
  the response carries a `STALE_PATH_SPELLING` warning naming both names

#### Scenario: A stale spelling survives a chain of stages
- **WHEN** a three-stage query's first stage selects `customers.regions.rname` and the second and third
  stages both reference `customers__regions__rname`
- **THEN** both bind to `customers__hr__rname`, each with a `STALE_PATH_SPELLING` warning

#### Scenario: A query-backed model's consumer written against the old spelling keeps working
- **WHEN** a query-backed model's query selects `customers.regions.rname`, and both a query on that model
  and another model's `DatasetField.sql` reference its column `customers__regions__rname`
- **THEN** both resolve to the canonical column with a `STALE_PATH_SPELLING` warning

#### Scenario: An edge named later does not break an existing reference
- **WHEN** a downstream reference `customers__regions__rname` was written while the `customers`→`regions`
  edge was unnamed, and the edge is later named `hr`
- **THEN** the reference still resolves, with a `STALE_PATH_SPELLING` warning

#### Scenario: An ambiguous stale spelling fails closed
- **WHEN** a stale flat name matches the respelling of two different upstream columns
- **THEN** the reference fails as an unknown reference

#### Scenario: A cached result carries the requesting query's warnings
- **WHEN** a result is served from the cache for a query that uses a stale spelling after the canonical
  spelling populated the cache (or the reverse)
- **THEN** the response carries exactly the requesting query's `STALE_PATH_SPELLING` warnings

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

### Requirement: Reference errors name models as the user spells them
An error raised while resolving a bare or dotted reference SHALL name every model by the spelling the user writes
for it — a stage by its name in its query list — and SHALL NOT expose a minted stage identity.

#### Scenario: An unknown field of a joined stage names the stage
- **WHEN** a query rooted at stored `addresses` joins stage `s` and selects `s.nope`
- **THEN** the error names `s` and lists its columns, and its text contains no `__slayer_` identity

#### Scenario: An unknown field of a stage source names the stage
- **WHEN** a stage over `s` selects `nope`
- **THEN** the error names `s` and its text contains no `__slayer_` identity

### Requirement: A joined stage's flattened columns are read by their flat names from any root
A stage's output column keeps its flat name when another query object joins the stage: a stage dimension
`customers.name` (flat name `customers__name`) SHALL be read through a join onto stage `s` as `s.customers__name`,
from a stored or a sibling-stage population alike, under the result key `s.customers__name`. The dotted spelling
of a flattened column SHALL NOT bind to it.

#### Scenario: A stored root reads a joined stage's flat column
- **WHEN** stage `s` over `orders` selects `customers.name` and `{"metric": "count(orders.*)", "alias": "n"}`, and a query with population stored `addresses` (or stored `regions`) joins `s` through a query extension and selects `s.customers__name`
- **THEN** it executes on SQLite and DuckDB and returns stage `s`'s `customers.name` values under the result key `s.customers__name`

#### Scenario: A root named like the flattened path reads a joined stage's flat column
- **WHEN** a query with population stored `customers` joins `s` through a query extension and selects `s.customers__name`
- **THEN** it executes and reads `s`'s column under the result key `s.customers__name`, never a hop back to the population
