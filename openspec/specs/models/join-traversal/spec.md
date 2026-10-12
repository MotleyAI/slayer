# models/join-traversal Specification

## Purpose
Defines how declared joins are traversed: every join is a symmetric edge usable in
either direction with oriented pairs and cardinality, ambiguity fails closed, and
optional edge names disambiguate parallel edges in path references.

## Requirements

### Requirement: Joins traverse in both directions
A declared join SHALL be a symmetric edge between its two models: path resolution,
reachability, reroot correlation, and SQL join building MUST traverse it from either
endpoint. Traversal against the declared direction orients the edge — join pairs swap
and the cardinality label inverts (`many_to_one` ↔ `one_to_many`; `one_to_one`,
`many_to_many`, and unset are self-inverse). Which model stores the declaration MUST
NOT affect any traversal answer. A graph holding only forward declarations SHALL
answer every reachability, traversal, and value question the previously mirrored graph
answered.

#### Scenario: Reverse dimension path resolves
- **WHEN** the only stored edge is `orders → customers (many_to_one)` and a query
  rooted at `customers` selects the dimension `orders.status`
- **THEN** the path resolves over the inverted edge as a `one_to_many` hop and the
  query executes

#### Scenario: LEFT joins are traversable in reverse
- **WHEN** the stored edge is a default (LEFT) join, which was never mirrored
- **THEN** reverse traversal works exactly as for an INNER edge

#### Scenario: Mirror parity
- **WHEN** a stored mirror pair is reduced to its forward half
- **THEN** every previously supported query over the mirrored direction returns
  identical results

### Requirement: Join type is root-relative
Emitted join type SHALL be interpreted relative to the traversal direction: a LEFT
edge keeps the querying root's rows whole whichever side is the root; an INNER edge
restricts to matched pairs whichever side is the root. A RIGHT join MUST never be
emitted.

#### Scenario: Reverse LEFT keeps the root side whole
- **WHEN** a query rooted at `customers` traverses a LEFT `orders → customers` edge
  in reverse
- **THEN** the SQL joins `LEFT` from customers — customers without orders stay in the
  result and orders without customers do not appear

#### Scenario: Reverse INNER restricts to matched pairs
- **WHEN** a query rooted at `customers` traverses an INNER `orders → customers` edge
  in reverse
- **THEN** customers with no orders are excluded, matching the behavior the mirrored
  INNER edge produced

### Requirement: Ambiguous hops fail closed in both directions
When two or more edges connect the same pair of models, a bare model-name hop between
them SHALL fail — in the forward direction as much as the reverse — with an error
naming every candidate edge (its declaring model, name if any, join pairs, and
cardinality) and the remediation. Silent first-match selection MUST NOT occur.

#### Scenario: Parallel forward edges no longer resolve by first match
- **WHEN** `orders` declares two joins to `customers` (billing and shipping) and a
  query references `orders.customers.name`
- **THEN** the query fails with an error naming both candidate edges, instead of
  silently using whichever is stored first

#### Scenario: Ambiguous reverse hop names the candidates
- **WHEN** only those two forward edges exist and a query rooted at `customers`
  references `orders.status`
- **THEN** the query fails with the same error form naming both candidate edges

### Requirement: Edge names disambiguate paths
A join MAY declare a `name`. A path segment matching an incident edge's name SHALL
traverse that edge from either endpoint, whether or not the pair is ambiguous, in
model SQL and query references alike. Result columns and metadata SHALL carry the
path's canonical spelling (the edge name for a named edge, whichever spelling was
typed), and downstream consumers (response metadata, time-dimension ownership,
ordering, filters, multi-stage scopes) SHALL resolve the traversal target from the
resolved edge, never by reading path tokens as model names.

#### Scenario: Named hop resolves an ambiguous pair
- **WHEN** the two `orders`↔`customers` edges are named `billing_customer` and
  `shipping_customer` and a query selects `orders.billing_customer.name`
- **THEN** it traverses the billing edge and the result key is
  `orders.billing_customer.name`

#### Scenario: Names are direction-agnostic
- **WHEN** a query rooted at `customers` references `sum(billing_customer.amount)`
- **THEN** the same token traverses the same edge in reverse, reaching `orders`

#### Scenario: Named paths carry correct terminal metadata
- **WHEN** a named-edge path terminates in a time dimension used for ordering and
  response metadata
- **THEN** ownership, ordering, and metadata behave exactly as for the equivalent
  model-name path

#### Scenario: A named edge typed by its model name answers under the edge name
- **WHEN** `customers`→`regions` is a single edge named `hr` and a query rooted at
  `orders` selects the dimension `customers.regions.rname` and the measure
  `max(customers.regions.pop)`
- **THEN** the result keys are `orders.customers.hr.rname` and
  `orders.customers.hr.pop_max`, with the same label, format and type metadata as
  the edge-name spelling

### Requirement: Edge names are validated at save time
An edge name SHALL be rejected at save when it equals any model name in the
datasource or duplicates another edge name incident to either endpoint model.
Saving a model whose name equals an edge name stored in its datasource SHALL be
rejected likewise — edge names and model names share one path-segment namespace.
Validation SHALL warn when a parallel edge set contains any unnamed edge — an unnamed parallel edge is unaddressable. Name characters follow model-name
identifier rules.

#### Scenario: Name colliding with a model is rejected
- **WHEN** a model saves a join named like an existing model in the datasource
- **THEN** the save fails with an error naming the collision

#### Scenario: Model named like an existing edge is rejected
- **WHEN** `orders` stores an edge named `returns` and a new model `returns` is saved in the same
  datasource
- **THEN** the save fails with an error naming the edge and the model that declares it

#### Scenario: Unnamed parallel edges warn
- **WHEN** a save leaves parallel edges between one pair of models and at least one is unnamed
- **THEN** validation warns that the unnamed edge cannot be addressed (bare token ambiguous)

### Requirement: Exact-inverse declarations are redundant
Declaring a join that is the exact inverse of an existing edge on the counterpart
model (swapped pair set, same join type, equal edge name — including both unset —
and cardinalities consistent under inversion or unset on one side) SHALL be rejected
at save time — traversal is automatic; a name held by only one half makes the pair
NOT an exact inverse, so a named reverse declaration is a legal disambiguator and
migration keeps both halves (every declared token stays resolvable). Stored
exact-inverse pairs from past mirroring SHALL be deduplicated by a one-time
migration on load: the surviving half is the to-one declaration where cardinality
says which that is, else the half carrying cardinality, else a deterministic
tiebreak; the outcome MUST NOT depend on which model loads first, and repeated loads
change nothing. A stored pair that is not an exact inverse SHALL be kept whole and
fail closed at traversal as an ambiguous hop.

#### Scenario: Mirrored pair collapses to one edge
- **WHEN** storage holds `orders → customers (INNER, many_to_one)` and its mirror
  `customers → orders (INNER, one_to_many)` with swapped pairs
- **THEN** after load exactly one edge remains and both traversal directions still
  work

#### Scenario: Drifted mirror pair fails closed
- **WHEN** the two halves disagree on join pairs after one was edited
- **THEN** both edges survive migration and traversing between the models fails
  closed naming both

#### Scenario: Newly declared exact inverse is rejected
- **WHEN** a model declares a join that exactly inverts an existing counterpart edge
- **THEN** the save fails, stating that reverse traversal is automatic

### Requirement: Route enumeration is edge-aware
Route counting SHALL treat parallel edges as distinct routes over the bidirectional edge set, whether or
not each edge is addressable. Every surface that emits a route — shortest-path selection, root
recommendation, short-form routing and its suggestions, population inference, the facade catalog, model
conversion — SHALL emit only executable paths: each hop spelled canonically (its edge name, else its target
model) and resolving to exactly the edge traversed. An edge whose canonical token resolves to more than one
edge (an unnamed edge in a parallel set) is unaddressable: route emission SHALL skip it and the surface
SHALL report each skipped pair with the remedy to name its edges — an item reachable only across such a pair
is reported unreachable, never silently and never with a path that would fail. Among routes of equal hop
count, the route with fewer hops that are not provably to-one SHALL be chosen, then the lexicographically
smallest canonical token sequence; a surface that prefers fan-out-free routes searches provably to-one hops
first. No search SHALL route through the time spine: a route may begin or end there, never cross it.

#### Scenario: Parallel edges count as distinct routes
- **WHEN** two edges connect a pair of models on the only route between two others
- **THEN** route counting reports the route as ambiguous, not unique

#### Scenario: Recommendation emits an executable named path
- **WHEN** root recommendation routes an item across an ambiguous pair whose edges
  are named
- **THEN** the recommended path uses the edge-name token and resolves without error

#### Scenario: A single named edge is emitted by its name
- **WHEN** `customers`→`regions` is a single edge named `hr` and root recommendation is given the items
  `orders.id` and `regions.rname`
- **THEN** the recommended path for `regions.rname` is `customers.hr.rname`, not `customers.regions.rname`

#### Scenario: Equal-length routes prefer the to-one hop
- **WHEN** `a` declares `to_b` (`a`→`b`, many-to-one) and `b` declares `to_a` (`b`→`a`, many-to-one), and
  root recommendation attaches a measure on `b` to the root `a`
- **THEN** the emitted path is `to_b`, never the fanning reverse traversal of `to_a`

#### Scenario: An unaddressable pair is reported, not silently dropped
- **WHEN** `orders` declares two unnamed joins to `addresses` and root recommendation is given `orders.id`
  and `addresses.city`
- **THEN** no path is emitted through that pair and the response warnings name `orders`↔`addresses` and the
  remedy to name the edges

#### Scenario: Routes never cross the spine
- **WHEN** `orders` and `returns` are each wired to the time spine by their axis and share no join
- **THEN** `returns` is unreachable from `orders` on every route-emitting surface

### Requirement: Both orientations are visible on inspection surfaces
Model inspection and search surfaces SHALL list reverse-reachable neighbors — edges
declared on other models that reach the inspected model — with the cardinality
oriented for traversal from the inspected side, so a consumer can discover every
traversable hop.

#### Scenario: Inspect shows an incoming edge
- **WHEN** only `orders → customers (many_to_one)` is stored and `customers` is
  inspected
- **THEN** the rendering lists the `orders` hop as reachable with `one_to_many`
  orientation

### Requirement: Paths resolve to one canonical spelling
Every resolved join path SHALL be spelled canonically: per hop, the traversed edge's
declared name, else its traversal-target model's name. Query references, saved
measures, auto-routed short forms, model SQL (`Column.sql`, `Column.filter`, model
filters) and definition defaults SHALL all resolve to that spelling, so two spellings
of one edge denote one value: one join, one slot, one home, and identical results.
Persisted models SHALL keep their authored spelling; error messages SHALL quote the
typed spelling.

#### Scenario: Mixed spellings of one edge join once
- **WHEN** a query rooted at `orders` filters on `customers.regions.rname` and selects
  the dimension `customers.hr.rname` (`hr` names the `customers`→`regions` edge)
- **THEN** the generated SQL joins `regions` exactly once and the filter and the
  dimension reference the same value

#### Scenario: An aggregate is grouped by a dimension spelled through the other name
- **WHEN** a query rooted at `orders` selects the dimension `customers.hr.rname` and
  the measure `max(customers.regions.pop)`
- **THEN** the measure is computed per region exactly as for
  `max(customers.hr.pop)`, and no broadcast warning is raised

#### Scenario: A reverse-hop route determines under a divergent spelling
- **WHEN** `regions`→`region_events` is one-to-many and a query rooted at `orders`
  selects the dimension `customers.regions.rname` and the measure
  `max(customers.hr.region_events.value)`
- **THEN** the measure is computed per region and joined back exactly as when both
  are spelled `customers.hr`, and no broadcast warning is raised

#### Scenario: Two spellings of one dimension equal a duplicate
- **WHEN** a query selects both `customers.hr.rname` and `customers.regions.rname`
- **THEN** it behaves exactly as a query selecting `customers.hr.rname` twice

#### Scenario: Model SQL spelled by the model name meets the edge name
- **WHEN** a derived column on `orders` has `sql` `customers.regions.rname` and a
  query selects it together with the dimension `customers.hr.rname`
- **THEN** the generated SQL joins `regions` exactly once

#### Scenario: A definition default spelled by the model name resolves canonically
- **WHEN** a custom aggregation on `orders` has a parameter default
  `customers.regions.pop` and a query groups by `customers.hr.rname`
- **THEN** the default resolves to the same value and home as `customers.hr.pop`

#### Scenario: An auto-routed short form through a named edge is canonical
- **WHEN** a query rooted at `orders` selects the short form `regions.rname`, which
  routes uniquely through the `hr` edge
- **THEN** the result key is `orders.customers.hr.rname`

#### Scenario: Spelling twins share one cached result
- **WHEN** the result cache is enabled and the same query is executed once with
  `customers.regions.rname` and once with `customers.hr.rname`
- **THEN** the database is queried once, and both responses carry the same columns
  and metadata

### Requirement: Join mutations address one edge
Model-editing surfaces SHALL address a join by its edge reference — the target model's name when it is
the only join from the model to that target, else the edge name — or by an exact reference
(`target_model`, `name`, `join_pairs`). A reference matching several joins SHALL fail with an error
listing the candidates; it MUST NOT act on the first match or on every match. A join upsert SHALL
update the join matching its name, else its target and key pairs, else the sole join to its target;
an unnamed upsert matching none of these whose target the model already joins SHALL be rejected,
asking for a name.

#### Scenario: Remove one of two parallel edges
- **WHEN** `orders` joins `addresses` twice, named `billing_address` and `shipping_address`, and
  `edit_model` removes the join `billing_address`
- **THEN** only the billing edge is removed

#### Scenario: Ambiguous target reference fails
- **WHEN** `orders` holds two unnamed joins to `addresses` and `edit_model` removes the join `addresses`
- **THEN** the edit fails listing both candidates with their key pairs, and no join is removed

#### Scenario: Name an unnamed parallel edge
- **WHEN** an upsert carries `target_model` `addresses`, the key pairs of one of two unnamed joins to
  `addresses`, and `name` `billing_address`
- **THEN** that join receives the name and the other join is unchanged

#### Scenario: Exact reference removes exactly one edge
- **WHEN** `edit_model` removes `{target_model: addresses, join_pairs: [[shipping_address_id, id]]}`
  from a model with two unnamed joins to `addresses`
- **THEN** only the matching join is removed

#### Scenario: Unnamed second join is rejected
- **WHEN** `orders` already joins `addresses` twice and an upsert adds an unnamed join to `addresses`
  with new key pairs
- **THEN** the edit fails asking for an edge name

### Requirement: Edge names are visible on inspection and wire surfaces
Model inspection SHALL show each edge's name. The models summary SHALL list each one-hop neighbour by
the token that addresses it, marking a pair no token can address as ambiguous. The wire-facade catalog
SHALL expose every incident edge in both orientations with its name, and a client SQL JOIN whose ON
columns match an incident edge in either orientation SHALL traverse that edge under its canonical
spelling.

#### Scenario: Inspect shows edge names
- **WHEN** a model with named parallel edges is inspected
- **THEN** each join row shows its edge name

#### Scenario: Models summary lists addressable tokens
- **WHEN** `orders` has edges named `billing_address` and `shipping_address` to `addresses`
- **THEN** the summary lists `billing_address` and `shipping_address` as `orders`' hops, not
  `addresses`

#### Scenario: Reverse-direction SQL JOIN matches the declared edge
- **WHEN** a client sends SQL from `customers` LEFT JOINed to an `orders` subquery on
  `orders.customer_id = customers.id`, and only `orders → customers` is declared
- **THEN** the translation traverses the declared edge in reverse, with no dynamic join added

### Requirement: Directed FK cycles keep literal-edge-first resolution
Inside a directed FK cycle a path token SHALL bind the incident edge it names even when a to-one route
to the same model exists; the route is reached only by spelling it. Population inference over a
directed cycle SHALL fail closed when every candidate root is blocked by a fanning literal edge.

#### Scenario: Token binds the direct fanning edge
- **WHEN** `x → y → z → x` are the FK edges and a query rooted at `x` sums `z.amount`
- **THEN** it sums over the `z` rows referencing each `x`, not the `z` reached via `y`

#### Scenario: Rootless query across a directed 3-cycle fails closed
- **WHEN** a query without `source_model` selects `x.v`, `y.v` and `z.v` over `x → y → z → x`
- **THEN** it fails with the no-viable-candidate error, and succeeds once `source_model` is given
