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
- **WHEN** a query rooted at `customers` references `billing_customer.amount:sum`
- **THEN** the same token traverses the same edge in reverse, reaching `orders`

#### Scenario: Named paths carry correct terminal metadata
- **WHEN** a named-edge path terminates in a time dimension used for ordering and
  response metadata
- **THEN** ownership, ordering, and metadata behave exactly as for the equivalent
  model-name path

#### Scenario: A named edge typed by its model name answers under the edge name
- **WHEN** `customers`→`regions` is a single edge named `hr` and a query rooted at
  `orders` selects the dimension `customers.regions.rname` and the measure
  `customers.regions.pop:max`
- **THEN** the result keys are `orders.customers.hr.rname` and
  `orders.customers.hr.pop_max`, with the same label, format and type metadata as
  the edge-name spelling

### Requirement: Edge names are validated at save time
An edge name SHALL be rejected at save when it equals any model name in the
datasource or duplicates another edge name incident to either endpoint model.
Validation SHALL warn when a parallel edge set contains any unnamed edge — an unnamed parallel edge is unaddressable. Name characters follow model-name
identifier rules.

#### Scenario: Name colliding with a model is rejected
- **WHEN** a model saves a join named like an existing model in the datasource
- **THEN** the save fails with an error naming the collision

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
Route counting, shortest-path selection, and root recommendation SHALL treat parallel
edges as distinct routes over the bidirectional edge set. A recommended path MUST be
executable: where a hop needs an edge name to be unambiguous, the recommendation
emits it, and when no unambiguous executable path exists the item is reported
unreachable rather than recommended with a path that would fail.

#### Scenario: Parallel edges count as distinct routes
- **WHEN** two edges connect a pair of models on the only route between two others
- **THEN** route counting reports the route as ambiguous, not unique

#### Scenario: Recommendation emits an executable named path
- **WHEN** root recommendation routes an item across an ambiguous pair whose edges
  are named
- **THEN** the recommended path uses the edge-name token and resolves without error

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
  the measure `customers.regions.pop:max`
- **THEN** the measure is computed per region exactly as for
  `customers.hr.pop:max`, and no broadcast warning is raised

#### Scenario: A reverse-hop route determines under a divergent spelling
- **WHEN** `regions`→`region_events` is one-to-many and a query rooted at `orders`
  selects the dimension `customers.regions.rname` and the measure
  `customers.hr.region_events.value:max`
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
