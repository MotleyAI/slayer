## MODIFIED Requirements

### Requirement: Edge names disambiguate paths
A join MAY declare a `name`. A segment of a reference's explicit route — any segment after its anchor
(`queries/references`) — or of a model-SQL path that matches an incident edge's name SHALL traverse that edge
from either endpoint, whether or not the pair is ambiguous. An edge name SHALL NOT be a query reference's first
segment. Result columns and metadata SHALL carry the route's canonical spelling (the edge name for a named
edge, whichever spelling was typed), and downstream consumers (response metadata, time-dimension ownership,
ordering, filters, multi-stage scopes) SHALL resolve the traversal target from the resolved edge, never by
reading path tokens as model names.

#### Scenario: Named hop resolves an ambiguous pair
- **WHEN** the two `orders`↔`customers` edges are named `billing_customer` and
  `shipping_customer` and a query selects `orders.billing_customer.name`
- **THEN** it traverses the billing edge and the result key is
  `orders.billing_customer.name`

#### Scenario: Names are direction-agnostic
- **WHEN** a query with population `customers` references `sum(customers.billing_customer.amount)`
- **THEN** the same token traverses the same edge in reverse, reaching `orders`

#### Scenario: Named paths carry correct terminal metadata
- **WHEN** a named-edge path terminates in a time dimension used for ordering and
  response metadata
- **THEN** ownership, ordering, and metadata behave exactly as for the equivalent
  model-name path

#### Scenario: A named edge typed by its model name answers under the edge name
- **WHEN** `customers`→`regions` is a single edge named `hr` and a query with population
  `orders` selects the dimension `customers.regions.rname` and the metric
  `max(customers.regions.pop)`
- **THEN** the result keys are `customers.hr.rname` and `customers.hr.pop_max`, with the
  same label, format and type metadata as the edge-name spelling

### Requirement: Paths resolve to one canonical spelling
Every resolved join path SHALL be spelled canonically: per hop, the traversed edge's
declared name, else its traversal-target model's name. Query references, saved
measures, routed anchors, model SQL (`DatasetField.sql`, `DatasetField.filter`, model
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
- **WHEN** a query rooted at `orders` selects `regions.rname`, whose anchor is reached
  uniquely through the `hr` edge
- **THEN** the result key is `regions.rname`, and the join path is spelled `customers.hr`

#### Scenario: Spelling twins share one cached result
- **WHEN** the result cache is enabled and the same query is executed once with
  `customers.regions.rname` and once with `customers.hr.rname`
- **THEN** the database is queried once, and both responses carry the same columns
  and metadata

### Requirement: An extension join repeating a stored edge is that edge

A join declared by a query extension of model `M` towards `T` SHALL be matched against every edge between `M` and `T` in the query object's join graph. That covers stored edges declared on either model, a stored query-backed model's declared joins, and edges added by the query's other extensions. Each candidate SHALL be compared in the `M → T` orientation: an edge declared `T → M` has its key pairs swapped and its cardinality inverted.
- **Named match.** A declaration whose name equals a candidate's name SHALL be that edge. Different key pairs or a conflicting cardinality SHALL be an error naming the edge.
- **Unique pair match.** Otherwise, a declaration whose key-pair set equals exactly one candidate's SHALL be that edge, applied once. A declared name SHALL then be a query-local alias of that edge, resolving within the declaring query object only. Paths, result keys and metadata SHALL keep the edge's canonical spelling: its stored name, else its target model.
- **Ambiguous pair match.** Several candidates with the declaration's key-pair set SHALL be an error naming every candidate.
- **No match.** A declaration matching no candidate SHALL be a new query-local edge.
- **Join type.** The declared join type SHALL take no part in this identity and SHALL NOT change how an edge is traversed: a matched edge keeps its stored join type, and a new query-local edge is traversed as LEFT. A declaration's description and meta SHALL be ignored on a match.
- **Cardinality.** Cardinalities conflict only when both are set and differ after orientation. A matched edge keeps its stored cardinality.
- **Names.** A declared name equal to a stored edge name of the datasource that does not belong to the matched edge SHALL be an error. A new query-local edge's name, and an alias, SHALL follow the stored edge-name rules: it SHALL NOT equal a model name of the datasource or a stage name in scope. A declared name equal to the target model's own name SHALL be accepted as that model's spelling.
- **One relationship declared twice.** Declarations of one new relationship from both sides, or twice from one side, SHALL be one edge: unnamed, or carrying the one name declared. Two different names for one new relationship SHALL be an error.
- **Parallel new edges.** A new unnamed edge parallel to another edge between the same models SHALL leave a bare hop between them failing closed as ambiguous.

#### Scenario: Repeating a stored join in an extension
- **WHEN** an extension over a table-backed model repeats a stored join and a query reads through it
- **THEN** the query resolves over that single edge instead of failing as an ambiguous hop

#### Scenario: A differing extension join stays parallel
- **WHEN** an extension adds an unnamed join to the same target with different join pairs
- **THEN** a bare hop to that target fails closed as ambiguous

#### Scenario: A reversed declaration is the stored edge
- **WHEN** only `orders → customers` on `customer_id = id` is stored and a query extends `customers` with a join to `orders` on `id = customer_id`
- **THEN** the declaration is the stored edge: one join is emitted, and `customers.orders` traverses it

#### Scenario: Declared join type does not split the edge
- **WHEN** the stored `orders → customers` edge is LEFT and an extension of `orders` declares it INNER with the same pairs
- **THEN** it is the stored edge, traversed as LEFT, and `customers.<field>` stays unambiguous

#### Scenario: Alias resolves to the stored edge under its canonical spelling
- **WHEN** an extension declares the stored `orders → customers` edge under the name `c`, and the query selects both `c.name` and `customers.region`
- **THEN** `customers` is joined once, and the result keys are `orders.customers.name` and `customers.region`

#### Scenario: Alias selects one of two parallel stored edges
- **WHEN** `orders` stores `billing_address` and `shipping_address` edges to `addresses` on different pairs, and an extension declares the billing pairs under the name `b`
- **THEN** `b.city` reads the billing address and its result key is spelled `orders.billing_address.city`

#### Scenario: Several candidates with equal pairs are ambiguous
- **WHEN** two stored edges between `orders` and `addresses` share one key-pair set and an unnamed extension join declares that set
- **THEN** the query fails naming both edges

#### Scenario: Named declaration with different pairs is an error
- **WHEN** an extension declares a join named `billing_address` on pairs that differ from the stored `billing_address` edge
- **THEN** the query fails naming `billing_address`

#### Scenario: Conflicting cardinality is an error
- **WHEN** the stored `orders → customers` edge is `many_to_one` and an extension of `customers` declares the reverse join as `many_to_one`
- **THEN** the query fails naming the edge and both cardinalities

#### Scenario: Alias equal to another edge's name is an error
- **WHEN** an extension matches the shipping edge by its pairs but names it `billing_address`
- **THEN** the query fails naming `billing_address`

#### Scenario: Alias equal to a model or stage name is an error
- **WHEN** an extension matches the stored `orders → customers` edge by its pairs but names it after another model or a stage in scope
- **THEN** the query fails naming the alias

#### Scenario: Two names for one new relationship
- **WHEN** an extension of `a` declares a new join to `b` named `x`, and an extension of `b` declares the reverse join named `y`
- **THEN** the query fails naming both names

#### Scenario: Multi-hop and cyclic query-local chains
- **WHEN** extensions add `a → b` and `b → c` as new edges and a query rooted at `a` selects `b.c.<field>`, or extensions close a cycle `a → b → a` on different pairs
- **THEN** the chain resolves over the query-local edges, and the cycle's hops resolve by their names exactly as stored edges in a cycle do

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
- **THEN** the emitted path is the item as written, resolving through `to_b`, never the fanning reverse traversal of `to_a`

#### Scenario: An unaddressable pair is reported, not silently dropped
- **WHEN** `orders` declares two unnamed joins to `addresses` and root recommendation is given `orders.id`
  and `addresses.city`
- **THEN** no path is emitted through that pair and the response warnings name `orders`↔`addresses` and the
  remedy to name the edges

#### Scenario: Routes never cross the spine
- **WHEN** `orders` and `returns` are each wired to the time spine by their axis and share no join
- **THEN** `returns` is unreachable from `orders` on every route-emitting surface

## REMOVED Requirements

### Requirement: Directed FK cycles keep literal-edge-first resolution
**Reason**: A reference's first segment names a dataset, reached by its only to-one path (`queries/references`), as Ossie Layer 3 §4 does; a direct edge has no precedence. Inside a directed FK cycle the to-one path is taken.
**Migration**: Spell the direct edge as an explicit route from the population (`x.z.amount`). Stored queries are rewritten to that spelling on load, and a fresh query whose meaning changed carries a typed notice until SLayer 3.0.

## ADDED Requirements

### Requirement: Directed FK cycles take the to-one path
Inside a directed FK cycle a reference anchored at a model of the cycle SHALL take the population's only to-one
path to it, even when a fanning edge connects the population to it directly; the direct edge is reached only
by spelling it as an explicit route. Population inference over a directed cycle SHALL fail closed when no
candidate is uniquely best.

#### Scenario: The anchor takes the to-one path
- **WHEN** `x → y → z → x` are the FK edges and a query with population `x` sums `z.amount`
- **THEN** it sums over the `z` reached through `x → y → z`, and `sum(x.z.amount)` sums over the `z` rows referencing each `x`

#### Scenario: Rootless query across a directed 3-cycle fails closed
- **WHEN** a query without a population selects `x.v`, `y.v` and `z.v` over `x → y → z → x`
- **THEN** it fails with the no-unique-candidate error, and succeeds once a population is given
