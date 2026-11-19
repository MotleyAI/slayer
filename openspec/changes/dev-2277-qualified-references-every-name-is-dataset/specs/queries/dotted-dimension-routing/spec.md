## MODIFIED Requirements

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
datasource is never an anchor or a route candidate. Routing SHALL apply on a base model scope. In a later stage
whose population is a stage, the stage's name SHALL anchor its flat columns (`s1.tot`); a further dotted path
past a stage column SHALL keep failing as an illegal cross-stage reference rather than routing.

#### Scenario: Cross-datasource target is not a candidate
- WHEN a same-named model exists in another datasource
- THEN it is not considered an anchor or a route candidate

#### Scenario: A stage's name anchors its columns
- WHEN a later stage over stage `s1` selects `s1.tot`
- THEN it reads `s1`'s column `tot`

#### Scenario: Downstream stage dotted reference stays illegal
- WHEN a downstream stage references a dotted path past a stage column
- THEN it fails as an illegal-scope reference, not as a routed dimension

### Requirement: Saved-measure short forms surface under their routed name and type
A dataset-metric reference whose anchor is reached by routing SHALL resolve, surface (when unaliased) under the
key `<anchor>.<metric>`, and inherit the saved metric's declared type with the usual precedence.

#### Scenario: Routed saved measure keeps type and full-path name
- WHEN a query with population `Invoice` selects `Consumer.aov` (a typed saved metric on the uniquely-routable `Consumer`) without an alias
- THEN the result column key is `Consumer.aov` and the column carries the saved metric's declared type

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

## REMOVED Requirements

### Requirement: Non-routing resolution is unchanged
**Reason**: A reference is anchored at its first segment (`queries/references`). The query root's own name is now an ordinary anchor with an empty path, not a qualifier that drops out, and a bare name is a metric, so "bare ≡ root-qualified" and the root-relative keys this requirement pinned no longer hold.
**Migration**: Write every field qualified by its dataset (`orders.status`). In a query that names its population, a bare field still converts to `<population>.<field>` with a deprecation warning until SLayer 3.0. Result keys of the population's own fields are unchanged; see `queries/references` › Result keys never depend on the population.
