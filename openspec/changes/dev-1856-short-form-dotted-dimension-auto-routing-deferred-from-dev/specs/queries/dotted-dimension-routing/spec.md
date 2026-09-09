## Purpose

Auto-resolves a short-form Mode-B dotted reference (target model plus column, first hop not a
direct join) to its full datasource-scoped join path when that path is determinable, and rejects
it with a route-aware error otherwise, uniformly across every query surface.

## ADDED Requirements

### Requirement: Unique-route resolution
A dotted reference naming a target model and column whose first hop is not a direct join of the
query root SHALL resolve to the full join path when the target is reachable by exactly one simple
route through the root's datasource-scoped join graph. The reference MUST behave bound-tree-identically
to that full explicit path. For a dimension or time dimension, the surfaced result column key SHALL
be the full routed path, not the short form typed.

#### Scenario: Short form with one route resolves to the full path
- WHEN a query rooted at `Invoice` (chain `Invoice → Subscription → Customer → Consumer`, each a single route) selects the dimension `Consumer.name`
- THEN it produces the same SQL and values as selecting `Subscription.Customer.Consumer.name`, and the result column key is the full routed path

#### Scenario: A unique route that fans out still resolves
- WHEN the sole route to the target crosses a one-to-many hop
- THEN the short form resolves to that route (the fan-out tie-break applies only to ambiguity)

### Requirement: Fan-out-free tie-break among ambiguous routes
When a short-form target is reachable by two or more routes, the reference SHALL resolve if and only
if exactly one of those routes is fan-out-free — every hop provably many-to-one or one-to-one
(declared, or the hop's target-side join columns cover a unique key on the target). A hop whose
cardinality is undetermined and not proven by a covering unique key is not fan-out-free.

#### Scenario: Exactly one fan-out-free route resolves
- WHEN two routes reach the target and only one has all-to-one hops
- THEN the reference resolves to the fan-out-free route

#### Scenario: Two fan-out-free routes stay ambiguous
- WHEN two or more routes reach the target and two of them are fan-out-free
- THEN the reference is rejected as ambiguous

### Requirement: Route-aware rejection of ambiguous and unreachable targets
A short-form target that is ambiguous (two or more routes, not uniquely fan-out-free) or unreachable
SHALL be rejected with `UnresolvableDimensionJoinError`. An ambiguous rejection MUST carry a
`suggested_path` (the shortest fan-out-free full path if one exists, else the shortest full path).
An unreachable rejection MUST carry no suggestion.

#### Scenario: Ambiguous target suggests a full path
- WHEN a short-form target is reachable by two routes with no unique fan-out-free choice
- THEN the error names the reference and root and suggests a fully-qualified path the caller can type

#### Scenario: Unreachable target has no suggestion
- WHEN a short-form target is reachable by no route in the root's datasource
- THEN the error rejects the reference with no suggested path

### Requirement: Broken explicit chains are never auto-fixed
A dotted reference of two or more hops in which some hop is not a direct join of the preceding model
SHALL be rejected and never silently repaired into a working route. The error SHALL suggest the short
form `Target.leaf` when that short form is itself routable, and otherwise offer no suggestion.

#### Scenario: Broken chain suggests the short form
- WHEN a query rooted at `Invoice` (no direct `Customer` join) selects `Customer.Consumer.name` and `Consumer` is uniquely routable
- THEN the reference is rejected and the error suggests `Consumer.name`

### Requirement: Uniform application across query surfaces
Short-form routing SHALL apply identically wherever a Mode-B dotted reference appears: dimensions,
time dimensions, cross-model measures and aggregations, star aggregations, query filters, and ORDER BY.
A short-form ORDER BY entry MUST bind to the same routed value as the matching selected dimension.

#### Scenario: Routing applies to a cross-model aggregation and star aggregation
- WHEN a query selects the measure `Consumer.amount:sum` and `Consumer.*:count` with `Consumer` uniquely routable
- THEN both resolve through the routed path and emit correct SQL

#### Scenario: Short-form ORDER BY matches its routed dimension
- WHEN a query selects the dimension `Consumer.name` and orders by `Consumer.name`
- THEN the order clause references the same column as the selected dimension

### Requirement: Datasource-scoped and deferred past stage boundaries
Route candidates SHALL be drawn only from the query root's datasource; a target in another datasource
is never a candidate. Routing SHALL apply on a base model scope; a dotted reference in a downstream
named-query stage (a flat stage schema) SHALL keep failing as an illegal cross-stage reference rather
than routing.

#### Scenario: Cross-datasource target is not a candidate
- WHEN a same-named model exists in another datasource
- THEN it is not considered a route candidate for the root's short forms

#### Scenario: Downstream stage dotted reference stays illegal
- WHEN a downstream stage references a dotted path across a stage boundary
- THEN it fails as an illegal-scope reference, not as a routed dimension

### Requirement: Saved-measure short forms surface under their routed name and type
A short-form dotted reference to a saved measure on a routable target SHALL resolve, surface (when
unnamed) under the full routed path as its implicit name, and inherit the saved measure's declared
type with the usual precedence.

#### Scenario: Routed saved measure keeps type and full-path name
- WHEN a query rooted at `Invoice` selects `Consumer.aov` (a typed saved measure on the uniquely-routable `Consumer`) without a name
- THEN the result column key is the full routed path and the column carries the saved measure's declared type

### Requirement: Schema-drift tracks routed references
Schema-drift attribution SHALL resolve a persisted short-form reference through the same
datasource-scoped routing, so a change to the routed terminal's column or an intervening join cascades
to the stage that uses the short form. An ambiguous or unreachable short form attributes to nothing
and never raises inside drift analysis.

#### Scenario: Dropping a routed column cascades
- WHEN a persisted stage uses the short form `Consumer.email` routed through `Consumer`, and `Consumer.email` is dropped
- THEN the drift cascade attributes the reference to `Consumer` and updates that stage

### Requirement: Non-routing resolution is unchanged
A directly-joined dotted path, a self-prefixed root reference, and a full explicit path SHALL resolve
exactly as before, with byte-identical result keys. A fully valid path whose terminal column does not
exist SHALL keep failing as an unknown reference, not as a routing failure.

#### Scenario: Existing full and self-prefixed paths are unchanged
- WHEN a query uses a full explicit join path or a self-prefixed root column
- THEN resolution and result keys are identical to prior behaviour
