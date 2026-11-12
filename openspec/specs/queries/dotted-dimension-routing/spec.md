# queries/dotted-dimension-routing Specification

## Purpose
Auto-resolves a short-form Mode-B dotted reference (target model plus column, first hop resolving
to no incident edge) to its full datasource-scoped join path when that path is determinable, and
rejects it with a route-aware error otherwise, uniformly across every query surface.

## Requirements

### Requirement: Unique-route resolution
A dotted reference naming a target model and column whose first hop token resolves to no edge
incident to the query root (either traversal orientation, no edge-name match) SHALL resolve to the
full join path when the target is reachable by exactly one simple route through the root's
bidirectional datasource-scoped join graph — reverse-orientation hops are route candidates and
parallel edges are distinct routes. The reference MUST behave bound-tree-identically
to that full explicit path. For a dimension or time dimension, the surfaced result column key SHALL
be the full routed path, not the short form typed.

#### Scenario: Short form with one route resolves to the full path
- WHEN a query rooted at `Invoice` (chain `Invoice → Subscription → Customer → Consumer`, each a single route) selects the dimension `Consumer.name`
- THEN it produces the same SQL and values as selecting `Subscription.Customer.Consumer.name`, and the result column key is the full routed path

#### Scenario: A unique route that fans out still resolves
- WHEN the sole route to the target crosses a one-to-many hop
- THEN the short form resolves to that route (the fan-out tie-break applies only to ambiguity)

#### Scenario: A unique route through a reverse hop resolves
- WHEN the only stored edges are `Payment → Invoice` and `Payment → Method`, and a query rooted at `Invoice` selects `Method.kind`
- THEN it resolves through the reverse `Payment` hop, equivalent to the explicit `Payment.Method.kind`

### Requirement: Fan-out-free tie-break among ambiguous routes
When a short-form target is reachable by two or more routes, the reference SHALL resolve if and only
if exactly one of those routes is fan-out-free — every hop provably many-to-one or one-to-one on its
traversal orientation (declared or inverted cardinality, or the hop's traversal-target join columns
cover a unique key on the target). A hop whose oriented cardinality is undetermined and not proven
by a covering unique key is not fan-out-free; a reverse hop over a to-one declaration fans out.

#### Scenario: Exactly one fan-out-free route resolves
- WHEN two routes reach the target and only one has all-to-one hops
- THEN the reference resolves to the fan-out-free route

#### Scenario: Two fan-out-free routes stay ambiguous
- WHEN two or more routes reach the target and two of them are fan-out-free
- THEN the reference is rejected as ambiguous

#### Scenario: Named parallel edges tie-break by oriented safety
- WHEN the only route crosses a pair joined by two named edges, one fan-out-free and one fanning out
- THEN the reference resolves through the fan-out-free edge and the result key carries its edge-name token

### Requirement: Routed paths are executable
The effective routed path and any `suggested_path` SHALL be canonical token sequences per the join-traversal
rules: each hop spelled by its edge name, else its target model, and resolving to exactly the edge traversed.
A route crossing an unaddressable edge (an unnamed edge in a parallel set) is not executable and SHALL NOT be
routed or suggested; when such a pair blocks every route, the rejection SHALL name the pair and the remedy to
name its edges. A short-form target directly adjacent to the root via two or more edges SHALL keep
failing as an ambiguous join hop (`AmbiguousJoinPathError`) — routing never applies to a hop that
resolves or fails closed.

#### Scenario: Unnamed parallel pair on the only route is unroutable
- WHEN the only route to the target crosses a pair joined by two unnamed edges
- THEN the reference is rejected with no suggested path, and the error names that pair and the remedy to name its edges

#### Scenario: Adjacent parallel edges stay an ambiguous hop
- WHEN the root itself has two named edges to the target and a reference names `Target.column` (dimension or saved measure)
- THEN it fails with the ambiguous-join-hop error naming both edges, not a routing error, and schema-drift attributes the reference to nothing

#### Scenario: A suggestion through a single named edge uses the edge name
- WHEN a suggested path crosses `customers`→`regions`, a single edge named `hr`
- THEN the suggestion spells that hop `hr`, never `regions`

### Requirement: Route-aware rejection of ambiguous and unreachable targets
A short-form target that is ambiguous (two or more routes, not uniquely fan-out-free) or unreachable
SHALL be rejected with `UnresolvableDimensionJoinError`. An ambiguous rejection SHALL carry a
`suggested_path` when an executable full path exists (the shortest fan-out-free one if any, else the
shortest executable full path); among equal-length candidates the suggestion SHALL be the one with fewer hops
that are not provably to-one, then the lexicographically-smallest canonical token sequence, so it is
deterministic and independent of model load order. An ambiguous target whose only routes cross an unnamed
parallel pair, and every unreachable target, MUST carry no suggestion.

#### Scenario: Ambiguous target suggests a full path
- WHEN a short-form target is reachable by two routes with no unique fan-out-free choice
- THEN the error names the reference and root and suggests a fully-qualified path the caller can type

#### Scenario: Unreachable target has no suggestion
- WHEN a short-form target is reachable by no route in the root's datasource
- THEN the error rejects the reference with no suggested path

#### Scenario: An equal-length suggestion prefers fewer fanning hops
- WHEN no fan-out-free route reaches an ambiguous target and two shortest routes of equal length differ in how many of their hops fan out
- THEN the suggestion is the route with fewer fanning hops

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

### Requirement: Uniform application across query surfaces
Short-form routing SHALL apply identically wherever a Mode-B dotted reference appears: dimensions,
time dimensions, cross-model measures and aggregations, star aggregations, query filters, and ORDER BY.
A short-form ORDER BY entry MUST bind to the same routed value as the matching selected dimension.
Rootless population inference SHALL be a routing surface too: viability probing applies the same
short-form route enumeration per candidate root, with two inference-specific constraints — literal
resolution takes precedence over short-form reinterpretation, and a route only supports determination
when every oriented hop is provably to-one.

#### Scenario: Routing applies to a cross-model aggregation and star aggregation
- WHEN a query selects the measure `sum(Consumer.amount)` and `count(Consumer.*)` with `Consumer` uniquely routable
- THEN both resolve through the routed path and emit correct SQL

#### Scenario: Short-form ORDER BY matches its routed dimension
- WHEN a query selects the dimension `Consumer.name` and orders by `Consumer.name`
- THEN the order clause references the same column as the selected dimension

#### Scenario: Inference and binding agree on a routed short form
- WHEN a query without `source_model` uses a short-form dimension that inference resolved via a route
  from the chosen population
- THEN binding resolves the same short form through the same route, so the executed query matches the
  inference decision

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
datasource-scoped routing, so a change to the routed terminal's column or the terminal-reaching join
cascades to the stage that uses the short form, mirroring the equivalent full path exactly. An
ambiguous or unreachable short form attributes to nothing and never raises inside drift analysis.

#### Scenario: Dropping a routed column cascades
- WHEN a persisted stage uses the short form `Consumer.email` routed through `Consumer`, and `Consumer.email` is dropped
- THEN the drift cascade attributes the reference to `Consumer` and updates that stage

### Requirement: Non-routing resolution is unchanged
A directly-joined dotted path, a self-prefixed root reference, and a full explicit path SHALL resolve
exactly as before, with byte-identical result keys whenever the typed path is already canonical (every
unnamed edge, and every named edge spelled by its name). A fully valid path whose terminal column does not
exist SHALL keep failing as an unknown reference, not as a routing failure.

A reference whose leading qualifier is the query root's own name — a model's name, an extension's base
model, an inline model's name, a stage's name in its own query list, or a spliced query-backed model's name
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
- **WHEN** a query rooted at an extension of `orders`, at an inline model named `ad_hoc`, or at a
  query-backed model `qb` selects `orders.status`, `ad_hoc.status` or `qb.status` respectively
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
  and another model's `Column.sql` reference its column `customers__regions__rname`
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
