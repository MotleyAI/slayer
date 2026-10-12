## MODIFIED Requirements

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
