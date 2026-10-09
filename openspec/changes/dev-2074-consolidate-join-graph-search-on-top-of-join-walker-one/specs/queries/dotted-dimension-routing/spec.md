## MODIFIED Requirements

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
