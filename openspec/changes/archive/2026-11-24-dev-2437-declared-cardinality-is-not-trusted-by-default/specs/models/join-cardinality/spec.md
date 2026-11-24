## MODIFIED Requirements

### Requirement: Provable to-one arity per orientation
A join hop SHALL count as provably many-to-one iff, on the hop's *traversal
orientation*, (a) the traversal-target-side join columns cover a declared primary-key
or unique set of the traversal-target model (structural proof), or (b) the oriented
cardinality — the declared label when traversed as declared, its inverse when
traversed in reverse — is `many_to_one` or `one_to_one`. Provable arity decides which
paths exist, what a dataset determines, attribution, warnings and modes; it SHALL NOT
by itself change how a value is computed (`queries/semantics` › Declared cardinality
never changes a value). An undeclared, unproven orientation is unproven and MUST be
treated as unsafe (fail-closed). Both orientations of every declared edge participate in
safety; proof is per orientation, so one direction of an edge may be provably to-one
while the other fans out.

#### Scenario: Structural proof from a covered primary key
- WHEN a join's target-side columns cover the target model's declared primary key
- THEN the hop is provably many-to-one with no cardinality declaration needed

#### Scenario: Unknown arity fails closed
- WHEN a join has no cardinality declaration and its target-side columns cover no declared unique set
- THEN the engine treats the hop as unsafe for value paths (metrics resolve across it per `to_many_handling` rather than join through it)

#### Scenario: Inverted declared one-to-many proves many-to-one
- WHEN the only stored edge is `customers → orders (one_to_many)` and a hop traverses
  `orders → customers`
- THEN the oriented cardinality is `many_to_one` and dimensions across that hop keep
  exact attributed values instead of resolving per `to_many_handling`

#### Scenario: Inverting a to-one hop yields fan-out
- WHEN a hop traverses `customers → orders` over a stored
  `orders → customers (many_to_one)` edge whose source columns cover no unique set of
  `orders`
- THEN that orientation is one-to-many, is not provably to-one, and metrics crossing
  it resolve per `to_many_handling`

#### Scenario: A declaration alone never skips de-duplication
- WHEN `orders → customers` is declared `many_to_one`, `customers` declares primary key
  `customer_id`, neither is trusted, and customer C1 appears twice in the data
- THEN the hop is still provably many-to-one for paths and determination, and
  `orders.total_revenue` by `customers.region` counts each order once in EAST

## ADDED Requirements

### Requirement: Trusted hops
A provably to-one hop SHALL be **trusted** iff, in that orientation, (a) its relationship
declares `trusted: true`, (b) its traversal-target-side fields cover a key set the target
records as enforced by the database (`enforced_keys`), or (c) its target key is unique by
construction: a stage's complete grain, the time spine's instant, a keyless home's
per-query row number, or a list child's hop to its own parent row. `trusted` covers every
orientation of its relationship that is provably to-one, by declared cardinality or by a
covered declared key; on a relationship with no provably to-one orientation it has no
effect and SHALL NOT be refused. A stage edge inherited on pass-through keys SHALL carry
the trust of the edge it derives from. Only a trusted hop MAY be evaluated assuming its
declared cardinality; on data violating a trusted relationship the result is undefined.
`trusted` defaults to `false` and is accepted on stored and query-local relationships.

#### Scenario: A trusted relationship keeps the inline join
- WHEN `orders → customers` is declared `many_to_one` with `trusted: true` and the data
  holds no duplicate customers
- THEN `orders.total_revenue` by `customers.region` returns the same values as without
  the flag, and its SQL joins `customers` inline

#### Scenario: Trust covers a key-proven orientation
- WHEN a relationship has no cardinality, its target-side fields cover the target's
  declared primary key, and it declares `trusted: true`
- THEN the hop is trusted

#### Scenario: Trust covers the to-one reverse of a one-to-many declaration
- WHEN the only stored edge is `customers → orders (one_to_many)` with `trusted: true`
- THEN the hop `orders → customers` is trusted

#### Scenario: A vacuous trusted flag is accepted and inert
- WHEN a `many_to_many` relationship whose columns cover no key on either side declares
  `trusted: true`
- THEN the model saves and every query across it evaluates exactly as without the flag

#### Scenario: An enforced target key is trusted
- WHEN `customers.enforced_keys` lists `[customer_id]` and `orders → customers` joins on it
- THEN the hop is trusted without a flag on the relationship

#### Scenario: A stage grain is trusted
- WHEN a later stage joins a sibling stage on its complete grain
- THEN the hop is trusted

#### Scenario: A list position alone is not a constructed key
- WHEN a hop's target-side fields cover a list child's position but not its parent row
- THEN the hop is not trusted by construction
