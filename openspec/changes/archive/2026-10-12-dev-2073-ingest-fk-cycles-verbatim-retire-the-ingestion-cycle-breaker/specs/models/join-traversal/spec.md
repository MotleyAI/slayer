## MODIFIED Requirements

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

## ADDED Requirements

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
