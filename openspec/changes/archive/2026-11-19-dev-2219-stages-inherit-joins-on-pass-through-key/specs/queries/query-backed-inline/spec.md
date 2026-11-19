## MODIFIED Requirements

### Requirement: Saved relation relationships survive splicing

A stored query-backed model consumed as a relation SHALL retain that model's own declared relationships, with keys resolved against its final-stage output columns, alongside the edges its final stage inherits (`queries/multi-stage` › Stages inherit edges on pass-through keys), a declared relationship winning over an inherited one (`queries/multi-stage` › Declared edges win over inherited ones). Those relationships SHALL remain available when the relation is a population, a relationship target or an extended source. An ordinary query stage SHALL expose its output columns and the edges it inherits, and no other relationship of its backing source.

#### Scenario: Joined segment filter on a saved relation
- **WHEN** a saved customer-total relation declares a customer-key relationship and a query filters to gold customers
- **THEN** only the related gold customers contribute, with the same values as an equivalent relationship declared solely on a query extension, on SQLite and DuckDB

#### Scenario: Joined dimensions and aggregate attribution
- **WHEN** a query reads a dimension or aggregate through a saved relation's declared relationship
- **THEN** the relationship determines the rows and attribution instead of being treated as an unrelated dataset

#### Scenario: A saved relation consumed through another model
- **WHEN** another dataset relates to a saved query-backed relation and a query traverses onward through that relation's own relationship
- **THEN** the declared relationships remain available with logical key spelling and correct values

#### Scenario: Extending a saved relation
- **WHEN** a query extension adds a distinct named relationship to a saved relation
- **THEN** both its stored relationships and the extension's relationships remain available

#### Scenario: Repeating an existing saved relationship
- **WHEN** an extension over a saved query-backed relation repeats an exactly identical declared relationship
- **THEN** that edge is applied once, without introducing an ambiguous parallel path

#### Scenario: Legal join targets do not collide with temporary identities
- **WHEN** a saved relation declares a relationship to a legal dataset named `_stage`
- **THEN** both unjoined and joined consumers execute without a false self-join error

#### Scenario: Ordinary stage isolation
- **WHEN** an ordinary query stage is consumed by a later stage
- **THEN** its output columns and the edges its pass-through keys inherit are visible, and a relationship of its backing source whose key it does not pass through is not

#### Scenario: A saved relation inherits like a stage
- **WHEN** a saved query-backed model grouped by `orders.customer_id` declares no relationship and another query reads `customers.region` from it
- **THEN** it reaches `customers` through the inherited edge, with the values the same stages give as an explicit query list

## ADDED Requirements

### Requirement: Inspect lists a saved relation's inherited edges
`inspect` and the dataset summary of a saved query-backed model SHALL list, beside its
declared relationships, the edges its final stage inherits, each marked as inherited and
naming the source edge and the dataset it was declared on, in the same shape on MCP, REST
and the CLI. The listing SHALL be derived under the model's default variables and never
stored. When the model's stages cannot be planned — a cycle, an invalid key, a required
variable without a default, or any other planning error — inspection SHALL still succeed,
listing no inherited edge and carrying that typed error's message.

#### Scenario: Inherited edges are listed
- **WHEN** a saved query-backed model grouped by `orders.customer_id` is inspected
- **THEN** its relationships include the edge to `customers`, marked inherited from the `orders` → `customers` relationship

#### Scenario: A declared relationship absorbs the inherited one in the listing
- **WHEN** that model also declares the identical relationship to `customers`
- **THEN** inspection lists that edge once, as declared

#### Scenario: Planning failures do not fail inspection
- **WHEN** a saved query-backed model whose stages form a cycle, read an invalid key, or need a variable with no default is inspected
- **THEN** inspection succeeds, lists no inherited edge, and carries the typed error's message, on MCP, REST and the CLI alike
