## MODIFIED Requirements

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
- **THEN** it is the stored edge, traversed as LEFT, and `customers.<column>` stays unambiguous

#### Scenario: Alias resolves to the stored edge under its canonical spelling
- **WHEN** an extension declares the stored `orders → customers` edge under the name `c`, and the query selects both `c.name` and `customers.region`
- **THEN** `customers` is joined once, and the result keys read `customers.name` and `customers.region`

#### Scenario: Alias selects one of two parallel stored edges
- **WHEN** `orders` stores `billing_address` and `shipping_address` edges to `addresses` on different pairs, and an extension declares the billing pairs under the name `b`
- **THEN** `b.city` reads the billing address and its result key is spelled `billing_address.city`

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
- **WHEN** extensions add `a → b` and `b → c` as new edges and a query rooted at `a` selects `b.c.<column>`, or extensions close a cycle `a → b → a` on different pairs
- **THEN** the chain resolves over the query-local edges, and the cycle's hops resolve by their names exactly as stored edges in a cycle do
