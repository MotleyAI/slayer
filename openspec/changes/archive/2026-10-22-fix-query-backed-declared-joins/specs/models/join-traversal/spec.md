## ADDED Requirements

### Requirement: An extension join repeating a stored edge is that edge

A query-level model extension's join that equals one of its base model's stored joins in every field except `description`, `meta` and pair order SHALL be that stored edge, applied once, for every model kind; an extension join differing in target, name, join type, cardinality or join pairs SHALL remain a separate edge.

#### Scenario: Repeating a stored join in an extension
- **WHEN** an extension over a table-backed model repeats a stored join and a query reads through it
- **THEN** the query resolves over that single edge instead of failing as an ambiguous hop

#### Scenario: A differing extension join stays parallel
- **WHEN** an extension adds an unnamed join to the same target with different join pairs
- **THEN** a bare hop to that target fails closed as ambiguous
