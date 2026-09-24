## Purpose

Defines how a multi-stage query list — named sibling stages plus a final root —
is ordered and validated so every stage can read the siblings it references.

## ADDED Requirements

### Requirement: Stages are ordered by every sibling reference
A multi-stage query list SHALL be ordered so each stage follows every sibling it
references — as its `source_model` (a name, or a `ModelExtension` over the
sibling), as a `joins[].target_model` of its source spec, or anywhere inside an
inline model's nested `source_queries` — regardless of the order the non-root
stages were supplied in. The last entry SHALL stay the root. A reference to a model
reached only through a stored model's own joins SHALL resolve to that model, never
to a same-named sibling stage. Duplicate stage names, an unnamed non-root stage, a
self-reference, a non-root stage referencing the root, and a cycle SHALL each be
rejected with an error naming the offending stages.

#### Scenario: A stage joining a sibling that reads another sibling
- **WHEN** the list `[x (source customers), c (source x), b (source orders joined to c), root (source b)]` is executed
- **THEN** it returns the hand-computed result instead of failing to resolve `c`

#### Scenario: Supply order of non-root stages does not matter
- **WHEN** the non-root stages of that list are supplied in any permutation, root last
- **THEN** every permutation returns the same result

#### Scenario: Sibling reached through an inline model
- **WHEN** a stage's source is an inline model whose joins, or whose nested `source_queries` at any depth, reference a sibling stage
- **THEN** that stage is ordered after the sibling and executes correctly

#### Scenario: One stage reading several siblings
- **WHEN** a stage's source is one sibling and it joins two other siblings
- **THEN** it is ordered after all three and executes correctly

#### Scenario: Stored join to a model named like a sibling
- **WHEN** a stage's source model has a stored join to model `customers` and a sibling stage is also named `customers`
- **THEN** a dotted reference through that join resolves to the `customers` model, and the stage does not depend on the sibling

#### Scenario: Malformed lists are rejected
- **WHEN** a list has an unnamed non-root stage, a stage referencing itself, a non-root stage referencing the root, or a cycle
- **THEN** it is rejected with an error naming the offending stages
