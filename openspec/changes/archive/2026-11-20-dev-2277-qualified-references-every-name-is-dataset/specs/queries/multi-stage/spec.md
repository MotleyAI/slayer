## MODIFIED Requirements

### Requirement: Stages are ordered by every sibling reference
A multi-stage query list SHALL be ordered so each stage follows every sibling it
references — as its `population`, as an extension's `dataset`, as the `to` of one
of its extensions' relationships, or as a dataset named by the first segment of any reference in any of its
expressions (dimensions, metrics, filters, order) — regardless of the order or the
names of the non-root stages. The last entry SHALL stay the root. A stage's join graph
SHALL contain exactly the sibling stages it references in one of these ways and the siblings
its inherited edges target, so no route passes through or into a sibling the stage neither
names nor reaches by an inherited edge. Duplicate stage names,
an unnamed non-root stage, a self-reference, a non-root stage referencing the root, and
a cycle SHALL each be rejected with an error naming the offending stages.

#### Scenario: A stage joining a sibling that reads another sibling
- **WHEN** the list `[x (population customers), c (population x), b (population orders, extended with a relationship to c), root (population b)]` is executed
- **THEN** it returns the hand-computed result instead of failing to resolve `c`

#### Scenario: Supply order of non-root stages does not matter
- **WHEN** the non-root stages of that list are supplied in any permutation, root last
- **THEN** every permutation returns the same result

#### Scenario: Sibling reached through an inline model
- **WHEN** a stage's source is an inline model whose relationships reference a sibling stage
- **THEN** the list is rejected at construction with the inline-model error, and the sibling is reached instead through an extension of a named source relating to it

#### Scenario: Sibling referenced inside nested `source_queries`
- **WHEN** a stage's source is an inline model carrying `source_queries` (which could only reach a sibling through nested `source_queries`)
- **THEN** the query is rejected at construction with the inline-model error, with a hint to write those queries as named stages

#### Scenario: One stage reading several siblings
- **WHEN** a stage's population is one sibling and its extensions relate to two other siblings, or extend a third
- **THEN** it is ordered after all of them and executes correctly

#### Scenario: Malformed lists are rejected
- **WHEN** a list has an unnamed non-root stage, a stage referencing itself, a non-root stage referencing the root, or a cycle
- **THEN** it is rejected with an error naming the offending stages

#### Scenario: A sibling named only in an expression
- **WHEN** a non-root stage `a_reader` (population `customers`) measures `max(z_ev.ne)` of a sibling `z_ev` (population `events`, metric `count(*)` aliased `ne`) that it reaches by no relationship, and the same list is run again with the stages named `z_reader` and `a_ev`
- **THEN** both lists execute and return the same hand-computed value

#### Scenario: A relationship named like a sibling is not a sibling reference
- **WHEN** stage `a` (population `orders`) groups by `orders.ship_region.name` through `orders`' relationship `ship_region`, and a sibling stage named `ship_region` reads `a`
- **THEN** the list executes, and `a` is not ordered after `ship_region`

#### Scenario: A first segment naming a sibling reads the sibling
- **WHEN** that stage `a` groups by `ship_region.name` instead
- **THEN** `a` reads the sibling `ship_region`, and the list is rejected as a cycle

#### Scenario: A route hop is no sibling reference
- **WHEN** a stage over `customers` measures `max(customers.z_ev.ne)` and a sibling is named `z_ev`
- **THEN** `z_ev` is a hop of the route from the anchor `customers`, not a sibling the stage reads

#### Scenario: An inherited edge reaches its sibling target
- **WHEN** stage `keyed` inherits its extension's relationship `totals_link` to sibling `totals`, and the root reads `keyed.totals_link.total` from `keyed` without naming `totals`
- **THEN** the root returns the hand-computed values

#### Scenario: Routing never passes through an unnamed sibling
- **WHEN** a sibling stage inherits edges to `customers` and `regions`, and a later stage over `customers` that never names that sibling selects `regions.name`
- **THEN** the sibling is not a route candidate: the reference resolves as it would without the sibling
