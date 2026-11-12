## MODIFIED Requirements

### Requirement: A model that reads an invisible model is invisible

A model SHALL read every model that one of the following references, and every model on the path of such a reference:

- the source of each stage of a query-backed model (a model name or an extension's base);
- the targets of extension joins;
- every dotted reference path in its stages' fields: dimensions, time dimensions, measures, filters and order;
- every dotted reference path in its own definitions: column SQL, column filters, model filters, saved measure formulas (including saved measures they reference), and aggregation parameters.

Paths SHALL be resolved against the full store, in which a stored model that cannot be loaded resolves as a model with no columns or joins; a path reaching it SHALL read it and every model before it. A bare model name that several datasources hold, none of them preferred, SHALL read every holder. A path that does not resolve even in the full store SHALL add no read. Declaring a join SHALL NOT by itself count as reading its target.

#### Scenario: A query-backed model on a hidden model is hidden

- **WHEN** a `{fin}` caller lists a store holding a query-backed model whose stage reads `hr`
- **THEN** that model is absent

#### Scenario: Dependents are hidden transitively

- **WHEN** a query-backed model reads a query-backed model that reads `hr`
- **THEN** both are absent for a `{fin}` caller

#### Scenario: A stage that only hops through a hidden model hides its model

- **WHEN** a query-backed model's stage source is `orders` and its only reference to `hr` is a dimension `orders.hr.dept`
- **THEN** the model is absent for a `{fin}` caller

#### Scenario: A column reading through a hidden join hides its model

- **WHEN** an untagged model has a column, a saved measure, or a model filter that reads `hr` through a join
- **THEN** the model is absent for a `{fin}` caller

#### Scenario: A stage order key alone hides its model

- **WHEN** a query-backed model's only reference to `hr` is in a stage's `order`
- **THEN** the model is absent for a `{fin}` caller

#### Scenario: A model reading an unloadable model is hidden

- **WHEN** an untagged model has a column that reads, through a join, a stored model that cannot be loaded
- **THEN** the model is absent for a non-bypass caller

#### Scenario: An ambiguous bare name reads every holder

- **WHEN** a memory's only link is a query whose source is `hr`, and two datasources each hold an `hr` hidden from `{fin}`
- **THEN** the memory is absent for a `{fin}` caller

#### Scenario: A declared but unused join does not hide

- **WHEN** an untagged model declares a join to `hr` and nothing in it reads through that join
- **THEN** the model is visible to a `{fin}` caller
