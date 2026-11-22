## MODIFIED Requirements

### Requirement: Edge names are validated at save time
An edge name SHALL be rejected at save when it duplicates another edge name incident to either endpoint model. An edge name MAY equal a model name: a hop segment SHALL resolve as "A hop segment naming both an edge and another edge's target is ambiguous" specifies. Validation SHALL warn when a parallel edge set contains any unnamed edge — an unnamed parallel edge is unaddressable. Name characters follow model-name identifier rules.

#### Scenario: Name colliding with a model is rejected
- **WHEN** a model saves a join named like an existing model in the datasource, and no other edge of the model targets that model
- **THEN** the save succeeds, and a path segment with that name resolves to the named edge

#### Scenario: Model named like an existing edge is rejected
- **WHEN** `orders` stores an edge named `returns` targeting `refunds`, and a new model `returns` is saved in the same datasource
- **THEN** the save succeeds, `orders.returns` resolves to the edge into `refunds`, and `returns` is a dataset anchor

#### Scenario: Unnamed parallel edges warn
- **WHEN** a save leaves parallel edges between one pair of models and at least one is unnamed
- **THEN** validation warns that the unnamed edge cannot be addressed (bare token ambiguous)

#### Scenario: Duplicate incident edge names are rejected
- **WHEN** `orders` declares two edges named `buyer`
- **THEN** the save fails with an error naming the duplicate

## ADDED Requirements

### Requirement: A hop segment naming both an edge and another edge's target is ambiguous
When a path segment after a dataset names an edge of that dataset and is also the name of the target dataset of a different edge of that dataset, the reference SHALL fail with a typed ambiguity error naming both readings and the remedy of spelling the hop by the other edge's name. A segment naming only one of them SHALL resolve to it.

#### Scenario: Both readings exist
- **WHEN** `orders` has an edge named `customers` targeting `regions` and an edge named `orders_to_customers` targeting `customers`, and a query reads `orders.customers.name`
- **THEN** the query fails with a typed ambiguity error naming the edge `customers` and the edge `orders_to_customers`

### Requirement: Self-relationships are accepted and take no part in paths
A relationship whose target is its own dataset SHALL be accepted at save and load. It SHALL take no part in route search, path resolution, association or population inference. A reference that spells a hop over a self-relationship SHALL fail with a typed error stating that self-relationships are not traversable.

#### Scenario: A self-relationship loads and stays inert
- **WHEN** `employees` declares the relationship `manager` from `manager_id` to its own `id`, and a query asks for `count(employees.*)` by `employees.department`
- **THEN** the dataset saves and the query returns each department's employee count

#### Scenario: Spelling the self-relationship fails
- **WHEN** a query reads `employees.manager.name`
- **THEN** the query fails with a typed error stating that self-relationships are not traversable
