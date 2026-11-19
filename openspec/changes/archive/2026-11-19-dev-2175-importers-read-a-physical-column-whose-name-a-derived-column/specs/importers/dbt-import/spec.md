## ADDED Requirements

### Requirement: dbt entity keys read physical columns

An entity whose `expr` is a bare column (or which has no `expr`) SHALL key its semantic model's joins and primary key on a physical reader of that column (importers/meaning-preservation › One hidden reader per physical column), whichever dimensions the semantic model declares; when a dimension holds the column's name with a different definition, the dimension SHALL keep its name and definition, the key SHALL be a hidden field reading the physical column, and no report SHALL be raised.

#### Scenario: A target key named by a derived dimension
- **WHEN** semantic model `customers` has primary entity `id` and declares dimension `id` with `expr: LOWER(id)`, and `orders` has foreign entity `customer` joining it
- **THEN** `orders` imports with a relationship to `customers` keyed on a hidden field `id_raw` that reads the physical `id` and carries the primary key, `customers.id` keeps `LOWER(id)` and is not the primary key, and no warning is raised

#### Scenario: A source key named by a derived dimension
- **WHEN** semantic model `orders` has foreign entity `customer` with `expr: customer_id` and declares dimension `customer_id` with `expr: UPPER(customer_id)`
- **THEN** the relationship is keyed on a hidden field reading the physical `customer_id`, and grouping orders by `customers.name` returns each customer's order count
