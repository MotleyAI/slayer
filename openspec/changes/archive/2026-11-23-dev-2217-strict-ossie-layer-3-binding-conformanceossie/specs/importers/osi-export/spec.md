## MODIFIED Requirements

### Requirement: Relationships

Each join SHALL export as one relationship whose `from` is the many side and `to` is the one side, keyed by field names: a many-to-one or one-to-one join from its model to its target, and a one-to-many join from its target to its model with its key pairs swapped. Each join SHALL export as its own relationship, also when another join relates the same datasets on the same keys, so that the document holds one relationship per edge native traversal sees. A join whose cardinality is undetermined SHALL export only when the declared keys prove a direction (the target side's keys are its primary key or a unique key, or else the source side's keys are), and SHALL otherwise be reported with the workaround of setting the join's cardinality. Many-to-many joins SHALL be reported. A relationship's `name` SHALL be the join's name, else a generated name; generated names SHALL be unique in the document.

#### Scenario: Many-to-one join
- **WHEN** `orders` joins `customers` many-to-one on `[customer_id, id]`
- **THEN** the document has a relationship `from: orders`, `to: customers`, `from_columns: [customer_id]`, `to_columns: [id]`

#### Scenario: One-to-many join
- **WHEN** `customers` joins `orders` one-to-many on `[id, customer_id]` and `orders` declares no join to `customers`
- **THEN** the document has a relationship `from: orders`, `to: customers`, `from_columns: [customer_id]`, `to_columns: [id]`

#### Scenario: A named mirror exports as a second relationship
- **WHEN** `orders` joins `customers` many-to-one on `[customer_id, id]` and `customers` declares the join `placed_orders` to `orders` one-to-many on `[id, customer_id]`
- **THEN** the document has two relationships `from: orders`, `to: customers` on those keys, one of them named `placed_orders`

#### Scenario: Undetermined cardinality proven by keys
- **WHEN** a join with no cardinality targets `customers` on `customers.id`, its primary key
- **THEN** it exports as a relationship to `customers`

#### Scenario: Undetermined cardinality not provable
- **WHEN** a join with no cardinality keys on columns that are a primary or unique key on neither side
- **THEN** no relationship is exported for it and the report suggests setting its cardinality
