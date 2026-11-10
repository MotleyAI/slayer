## MODIFIED Requirements

### Requirement: Every datasource has a virtual time spine model

Every datasource SHALL expose a model named `time_spine` with exactly one column, `timestamp`, of type `time`, which is its primary key and denotes every instant. The model SHALL NOT be stored; it resolves in the datasource a query runs against. It SHALL appear in model listings, `inspect` and search as a built-in model, and its description SHALL list the models wired to it with their axis columns. Saving a user model named `time_spine` SHALL be rejected with a typed error; if a stored model already has that name, any query referencing `time_spine` SHALL fail with a typed error naming the conflict, never silently using either model.

#### Scenario: Spine is listed with its wired models

- **WHEN** the models of the scenario datasource are listed and `time_spine` is inspected
- **THEN** `time_spine` appears as a built-in model with the single column `timestamp`, and its inspection names `orders` via `order_date` and `returns` via `return_date`

#### Scenario: Reserved name rejected on save

- **WHEN** a user saves a model named `time_spine`
- **THEN** the save fails with a typed error naming the reserved model and nothing is stored

#### Scenario: Pre-existing stored clash fails closed

- **WHEN** a model named `time_spine` is already stored in a datasource and a query there groups by `time_spine.timestamp` month
- **THEN** the query fails with a typed error naming the clash and asking to rename the stored model
