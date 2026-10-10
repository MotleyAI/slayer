## ADDED Requirements

### Requirement: A join drop addresses exactly one edge
Explicit validation SHALL report a dropped join by its edge reference (the target model's name when it
is the only join from the model to that target, else the edge name) and SHALL additionally carry an
exact reference (`target_model`, `name`, `join_pairs`) for every dropped join. Applying the drift SHALL
remove exactly the joins whose live keys or target are gone, never another join to the same target.
The exact references SHALL be accepted unchanged by the model-editing surface.

#### Scenario: Dropping one FK column keeps the parallel edge
- **WHEN** `orders` joins `addresses` on `billing_address_id` and on `shipping_address_id`, the live
  `billing_address_id` column is dropped, and the drift is applied
- **THEN** only the billing join is removed and the shipping join remains

#### Scenario: Non-parallel join drops report the target as before
- **WHEN** the only join from `orders` to `customers` loses its live key column
- **THEN** the drift entry's join list reports `customers`

#### Scenario: Exact references replay through model editing
- **WHEN** a drift entry's removal, serialised as JSON, is passed unchanged to the model-editing
  surface
- **THEN** exactly the reported joins are removed

#### Scenario: A query-backed stage cascades only on the edges it walks
- **WHEN** a query-backed stage over `orders` reads `shipping_address.city` (or `orders.shipping_address.city`)
  and the billing join is dropped
- **THEN** that model is kept, while a stage reading `billing_address.city` is removed

#### Scenario: Every hop of a stage reference's route counts
- **WHEN** a stage over `Invoice` reads `Consumer.email` along `Invoice → Subscription → Customer → Consumer`,
  spelled in full or short form, and any one of those joins is dropped
- **THEN** the query-backed model is removed; dropping a join off that route does not remove it
