## ADDED Requirements

### Requirement: The demo carries an order status and a store region hierarchy
The demo SHALL give every order a non-null `orders.status` of `completed`, `refunded` or `cancelled`,
fixed by the order's id so the same order always gets the same status; refunded and cancelled orders
keep their amounts. The demo SHALL carry a `regions` table and give every store a non-null
`stores.region_id` referencing it, so `orders → stores → regions` is a two-hop join path. A generated
store with no region assignment SHALL fail demo setup with an error naming the store, leaving any
existing demo database in place.

#### Scenario: Orders carry a status
- **WHEN** the demo is built with the default two years of data
- **THEN** every order's status is one of `completed`, `refunded`, `cancelled`, and each of the three
  occurs

#### Scenario: An order's status is fixed by its id
- **WHEN** two demo databases are built from the same generated orders
- **THEN** each order has the same status in both

#### Scenario: Region is a two-hop dimension of orders
- **WHEN** `{"source_model": "orders", "measures": ["count(*)"], "dimensions": ["stores.regions.name"]}`
  is queried on the demo
- **THEN** it returns one row per region that has orders, and the counts sum to the total order count

#### Scenario: An unassigned store fails setup
- **WHEN** the generated data contains a store with no region assignment and a demo database already
  exists
- **THEN** setup fails with an error naming that store, and the existing demo database is unchanged
