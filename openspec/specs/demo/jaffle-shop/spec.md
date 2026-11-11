# demo/jaffle-shop Specification

## Purpose
Defines the bundled Jaffle Shop demo's guarantees: exact monetary values, a database that matches the
bundled schema, and a setup that heals stored demo models without discarding user edits.

## Requirements

### Requirement: Demo money is exact
The demo's monetary columns (`products.price`, `orders.subtotal`, `orders.tax_paid`,
`orders.order_total`, `supplies.cost`) SHALL be stored as exact decimals with two fractional digits,
converted exactly from the generator's integer cents, and the demo models SHALL declare that physical
type, so aggregates over them return exact decimal values with no floating-point noise.

#### Scenario: Revenue sums are exact
- **WHEN** `sum(order_total)` is queried by store and month on the demo
- **THEN** every value equals the exact sum of the stored two-digit decimals, with no float noise

#### Scenario: A user-set physical type survives setup
- **WHEN** a user has set a different non-empty physical type on a demo money column and the demo is
  set up again
- **THEN** the user's physical type is kept

### Requirement: A stale demo database is rebuilt
Demo setup SHALL regenerate the demo database when its tables or columns, or their types, differ from
the bundled schema, and SHALL NOT regenerate an up-to-date one. Regeneration SHALL never leave the
demo without a database file: the old file is replaced only by a complete new one. If another process
holds the file, setup SHALL fail with a one-line error naming the file.

#### Scenario: A demo built with floating-point money is rebuilt
- **WHEN** demo setup finds an existing demo database whose `orders.order_total` is a floating-point
  column
- **THEN** the database is regenerated with the bundled schema

#### Scenario: An up-to-date demo is reused
- **WHEN** demo setup runs twice in a row
- **THEN** the second run regenerates nothing

### Requirement: Demo setup heals stored models without discarding edits
Every demo setup SHALL ingest the demo models through idempotent ingestion — creating missing models
and additively merging existing ones — and then apply the curated demo enrichment, so stored demo
models gain any joins, keys and curated metadata they lack while every user edit is kept.

#### Scenario: Join-less demo models regain their joins
- **WHEN** stored demo models have no joins (ingested by an affected release) and the demo is set up
  again
- **THEN** the models carry their FK joins and the README query
  `{"source_model": "orders", "measures": ["sum(order_total)", "change_pct(sum(order_total))"], "dimensions": ["stores.name", "date_trunc('month', ordered_at)"]}`
  returns rows

#### Scenario: A user edit survives setup
- **WHEN** a user changed a demo column's description and the demo is set up again, with or without
  a rebuild
- **THEN** the changed description is kept

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
