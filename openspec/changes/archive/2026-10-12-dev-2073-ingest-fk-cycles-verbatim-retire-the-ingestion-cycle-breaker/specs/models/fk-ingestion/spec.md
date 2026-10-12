## Purpose

Defines how ingestion turns a datasource's live foreign-key constraints into model joins: every FK
relationship becomes exactly one addressable edge, whatever cycles or parallel pairs the FK graph holds.

## ADDED Requirements

### Requirement: FK cycles ingest verbatim
Ingestion SHALL emit one join per in-schema FK constraint between two distinct ingested objects,
whatever directed or undirected cycles the FK graph contains. A cycle MUST NOT suppress, drop or
re-key any join. Self-referencing and cross-schema FKs remain excluded.

#### Scenario: A 2-cycle keeps both FKs
- **WHEN** `transactions.coupon_usage_id → coupon_usages.id` and `coupon_usages.transaction_id →
  transactions.id` exist and the datasource is ingested
- **THEN** both joins are ingested, and every other FK join in the schema is ingested too

#### Scenario: A 3-cycle keeps every edge
- **WHEN** `employees → depts → offices → employees` FKs exist
- **THEN** all three joins are ingested and `employees.depts.offices.<column>` resolves

#### Scenario: Latest-child pointer keeps the main FK
- **WHEN** `orders.customer_id → customers.id` and `customers.last_order_id → orders.id` exist and a
  query rooted at `orders` selects `customer.name` (the edge name of the `customer_id` edge)
- **THEN** every order row carries the name of the customer its `customer_id` references

### Requirement: FK reflection failure loses only that table's joins
When reading one table's foreign keys fails, ingestion SHALL still ingest that table, with no joins
declared on it, and SHALL ingest every other table and join unaffected.

#### Scenario: One table's FK reflection fails
- **WHEN** reading the foreign keys of `orders` raises during ingestion
- **THEN** `orders` is ingested with its columns and no declared joins, and is not reported as skipped

### Requirement: One edge per FK relationship
Each live FK relationship SHALL be represented by at most one edge between its two models, whatever
the edge's orientation or name. Two FK constraints that are exact inverses (mutual FKs on one key
pair) SHALL ingest as a single edge — the to-one half where cardinality says which that is, else a
deterministic choice — and both models SHALL ingest. A FK whose key pairs, in either orientation,
match an edge already stored between the two models SHALL NOT add an edge.

#### Scenario: Mutual FKs on one key pair
- **WHEN** `users.id REFERENCES profiles(user_id)` and `profiles.user_id REFERENCES users(id)` both
  exist and the datasource is ingested, then ingested again
- **THEN** both models are ingested, exactly one edge connects them, and the second ingest reports no
  error and adds no edge

#### Scenario: A FK stored in the reverse orientation is not re-added
- **WHEN** `customers` stores a join to `orders` whose key pairs are the swapped pairs of the live FK
  `orders.customer_id → customers.id`, named or not, and the datasource is re-ingested
- **THEN** no join from `orders` to `customers` is added

### Requirement: Parallel sets containing a FK edge are named
When two or more edges connect one unordered pair of models and at least one of them is FK-backed
(its key pairs, in either orientation, match a live FK), ingestion SHALL give every unnamed member of
that set an edge name; a hand-authored member is named from its own declaring-side key columns. An
edge with a user-set name SHALL never be renamed. Edges with no parallel twin SHALL stay unnamed, and
a parallel set with no FK-backed member SHALL NOT be touched.

#### Scenario: Two FKs to one target are addressable by name
- **WHEN** `orders.billing_address_id → addresses.id` and `orders.shipping_address_id → addresses.id`
  exist and a query rooted at `orders` selects `billing_address.city` and `shipping_address.city`
- **THEN** each column comes from its own address and the result keys are
  `orders.billing_address.city` and `orders.shipping_address.city`

#### Scenario: 2-cycle edges are named
- **WHEN** the `transactions` ↔ `coupon_usages` 2-cycle is ingested
- **THEN** the edges are named `coupon_usage` and `transaction`, and `transactions.coupon_usage.<column>`
  resolves to the coupon usage each transaction references

#### Scenario: A singleton edge stays unnamed
- **WHEN** `orders.customer_id → customers.id` is the only edge between `orders` and `customers`
- **THEN** that join has no name and `orders.customers.<column>` keeps its spelling

#### Scenario: A hand-authored member of a FK parallel set is named
- **WHEN** `orders` stores an unnamed hand-authored join to `addresses` on `legacy_addr = id` and
  re-ingest adds the live FK `orders.billing_address_id → addresses.id`
- **THEN** both edges are named (`legacy_addr` and `billing_address`) and each is addressable by name

#### Scenario: User-set names are kept
- **WHEN** a stored FK-backed edge is named `bill_to` by a user and a second FK to the same target
  appears on re-ingest
- **THEN** that edge keeps the name `bill_to` and only the new edge receives a generated name

### Requirement: Generated edge names are deterministic and collision-free
A generated edge name SHALL be the first valid, non-colliding candidate of: the FK stem (the
declaring-side key column name with a trailing `_id` or `_fk` removed, case-insensitively; a
composite key's stems joined by `_`; the column name itself when the stem is empty); then
`<declaring_model>_<stem>`; then that name suffixed `_2`, `_3`, and so on. A candidate collides when it
equals any model name in the datasource or any edge name incident to either endpoint model, including
names generated earlier in the same ingest. A candidate equal to a column name is not a collision.
The outcome SHALL NOT depend on table discovery or scan order.

#### Scenario: Stem equal to a model name falls back
- **WHEN** a parallel FK `orders.customer_id → people.id` has stem `customer` and a model named
  `customer` exists in the datasource
- **THEN** the edge is named `orders_customer`

#### Scenario: Stem already used by an incident edge falls back
- **WHEN** the stem of a new parallel edge equals an edge name already incident to one of its endpoints
- **THEN** the edge receives the next candidate, never a duplicate incident name

#### Scenario: Composite FK stem
- **WHEN** a parallel composite FK keys on `(order_id, line_no)`
- **THEN** its generated name is `order_line_no`

#### Scenario: Scan order does not change names
- **WHEN** the same schema is ingested twice with tables discovered in different orders
- **THEN** both ingests produce identical edge names

### Requirement: Re-ingest reconciles joins datasource-wide
Re-ingest SHALL add every live FK not already represented, as a new edge, even when its source model
already joins the same target; it SHALL NOT fail or discard a model's other merged columns or joins
because of an existing join. Naming SHALL apply to the merged, datasource-wide edge set, so a stored
unnamed edge on another model is named when its pair becomes parallel, and that model is saved. The
ingest report SHALL list added joins by edge reference and list every edge it named. Re-running an
ingest with no live change SHALL change nothing.

#### Scenario: A reverse FK appears
- **WHEN** `transactions → coupon_usages` is stored unnamed and re-ingest finds the new FK
  `coupon_usages.transaction_id → transactions.id`
- **THEN** the new edge is added on `coupon_usages`, both edges are named, `transactions` is saved
  with its edge's name, and the report lists both names

#### Scenario: A second FK to an existing target appears
- **WHEN** `orders → addresses` on `billing_address_id` is stored and re-ingest finds the new FK
  `shipping_address_id → addresses.id` plus a new column on `orders`
- **THEN** the new edge and the new column are both merged into `orders` and no ingestion error is
  reported

#### Scenario: Re-ingest is idempotent
- **WHEN** a datasource with cycles and parallel FKs is ingested and then re-ingested unchanged
- **THEN** the second ingest saves nothing and reports no additions

### Requirement: Re-ingest converges from a partial save
When a re-ingest stops after persisting only some of its model updates, running it again SHALL
produce exactly the documents and edge names an uninterrupted run produces.

#### Scenario: Interrupted re-ingest is completed by a rerun
- **WHEN** a re-ingest that adds and names several edges fails after any one of its model saves, and
  re-ingest is run again
- **THEN** every stored model equals the result of an uninterrupted re-ingest

### Requirement: A table whose model name is an edge name is skipped
Ingestion SHALL NOT create a model whose name equals an edge name already stored in the datasource;
it SHALL report that table as skipped, naming the colliding edge and the model that declares it.

#### Scenario: New table named like a stored edge
- **WHEN** `orders` stores an edge named `returns` and a new table `returns` appears on re-ingest
- **THEN** no `returns` model is created and the report lists `returns` as skipped because the name
  collides with the edge `returns` on `orders`
