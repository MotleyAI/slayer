## MODIFIED Requirements

### Requirement: The anchor is reached by its only to-one path

A query SHALL reach a reference's anchor from its **reading dataset** as follows: an aggregation's home for that aggregation's grain dimensions and for the filter conjuncts restricting its rows, the dimension anchor for the dimensions and filters it supplies or is restricted by, and the population for every other reference. When the anchor is the reading dataset, the path SHALL be empty. When the reading dataset is an aggregation's home and the reference is anchored where that aggregation's source is anchored, the path SHALL be the source's own written path from that anchor to the home, traversed back. When exactly one path from the reading dataset to the anchor has every hop provably to-one on its traversal orientation, that path SHALL be taken, whatever other routes exist. When no such path exists and exactly one simple route exists, that route SHALL be taken. When two or more to-one paths exist, or none exists and two or more routes exist, the reference SHALL fail with a typed ambiguity error naming each executable explicit spelling; a direct relationship SHALL have no precedence over a longer path. This rule SHALL run over the **declared** relationships: the datasets' own, the query's extension relationships, and any relationship a stage inherits that lies beside a declared one between the same two datasets. Only when they give no route SHALL inherited relationships be added: a single inherited relationship joining the reading dataset directly to the anchor SHALL then be taken, else the rule SHALL run over all relationships. When no route exists, the anchor SHALL be an unrelated dataset (`queries/unrelated-datasets`). Routes never cross the time spine. Binding, the planning of every aggregation rooted at its home, population inference, population recommendation, schema-drift attribution and the stored-query migration SHALL use this one rule, so they never disagree about which path a reference takes.

#### Scenario: One to-one path wins over fanning routes
- **WHEN** `orders → customers` and `orders → managers` are many-to-one, `managers → customers` is one-to-many, and a query with `population: "orders"` selects `customers.region`
- **THEN** it takes the direct to-one relationship and executes

#### Scenario: Two to-one paths are refused even with a direct relationship
- **WHEN** `orders → stores` and `stores → customers` are also many-to-one, and a query with `population: "orders"` selects `customers.region`
- **THEN** it fails with the ambiguity error naming `orders.customers.region` and `orders.stores.customers.region`

#### Scenario: The explicit route resolves the ambiguity
- **WHEN** the same query selects `orders.customers.region`
- **THEN** it takes the direct relationship and executes

#### Scenario: A unique fanning route is taken
- **WHEN** a query with `population: "customers"` measures `sum(orders.amount)` and the only route from `customers` to `orders` is the reversed `orders → customers` relationship
- **THEN** it takes that route and each customer's value is the sum of its own orders

#### Scenario: Several fanning routes are refused
- **WHEN** `customers` reaches `orders` both by the reversed `orders → customers` relationship and through `managers` with no to-one path, and a query with `population: "customers"` measures `sum(orders.amount)`
- **THEN** it fails with the ambiguity error naming each explicit spelling

#### Scenario: An inherited detour never makes a declared path ambiguous
- **WHEN** `orders` declares a relationship to `customers` and to `clients`, `clients` declares a to-one relationship to the query-backed dataset `cust_rev`, whose grain `customer_id` inherits a to-one relationship to `customers`, and a query with `population: "orders"` reads `customers.tier`
- **THEN** it reads `customers` over `orders → customers`; with `cust_rev → customers` declared instead, the reference is refused as ambiguous

#### Scenario: An inherited relationship beside a declared one stays parallel
- **WHEN** a stage over `per_customer` inherits the relationship `buyer` to `customers`, the reading query declares an extension relationship `payer` from `per_customer` to `customers` on the same keys, and it reads `customers.name`
- **THEN** it fails with the typed ambiguity error naming `per_customer.buyer.name` and `per_customer.payer.name`

#### Scenario: A single direct inherited relationship is taken first
- **WHEN** stage `s` is grouped by `customers.id`, inheriting relationships to `orders` and to `events`, and a query with `population: "s"` measures `sum(orders.amount)`
- **THEN** it reads `orders` over `s → orders`, not the detour `s → events → customers → orders`

#### Scenario: An unrelated anchor spelled like a relationship of the reader is read as the anchor
- **WHEN** stage `z` (population `orders`, which has a relationship `ship_region`) measures `max(ship_region.nc)` while a sibling stage named `ship_region` exists that no route from `orders` reaches
- **THEN** it reads the sibling across the product, never the relationship; reading `ship_region.name` fails as an unknown field of the sibling

#### Scenario: A directed cycle takes its to-one path
- **WHEN** `x → y`, `y → z` and `z → x` are many-to-one and a query with `population: "x"` groups by `x.v` and measures `sum(z.amount)` and `sum(x.z.amount)`
- **THEN** both sum the `z` rows referencing each `x`, the home `z` reaching `x` by its only to-one path `z → x`

#### Scenario: Inference and binding agree
- **WHEN** a query without a population has the dimensions `orders.status` and `customers.region` over the two-path schema above
- **THEN** inference and binding report the same ambiguity, and with `orders.customers.region` in place of `customers.region` both choose `orders`

#### Scenario: An aggregation reads its grain from its home
- **WHEN** `orders → customers → regions`, `regions → managers` and `orders → managers` are many-to-one, and a query groups by `regions.name` and measures `orders.total_revenue`, once without `population` and once with `population: "regions"`
- **THEN** both execute, each order reaching its region through `orders → customers → regions`, though `regions` reaches `orders` by two routes neither of which is to-one

#### Scenario: A filter restricting a home is routed from that home
- **WHEN** the same schema's query without `population` groups by `regions.name`, measures `orders.total_revenue` and filters `customers.segment = 'retail'`
- **THEN** each order is kept when its own customer, reached over `orders → customers`, is retail, though `regions` reaches `customers` by two routes neither of which is to-one

#### Scenario: A home reads its source's anchor back along the source's path
- **WHEN** `orders` relates to `customers` twice, by `billing_customer` and by `shipping_customer`, and a query with `population: "orders"` filters `orders.status = 'ok'` and measures `sum(orders.billing_customer.spend)`
- **THEN** it sums the spend of each customer billed for an ok order, though the home `customers` reaches `orders` by two routes neither of which is to-one
