## MODIFIED Requirements

### Requirement: A query reference is anchored at its first segment

In every position of a query (dimensions, time dimensions, metrics and their aggregation arguments, `partition_by=` members, filters, order, `main_time_dimension`), a dotted reference's first segment SHALL name its **anchor**: a dataset of the reading dataset's datasource that is visible to the caller, a stage of the same query list (which precedes a same-named dataset), or the time spine. The remaining segments SHALL be an explicit route from the anchor, each hop spelled by an edge name or a target dataset name as `models/join-traversal` defines, ending at a field, a dataset metric, `*` or a list element. A first segment SHALL be matched by the identifier-case rule of `queries/identifier-case` (exact spelling first, then a unique case-insensitive match; a quoted segment exactly, else by its Ossie normalized form). An edge name, a dataset of another datasource and a dataset hidden from the caller SHALL NOT be an anchor; such a first segment SHALL fail with a typed error naming it, which for an edge name suggests the spelling `<dataset>.<edge>.<rest>` for each dataset declaring or receiving that edge, and which never reveals a hidden dataset. A stored definition (a field's expression, a dataset filter, a saved metric, a custom aggregation's defaults) SHALL keep reading its references from its declaring dataset and SHALL NOT be anchored this way.

#### Scenario: An Ossie query runs with Ossie's reference meaning
- **WHEN** the query `{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}` runs
- **THEN** the EAST row carries 350 and the WEST row 300, under the keys `customers.region` and `orders.total_revenue`

#### Scenario: The population's own name anchors its own fields
- **WHEN** a query with `population: "orders"` selects `orders.status` and `count(orders.*)`
- **THEN** both read `orders`' own rows with no join

#### Scenario: A stage precedes a same-named dataset
- **WHEN** a query list has a stage named `customers` over `orders` projecting `orders.status`, and a later stage with `population: "customers"` selects `customers.status`
- **THEN** the reference reads the stage's column, not the stored `customers` dataset

#### Scenario: An edge name is not an anchor
- **WHEN** `orders` declares a relationship named `buyer` to `customers`, and a query without a population selects `buyer.region`
- **THEN** it fails with a typed error naming `buyer` and suggesting `orders.buyer.region`

#### Scenario: A dataset of another datasource is not an anchor
- **WHEN** a dataset `events` exists only in another datasource and a query with `population: "customers"` measures `count(events.*)`
- **THEN** it fails with the unknown-dataset error, never as an unrelated product

#### Scenario: A hidden dataset is reported as unknown
- **WHEN** the caller's access context hides `returns` and a query selects `returns.amount`
- **THEN** it fails exactly as for a dataset that does not exist

#### Scenario: First segments follow the identifier-case rule
- **WHEN** a query selects `Customers.region` unquoted, and another selects `"Customers".region` quoted, with only `customers` stored
- **THEN** the first reads `customers.region` and the second fails as an unknown dataset

#### Scenario: Stored definitions stay dataset-local
- **WHEN** `customers` has a field `region_label` with expression `upper(region)`, a dataset filter `region <> 'TEST'`, and a saved metric `aov = sum(orders.amount) / count(*)`, and a query with `population: "orders"` selects `customers.region_label` and `customers.aov`
- **THEN** `region` and `count(*)` inside the definitions read `customers`' own field and rows, and `orders.amount` crosses `customers`' own relationship, exactly as before this change
