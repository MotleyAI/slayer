## ADDED Requirements

### Requirement: A named computed dimension keys by its name
A computed dimension with a user-chosen `name` SHALL surface under that name — result key `<model>.<name>` in a single query, column `<name>` in any downstream stage or when its query backs a model — whatever its expression, including a bare joined path of any length. A computed dimension without a user-chosen name whose expression is a bare joined path SHALL behave exactly as the plain dotted dimension: result key `<model>.<canonical path>.<column>`, downstream column `<canonical path, __-joined>__<column>`. Every dimension entry SHALL project its own column, including two entries over the same joined path. A query's planned result keys SHALL equal, in projection order, the result keys its rendered SQL returns.

#### Scenario: Named one-hop path in a single query
- **WHEN** `orders` is queried with dimension `{"name": "region_id", "expression": "customers.region_id"}` and measure `{"formula": "sum(amount)", "name": "revenue"}`
- **THEN** the result columns are `orders.region_id` and `orders.revenue`, with rows `(1, 160)` and `(2, 50)`

#### Scenario: Named one-hop path crosses a stage
- **WHEN** stage `cr` over `orders` declares dimensions `{"name": "region_id", "expression": "customers.region_id"}` and `customer_id` and measure `{"formula": "sum(amount)", "name": "revenue"}`, and the next stage over `cr` selects dimensions `region_id`, `customer_id`, `revenue`
- **THEN** the query executes, with rows `(1, 1, 90)`, `(1, 2, 70)`, `(2, 3, 50)`

#### Scenario: Named two-hop path crosses a stage
- **WHEN** stage `cr` over `orders` declares dimension `{"name": "region_name", "expression": "customers.regions.name"}` and measure `revenue = sum(amount)`, and the next stage over `cr` selects dimensions `region_name`, `revenue`
- **THEN** the query executes, with rows `(RegN, 160)` and `(RegS, 50)`

#### Scenario: A downstream stage aggregates by a named joined dimension
- **WHEN** stage `cr` over `orders` declares dimensions `{"name": "rid", "expression": "customers.region_id"}` and `customer_id` and measure `revenue = sum(amount)`, and the next stage over `cr` selects dimension `rid` and measure `sum(revenue)` named `tot`
- **THEN** the query executes, with rows `(1, 160)` and `(2, 50)`

#### Scenario: A query-backed model exposes a named joined dimension
- **WHEN** a saved query-backed model `cr` over `orders` declares dimension `{"name": "region_id", "expression": "customers.region_id"}` and measure `revenue = sum(amount)`, and `cr` is queried with dimensions `region_id` and `revenue`
- **THEN** the query executes, with rows `(1, 160)` and `(2, 50)`

#### Scenario: Unnamed computed path equals the plain dimension
- **WHEN** `orders` is queried with dimension `{"expression": "customers.region_id"}` and measure `revenue = sum(amount)`, alone and as stage `cr` read downstream by dimension `customers__region_id`
- **THEN** the single query's result key is `orders.customers.region_id`, and the two-stage query executes with rows `(1, 160)` and `(2, 50)`

#### Scenario: Plain and named entries over one path each project a column
- **WHEN** `orders` is queried with dimensions `customers.region_id`, `status`, `{"name": "rid", "expression": "customers.region_id"}` and measure `revenue = sum(amount)`, alone and as stage `cr` read downstream by `customers__region_id`, `status`, `rid`
- **THEN** the single query's result columns are `orders.customers.region_id`, `orders.status`, `orders.rid`, `orders.revenue` in that order with `rid` equal to `customers.region_id` on every row, and the two-stage query executes with both columns

#### Scenario: Ordering and filtering by the name
- **WHEN** `orders` is queried with dimension `{"name": "rid", "expression": "customers.region_id"}`, measure `revenue = sum(amount)`, and either order `rid` descending or filter `rid = 1`
- **THEN** the rows are keyed `orders.rid` and come back as `(2, 50), (1, 160)` or `(1, 160)` respectively

#### Scenario: Plain dotted dimension and local rename unchanged
- **WHEN** stage `cr` over `orders` declares dimension `customers.region_id` read downstream as `customers__region_id`, or declares `{"name": "cust", "expression": "customer_id"}` read downstream as `cust`
- **THEN** both queries execute with their existing names and values

#### Scenario: A name colliding with a host column is still rejected
- **WHEN** `orders` (which has a column `region`) is queried with dimension `{"name": "region", "expression": "customers.regions.name"}`
- **THEN** the query fails with the computed-dimension name-collision error
