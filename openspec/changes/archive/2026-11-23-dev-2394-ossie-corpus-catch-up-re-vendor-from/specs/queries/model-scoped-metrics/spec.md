## MODIFIED Requirements

### Requirement: AGG evaluates a dataset metric as one component
`AGG(d.m)` SHALL evaluate the dataset metric `m` of dataset `d` as one component homed on `d`, returning the same values as the metric reference `d.m`. It SHALL be accepted in model-scoped metrics, in query expressions, and in stored dataset metrics, where its argument follows that definition's own reference rules (`AGG(m)` for a metric of the same dataset, `AGG(path.m)` across to-one relationships). Its argument SHALL be a single reference to a dataset metric; `AGG` with a `FILTER` clause, `AGG` of a field, `AGG` of a model-scoped metric and `AGG` of any other expression SHALL each be refused with a typed error, at save for a stored definition and before SQL generation for a query. In a stored dataset metric, `AGG(source.x)` (`source` in any case) SHALL be Ossie's evaluation of a measure of the dataset's source, never a relationship path or a dataset named `source`, and SHALL follow `models/semantic-model-documents` ("Entities SLayer cannot evaluate yet still load"). A stored dataset metric using any other `AGG` form, which Ossie does not define in that position, SHALL be stored only in its entity's `SLAYER` extension when it belongs to a document.

#### Scenario: AGG in a query equals the metric reference
- **WHEN** a query asks for `AGG(orders.total_revenue)` and another for `orders.total_revenue`, both by `customers.region`
- **THEN** both return the same values

#### Scenario: AGG of a field is refused
- **WHEN** a query asks for `AGG(orders.amount)` where `amount` is a field
- **THEN** the query fails with a typed error stating that AGG takes a dataset metric

#### Scenario: AGG with FILTER is refused
- **WHEN** a model-scoped metric `AGG(orders.total_revenue) FILTER (WHERE orders.status = 'paid')` is saved
- **THEN** the save fails with a typed error stating that AGG takes no FILTER clause

#### Scenario: AGG(source.x) in a dataset metric is never a path
- **WHEN** dataset `orders` has a to-one relationship named `source` to a dataset with metric `revenue`, and a dataset tool adds to `orders` the metric `r` with expression `AGG(source.revenue)`
- **THEN** the save fails with the typed error for a definition SLayer cannot evaluate yet, naming `r` and stating that it evaluates a measure of its source
