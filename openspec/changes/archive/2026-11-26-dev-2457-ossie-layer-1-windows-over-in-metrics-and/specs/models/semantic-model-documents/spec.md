## MODIFIED Requirements

### Requirement: Entities SLayer cannot evaluate yet still load
A field or metric of a valid document whose expression SLayer cannot evaluate yet SHALL load with a structured load warning naming the entity and the reason, visible on `inspect`, `datasets_summary` and search results, and reported by `slayer import-osi`. These are: a model-scoped metric reading a field unqualified or outside an aggregate; an expression whose runnable form SLayer's expression reader cannot parse (for example a subquery); a dataset-scoped metric that evaluates a measure of its dataset's source with `AGG(source.measure)` (SLayer's sources expose no measure columns, and Ossie requires a consumer to reject `AGG` on a column that is not a measure and never to replace it with an ordinary aggregate); and any model-scoped metric that reaches such an entity, directly through `AGG(d.m)` or transitively through other model-scoped metrics, whose warning SHALL name the dependency chain. Every query reading the entity SHALL fail with a typed error naming it and the reason; no other entity SHALL be affected. A save SLayer authors that adds or changes a definition to one SLayer cannot evaluate SHALL be refused with that typed error; an unchanged entity that loaded with a warning SHALL survive saves of other parts of its document. A window function SHALL NOT make an entity one SLayer cannot evaluate: a windowed field, a windowed metric and a metric reading either SHALL load without a warning and SHALL be evaluated (`queries/window-functions`).

#### Scenario: A windowed field loads and fails only when read
- **WHEN** a document declares a field `running_total` with expression `SUM(amount) OVER (ORDER BY order_date)` beside a field `amount`, and a model-scoped metric `running` with expression `SUM(SUM(sales.amount)) OVER ()`
- **THEN** the document loads with no warning on either, `slayer import-osi` reports neither as not evaluable, and queries reading `sales.running_total` and `running` return their values; neither fails

#### Scenario: An expression SLayer cannot parse loads not evaluable
- **WHEN** a document declares a field `flag` with expression `(SELECT 1)` beside a field `amount`
- **THEN** the document loads, `inspect` shows a warning on `flag` naming the parse failure, `slayer import-osi` reports `flag` as not evaluable, a query of `sum(sales.amount)` succeeds, and a query reading `sales.flag` fails with a typed error naming `flag`

#### Scenario: SQL keywords inside literals stay evaluable
- **WHEN** a document declares a field `kind` with expression `CASE WHEN status IN ('select', 'from') THEN 'where' END`
- **THEN** `kind` loads with no load warning and a query reading it succeeds
#### Scenario: A source-measure metric loads and fails only when read
- **WHEN** a document's dataset `orders` declares the metrics `revenue_total` (`AGG(source.revenue)`) and `order_count` (`COUNT(*)`)
- **THEN** the document loads, `inspect` shows on `orders.revenue_total` a warning that it evaluates a measure of its source, which SLayer cannot evaluate yet, a query of `orders.order_count` succeeds, and a query reading `orders.revenue_total` fails with a typed error naming `revenue_total` and that reason

#### Scenario: A model-scoped metric over a source-measure metric fails with its chain
- **WHEN** the same document declares the model-scoped metrics `revenue_per_customer` (`AGG(orders.revenue_total) / COUNT(DISTINCT customers.id)`) and `doubled` (`revenue_per_customer * 2`, stored in the `SLAYER` extension)
- **THEN** both load with warnings, and a query naming `doubled` fails with a typed error naming `doubled`, the chain through `revenue_per_customer` to `orders.revenue_total`, and the reason

#### Scenario: Authoring a source-measure metric is refused
- **WHEN** a dataset tool adds the dataset metric `revenue` with expression `AGG(source.revenue)` to dataset `orders`
- **THEN** the save fails with the typed error naming `revenue` and the reason, and nothing is stored

#### Scenario: An unrelated edit keeps a warned metric
- **WHEN** a dataset tool edits the description of field `orders.order_id` in a document whose `orders.revenue_total` is `AGG(source.revenue)`
- **THEN** the save succeeds, and the document text keeps `revenue_total` unchanged
