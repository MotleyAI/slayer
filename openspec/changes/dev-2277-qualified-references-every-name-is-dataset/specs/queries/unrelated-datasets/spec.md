## MODIFIED Requirements

### Requirement: An unrelated dataset is reached by naming it

In a query frame — the main query and every stage of a query list, including the stored stages of a query-backed model — a reference whose anchor (`queries/references`) has no route from the population SHALL resolve through a product hop to that anchor, when the anchor is a model visible to the caller or a query-local stage (the stage spelling taking precedence) in the population's datasource. The product hop SHALL pair every population row with every row of that dataset (`P × D`) and determine nothing across it. Every route resolution SHALL take precedence, and no route search — anchor routing, route counting, population inference, root recommendation's determination probe — SHALL take a product hop. A reference SHALL continue past the product hop over ordinary edges of its explicit route. A first segment naming no visible dataset of the population's datasource SHALL keep its existing error; the time spine and the population itself are anchors with their own rules, never product hops. Stored definitions (`Column.sql`, model filters, saved measures, joins) SHALL refuse a reference with no join path: a saved measure fails when used with the existing no-route error, and a `Column.sql` or model filter is rejected at save time with a typed error naming the definition and the unreachable model, and fails closed with that error at query time when stored without validation.

#### Scenario: A grand-total ratio across unrelated models

- **WHEN** a query with `source_model: customers` measures `{"formula": "count(events.id) / count(customers.*)", "name": "events_per_customer"}`
- **THEN** one row is returned with `events_per_customer` = 5 / 3, equal to dividing the two separate single-model answers

#### Scenario: A path continues past the product hop

- **WHEN** `events` joins `users` many-to-one and a query rooted at `customers` measures `count(events.users.id)`
- **THEN** the value equals that of an `events`-rooted query measuring `count(users.id)`

#### Scenario: A query-local stage is an unrelated dataset

- **WHEN** a query list has stages `c` (`source_model: customers`, measure `count(customers.*)` named `nc`) and `e` (`source_model: events`, measure `count(events.*)` named `ne`) and a final stage `{"source_model": "c", "dimensions": ["c.nc"], "measures": [{"formula": "max(e.ne)", "name": "ne"}]}`
- **THEN** exactly one row (nc 3, ne 5) is returned

#### Scenario: References in every position are found

- **WHEN** the only reference to `events` in a `customers`-rooted query sits in a filter, an order entry, or an aggregation's `partition_by=[]` measure
- **THEN** it resolves through the product hop exactly as it would in a measure

#### Scenario: Existing paths and routing are unchanged

- **WHEN** a query references a model whose anchor a route reaches from the population
- **THEN** it resolves along that route, and no product hop is taken

#### Scenario: Names that are not reachable datasets keep their errors

- **WHEN** a `customers`-rooted query references `evnts.id` (no such model), a model of another datasource, a model hidden from the caller by its access context, or `customers.id`
- **THEN** the first three fail as unknown datasets, a hidden model being reported as unknown, and `customers.id` reads the population's own field

#### Scenario: Row-level security scopes the unrelated dataset

- **WHEN** an engine-level tenant policy restricts both models to one tenant, owning customers 1–2 and events 1–3, and the grand-total ratio query runs
- **THEN** the ratio is 3 / 2

#### Scenario: Stored definitions keep refusing

- **WHEN** a saved measure on `customers` reads `count(events.id)`, a `customers` column's `sql` reads `events.amount`, or a `customers` model filter reads `events.amount > 1`
- **THEN** using the saved measure fails with the existing no-route error; saving the column or the filter fails with the typed error, and using either one stored without validation fails with the same error
