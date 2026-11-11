## Purpose

How Mode-B names match their declarations: unquoted identifiers and function names are case-insensitive, quoted identifiers match exactly, and names that differ only in case are kept apart at save, query and ingestion time.

## ADDED Requirements

### Requirement: Unquoted names fold, quoted names match exactly
An unquoted identifier SHALL match a declared name case-insensitively (Unicode case folding), and a double-quoted identifier SHALL match only the declared name spelled exactly as written. This SHALL hold in every namespace: model names in a datasource, stage names in a query list (where a stage overrides a model whose name it matches), a model's columns and saved measures (one namespace), join names, model aggregations, and a query's result names (measure and dimension names). Quoting SHALL apply per path segment: in `"Orders".amount` the first segment matches exactly and the second case-insensitively. Function names — scalar functions, aggregations, transforms, date-part aliases and `date_trunc` units — SHALL match case-insensitively. Mode-A SQL and `{variable}` names SHALL be unaffected.

#### Scenario: Any case reaches a column
- **WHEN** a query rooted at `orders` has dimension `ORDERS.Status` and measure `SUM(Amount)`, and the model declares `status` and `amount`
- **THEN** it groups by `status` and sums `amount`

#### Scenario: Quoted names are exact
- **WHEN** model `orders` declares the column `amount` and a measure is `sum("Amount")`
- **THEN** the query fails with the unresolved-reference error, and `sum("amount")` binds

#### Scenario: Quoting per segment
- **WHEN** a query rooted at `orders` joined to `customers` (declaring `region`) has dimensions `"customers".REGION` and `CUSTOMERS."region"`
- **THEN** both resolve to `customers.region`, while `"Customers".region` fails as an unresolved reference

#### Scenario: Join, model and stage names fold
- **WHEN** a query list has a stage `Monthly` and its main query is rooted at `monthly`, and another query rooted at `orders` reads `CUSTOMERS.region` through the join named `customers`
- **THEN** the first reads the stage and the second walks the join

#### Scenario: Transform names fold
- **WHEN** a measure is `RANK(SUM(amount), direction = 'desc')` and another `CumSum(sum(amount))` in a monthly query
- **THEN** they equal `rank(sum(amount), direction = 'desc')` and `cumsum(sum(amount))`

### Requirement: The exact spelling wins, otherwise one match or a typed ambiguity
An unquoted name SHALL resolve to the declared name spelled exactly as written when one exists; otherwise to the unique declared name it matches case-insensitively. When several declared names match case-insensitively and none exactly, the reference SHALL fail with a typed ambiguity error naming every candidate and the remedy of quoting the intended name. The same rule SHALL apply to function names, which cannot be quoted.

#### Scenario: Exact spelling in a colliding model
- **WHEN** a stored model declares the columns `Revenue` and `revenue` and measures are `sum(Revenue)` and `sum(revenue)`
- **THEN** each sums its exactly-spelled column

#### Scenario: Ambiguous spelling
- **WHEN** the same model is queried with `sum(REVENUE)`
- **THEN** the query fails with the ambiguity error naming `Revenue` and `revenue` and suggesting `"Revenue"` or `"revenue"`

### Requirement: Declared spellings are canonical
Result keys, join aliases and result names SHALL carry the declared spelling of every name, whatever case the reference used, so spellings that differ only in case share one value and one key.

#### Scenario: Case-variant references share one key
- **WHEN** a query rooted at `orders` has measures `sum(AMOUNT)` and `sum(amount)`
- **THEN** they are one measure with key `orders.amount_sum`

#### Scenario: Dimension key uses the declared spelling
- **WHEN** a query rooted at `orders` has dimension `CUSTOMERS.REGION`
- **THEN** its result key is `orders.customers.region`

### Requirement: Saves never introduce a case collision
A save of a model, datasource or saved query SHALL be refused with a typed error naming both names when it introduces two names in one namespace (model names in a datasource, a model's columns and saved measures, its join names, its model aggregations, the stage names of a query list, a query's result names) that are equal case-insensitively and are not both present in the persisted document of the same identity. A new document has no persisted collisions. A collision present in the persisted document SHALL keep loading and saving; it SHALL produce a structured warning naming both names on load, on save, and on every query response that reads the model.

#### Scenario: A new collision is refused
- **WHEN** a model declaring the column `revenue` is saved with an added saved measure `Revenue`
- **THEN** the save fails with the typed error naming `revenue` and `Revenue`

#### Scenario: A grandfathered collision keeps working
- **WHEN** a stored model already declares the columns `Revenue` and `revenue` and is saved unchanged, and again with an edited description
- **THEN** both saves succeed with the collision warning, and a query reading the model returns it among its warnings

#### Scenario: Removing and reintroducing a collision
- **WHEN** a grandfathered model is saved without `revenue`, and later saved with `revenue` added back
- **THEN** the first save succeeds and the second is refused as a new collision

#### Scenario: Model names in a datasource
- **WHEN** a datasource holds a model `orders` and a model `Orders` is created
- **THEN** the save fails with the typed error naming both models

### Requirement: A query's result names never collide by case
A query (or each query of a query list) whose result names, after resolution, include two that are equal case-insensitively but differ in spelling SHALL fail before SQL is generated, on every dialect, with a typed error naming both. Saved queries that already hold such names SHALL keep running.

#### Scenario: Two measures named by case
- **WHEN** a query has the measures `{"formula": "sum(amount)", "name": "Total"}` and `{"formula": "count(*)", "name": "total"}`
- **THEN** it fails with the typed error naming `Total` and `total`, on every dialect

### Requirement: Ingestion keeps case-only-distinct columns apart
When a table being ingested has physical columns whose names are equal case-insensitively, the first in physical column order SHALL keep its spelling as the logical name, and each later one SHALL get the smallest `<name>_<n>` (n ≥ 2) not equal case-insensitively to any persisted, incoming or already generated name, with `sql` naming the physical column, and a structured warning naming the physical column and the logical name. Re-ingesting the same table SHALL produce the same names.

#### Scenario: Two case variants
- **WHEN** a Postgres table with columns `"Foo"` and `foo` (in that order) is ingested
- **THEN** the model has columns `Foo` and `foo_2` (`sql` reading the physical `foo`), with a warning, and each returns its own physical column's values

#### Scenario: Suffix already taken
- **WHEN** a table with columns `Foo`, `foo` and `foo_2` is ingested, and then ingested again
- **THEN** the model has columns `Foo`, `foo_3` (reading `foo`) and `foo_2`, the same both times
