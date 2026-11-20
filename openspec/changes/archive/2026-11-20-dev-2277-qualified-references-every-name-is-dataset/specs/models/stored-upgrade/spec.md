## ADDED Requirements

### Requirement: Stored queries are upgraded to qualified references
A stored query written before this change — a saved query, a query-backed model's `source_queries` stage, or a memory's query, nested or not, with or without a version of its own — SHALL load in the qualified syntax of `queries/references`, with no deprecation warning, by a conversion that reads the stored datasets:
- each reference SHALL be rewritten to the qualified spelling that has the meaning it had before this change, reading the stage's own population;
- a reference the new routing would send along a different path SHALL be rewritten to the explicit route from the population that keeps its old path;
- a reference whose anchor was reached by a route longer than its written path SHALL be rewritten to its explicit route from the population, so its column name is kept;
- a rewritten path SHALL be kept only if it resolves back to the same oriented sequence of relationships;
- an unaliased entry whose references share no anchor SHALL get an alias equal to its column name before this change, so every later stage, query-backed model consumer and `DatasetField.sql` reading that column keeps working;
- `count(*)` SHALL become `count(<population>.*)`.

A stored query that cannot be converted this way (a dataset it reads is missing, or an old path has no unique explicit spelling) SHALL fail to load through the existing unloadable-document boundary, untouched on disk, with a warning naming the document and the reference. Saved queries and query-backed models SHALL be written back as other migrated documents are; memories SHALL convert on each load.

#### Scenario: A saved query loads qualified with its results
- **WHEN** a store holds a saved query with `population: orders`, the dimension `status`, the metric `sum(amount)` and the filter `customers.region = 'EAST'`
- **THEN** it loads as `orders.status`, `sum(orders.amount)` and `customers.region = 'EAST'`, with no `FutureWarning`, and returns the rows it returned before

#### Scenario: A stored cycle reference keeps its path
- **WHEN** a stored query with `population: x` over the cycle `x → y → z → x` measures `sum(z.amount)`
- **THEN** it loads as `sum(x.z.amount)` and returns the value it returned before

#### Scenario: A stored stage chain keeps its column names
- **WHEN** a stored query-backed model's first stage measures `sum(amount) / count(customers.*)` unaliased and its second stage reads that column by its old name
- **THEN** the first stage's entry gets that name as its alias and the model returns the rows it returned before

#### Scenario: An unspellable old path makes the document unloadable
- **WHEN** a stored query's reference reads a dataset that its population reaches only across two unnamed parallel relationships, so its old path has no explicit spelling
- **THEN** the document fails to load, stays untouched on disk, and the warning names the document and the reference

#### Scenario: A memory's query converts on every load
- **WHEN** a stored memory holds a query with `population: orders` measuring `count(*)`
- **THEN** each load reads it as `count(orders.*)`, with no `FutureWarning`, and the stored memory is not rewritten
