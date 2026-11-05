## ADDED Requirements

### Requirement: SLAYER extensions restore SLayer structure

The importer SHALL read custom extensions with `vendor_name: SLAYER` and apply the SLayer content they carry: join type, name, cardinality and declaring model; measure formula, model, name, label, type and metadata; column display attributes, metadata and value/filter split; model hidden flag, access tags, default time dimension, metadata, custom aggregations, source structure, and SLayer-only columns and measures. Content that duplicates Ossie content (a measure's formula and model, a column's value/filter split, a model's source structure) SHALL be restored only when the payload's fingerprint matches the entity's Ossie content as found in the document; otherwise the Ossie content SHALL be imported as it would be without the extension, and the report SHALL hold an informational entry naming the entity. A payload that is malformed, of an unknown version, or attached to an entity it does not describe SHALL be reported and ignored, and the import SHALL NOT raise. Extensions of other vendors SHALL keep being stored on the entity as before.

#### Scenario: Unedited formula restored
- **WHEN** an exported metric `aov` with `sql: SUM(amount) / COUNT(*)` carries a `SLAYER` extension recording the formula `revenue / n` and a matching fingerprint
- **THEN** the imported measure `aov` has the formula `revenue / n`

#### Scenario: Edited metric falls back to its SQL
- **WHEN** the same metric's `sql` was edited to `SUM(amount) / NULLIF(COUNT(*), 0)` after export
- **THEN** the imported measure is converted from the edited SQL and the report holds an informational entry naming `aov`

#### Scenario: Inner join restored
- **WHEN** a relationship carries a `SLAYER` extension recording an inner join declared on `orders`
- **THEN** the imported join from `orders` is an inner join

#### Scenario: One-to-many declaration restored
- **WHEN** a relationship `from: orders, to: customers` carries a `SLAYER` extension recording a one-to-many join declared on `customers`
- **THEN** `customers` has a one-to-many join to `orders` and `orders` has no join to `customers`

#### Scenario: Malformed payload
- **WHEN** a dataset's `SLAYER` extension `data` is not valid JSON
- **THEN** the dataset imports as it would without the extension and the report names it

### Requirement: Restoration is validated per entity

Every model rebuilt from a `SLAYER` extension SHALL be a fully validated SLayer model. When restoring an entity's SLayer content would make its model invalid or would reference an entity that was not imported, that entity SHALL be imported from its Ossie content alone and reported; no other entity SHALL be affected.

#### Scenario: Restored join to a dropped dataset
- **WHEN** an extension records a join whose target dataset was skipped during import
- **THEN** the join is not created, the report names it, and the rest of the model imports with its restored content

### Requirement: Restored source structure

A dataset whose extension records a table or SQL with row filters, or saved queries with variables, and whose fingerprint matches its `source`, SHALL import with that structure: a table-backed or sql-mode model with its row filters, or a query-backed model with its saved queries and variable defaults. A query-backed model SHALL be restored after the models its queries read; when it fails validation, or reads a model that was not imported, it SHALL import as a sql-mode model over its `source` and be reported.

#### Scenario: Filtered model restored
- **WHEN** a dataset's `source` is the query an export wrote for a table with filters, and its extension's fingerprint matches
- **THEN** the imported model is table-backed with those filters

#### Scenario: Query-backed model restored
- **WHEN** an exported query-backed model and the models its saved query reads are imported together
- **THEN** the imported model is query-backed with the same saved query and variable defaults

#### Scenario: Query-backed model whose input is missing
- **WHEN** an exported query-backed model is imported without the model its saved query reads
- **THEN** it imports as a sql-mode model over its `source` and the report names it

### Requirement: SLayer-only columns and measures

Columns and measures carried as SLayer-only content in a dataset's `SLAYER` extension SHALL be added to its model after its fields and metrics; measures SHALL be added in an order where every measure they reference already exists. A carried column or measure that does not validate on the imported model SHALL be reported and skipped.

#### Scenario: Transform measure restored
- **WHEN** a dataset's extension carries the measure `revenue_growth: change_pct(revenue)` and `revenue` is a metric of that dataset
- **THEN** the imported model has both measures and `revenue_growth` returns the period-over-period change of `revenue`

### Requirement: Statistical and approximate aggregates

`STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP` and `VAR_POP` of one argument, `CORR`, `COVAR_SAMP` and `COVAR_POP` of two arguments, and `APPROX_COUNT_DISTINCT` of one argument SHALL import as the corresponding SLayer aggregations, the second argument of the two-argument ones becoming their `other` parameter, with or without `FILTER`.

#### Scenario: Sample standard deviation
- **WHEN** a dataset-scoped metric is `STDDEV_SAMP(amount)` over amounts 10, 5, 7
- **THEN** the measure returns the sample standard deviation of 10, 5 and 7

#### Scenario: Filtered correlation
- **WHEN** a dataset-scoped metric is `CORR(amount, quantity) FILTER (WHERE status = 'completed')`
- **THEN** the measure returns the correlation of `amount` and `quantity` over completed rows only

#### Scenario: Approximate distinct count
- **WHEN** a dataset-scoped metric's only expression is a DuckDB entry `APPROX_COUNT_DISTINCT(customer_id)` and the target is DuckDB
- **THEN** the measure imports as an approximate distinct count of `customer_id`

### Requirement: Explicit time role is recorded

A field that declares `dimension.is_time` explicitly SHALL keep the declared value, `true` or `false`, in its column's metadata.

#### Scenario: Explicit opt-out recorded
- **WHEN** a TIMESTAMP field declares `dimension: {is_time: false}`
- **THEN** its column's metadata records the time role as false
