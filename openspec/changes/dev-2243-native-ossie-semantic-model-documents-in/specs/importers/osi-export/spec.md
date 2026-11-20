## MODIFIED Requirements

### Requirement: Export writes one current-spec document per datasource
`slayer export-osi --datasource X` and the Python export API SHALL write the datasource's semantic model — every stored semantic-model document and every standalone dataset of `X` — as one flat current-spec Ossie document (`version: 0.3.0.dev0`) that validates against the pinned upstream Ossie schema. When `X` holds exactly one semantic-model document and no standalone dataset, and no `--datasets` subset is given, the output SHALL be that document's stored text unchanged (converted to JSON only when the output file is JSON). Otherwise the document SHALL hold every dataset, relationship and model-scoped metric of `X`: document entities as stored, standalone datasets exported by the rules below; the document's `name` and `description` SHALL come from the datasource; and each source document's `name`, `description`, `ai_context`, non-portable extensions and the names of its datasets SHALL be carried in a document-level `SLAYER` entry, so that importing the export into an empty datasource restores each source document. Datasets SHALL be ordered by name, and fields, metrics and relationships in a deterministic order, so that exporting unchanged content twice produces identical documents. Export SHALL NOT access the database directly; any metadata it needs SHALL be read through the query engine.

#### Scenario: Exported document validates
- **WHEN** a datasource with table-backed, sql-mode and query-backed models, joins and measures is exported
- **THEN** the document declares `version: 0.3.0.dev0` and validates against the pinned Ossie schema

#### Scenario: Deterministic output
- **WHEN** the same unchanged models are exported twice
- **THEN** the two documents are byte-identical

#### Scenario: No direct database access for table and sql-mode models
- **WHEN** a datasource holding only table-backed and sql-mode models is exported through an engine whose database client fails on every call
- **THEN** the export succeeds

#### Scenario: Imported source columns round trip
- **WHEN** a document whose dataset-scoped metrics read `source.quantity` and `source.amount` is imported, exported, validated, re-imported and exported again
- **THEN** both exports are byte-identical to the original document, and both metrics return the same values as before

#### Scenario: A single-document datasource exports verbatim
- **WHEN** a datasource holds only the document `sales`, whose text carries comments, and is exported to YAML
- **THEN** the output equals the stored text of `sales`

#### Scenario: Two documents and a standalone dataset merge
- **WHEN** a datasource holds documents `sales` and `support` and a standalone dataset `stores` with a relationship into `sales`'s dataset `orders`, and is exported and imported into an empty datasource
- **THEN** the export is one valid document holding all three sources' datasets and the relationship, and the re-import restores the documents `sales` and `support` and the standalone dataset `stores`

### Requirement: Metrics
A measure SHALL export as an Ossie metric when every aggregate in it reads exactly one model and, for a measure reading several models, each of them is reachable from the measure's model by exactly one join route. Each aggregate SHALL be evaluated over the rows of the one model it reads, which is Ossie's component semantics. A measure whose aggregates all read one model SHALL export as a dataset-scoped metric on that model's dataset, with unqualified field names, and its references to other measures SHALL be expanded; any other exportable measure SHALL export as a model-scoped metric with `dataset.field` references, and its references to measures SHALL be written as `AGG(dataset.metric)` of the referenced measure's exported dataset metric. A stored model-scoped metric SHALL export as a top-level metric unchanged; one that Ossie does not define (it refers to another model-scoped metric) SHALL travel only in the `SLAYER` extension. Metric expressions SHALL be standard portable SQL (`OSSIE_SQL_2026` in `sql`); an approximate distinct count, which has no portable form, SHALL be written only as a datasource-dialect entry. Generated metric names SHALL be unique in their scope. An identifier that would be ambiguous under case-insensitive matching SHALL be double-quoted.

#### Scenario: Dataset-scoped metric
- **WHEN** `orders` has the measure `revenue: sum(amount)`
- **THEN** dataset `orders` has the metric `revenue` with `sql: SUM(amount)`

#### Scenario: Measure over another model
- **WHEN** `orders` has the measure `customer_discount: sum(customers.discount)` and `orders` joins `customers` many-to-one
- **THEN** dataset `customers` has a metric with `sql: SUM(discount)`, and querying the re-imported measure on `orders` returns the sum over all customers, including customers with no orders

#### Scenario: Cross-model ratio
- **WHEN** `orders` has the measure `aov_per_customer: sum(amount) / count_distinct(customers.id)`
- **THEN** the document has a model-scoped metric `SUM(orders.amount) / COUNT(DISTINCT customers.id)`

#### Scenario: Measure reference expanded
- **WHEN** `orders` has measures `revenue: sum(amount)`, `n: count(*)` and `aov: revenue / n`
- **THEN** metric `aov` has `sql: SUM(amount) / COUNT(*)`

#### Scenario: Model-scoped reference to a dataset metric uses AGG
- **WHEN** `orders` has measures `revenue: sum(amount)` and `revenue_per_customer: revenue / count_distinct(customers.id)`
- **THEN** the document has a model-scoped metric `AGG(orders.revenue) / COUNT(DISTINCT customers.id)`

#### Scenario: Percentile on SQLite
- **WHEN** a SQLite datasource's model has the measure `p90: percentile(amount, 0.9)`
- **THEN** the metric's `sql` is `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)`

#### Scenario: Boolean sum
- **WHEN** a measure is `sum(is_paid)` over a BOOLEAN column
- **THEN** the metric counts the true values on every supported database

### Requirement: Round trip
Importing an exported document with `import-osi` into an empty datasource SHALL yield datasets, relationships and model-scoped metrics on which every exported and extension-carried field and metric returns the same results as the original, grouped and ungrouped, on DuckDB and SQLite; restored metric expressions SHALL be identical to the originals, and stored documents SHALL be restored with their original text.

#### Scenario: Worked example round trip
- **WHEN** `docs/examples/13_osi_import/shop.osi.yaml` is imported, exported and imported again
- **THEN** every field and metric returns the same results after the second import as after the first, and the stored text is unchanged

#### Scenario: Formula restored
- **WHEN** a model with measure `aov: revenue / n` is exported and re-imported
- **THEN** the re-imported metric's expression is `revenue / n`
