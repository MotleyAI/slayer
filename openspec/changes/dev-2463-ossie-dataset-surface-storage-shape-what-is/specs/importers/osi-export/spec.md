## MODIFIED Requirements

### Requirement: Export writes one current-spec document per datasource
`slayer export-osi --datasource X` and the Python export API SHALL write the datasource's semantic model — every model of `X`, its datasource model included (and, on a backend without the semantic-model hooks, every standalone dataset) — as one flat current-spec Ossie document (`version: 0.3.0.dev0`) that validates against the pinned upstream Ossie schema. Every dataset SHALL export by one rule: each Ossie-defined key of its stored entity exactly as stored, and, wherever the dataset's `SLAYER` entry changes what its core means (row filters, a query-backed or `sql`-structured source, SLayer-only fields and metrics), the rules below in place of the stored core for that content; a dataset with no stored entity (a backend without the semantic-model hooks) SHALL export by the rules below. When `X` holds exactly one model, stored as one flat document none of whose entities carry such `SLAYER` content, no `--datasets` subset is given and extensions are not omitted, the output SHALL be that document's stored text unchanged (converted to JSON only when the output file is JSON). With `--no-extensions`, the output for such a document SHALL be that text with only its `SLAYER` entries removed, every other line kept. Otherwise the document SHALL hold every dataset, relationship and model-scoped metric of `X`; the document's `name` and `description` SHALL come from the datasource; and each source model's `name`, `description`, `ai_context`, non-portable extensions and the names of its datasets SHALL be carried in a document-level `SLAYER` entry, so that importing the export into an empty datasource restores each source model. Datasets SHALL be ordered by name, and fields, metrics and relationships in a deterministic order, so that exporting unchanged content twice produces identical documents. Export SHALL NOT access the database directly; any metadata it needs SHALL be read through the query engine.

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

#### Scenario: No extensions strips only SLAYER entries
- **WHEN** a datasource holds only the document `sales`, one of whose datasets carries a `SLAYER` entry holding its `hidden` flag and a `DBT` entry, and is exported with `--no-extensions`
- **THEN** the output equals the stored text without the `SLAYER` entry, and the `DBT` entry and every other line are kept

#### Scenario: Two documents and a standalone dataset merge
- **WHEN** a datasource holds documents `sales` and `support` and the dataset `stores` of its datasource model with a relationship into `sales`'s dataset `orders`, and is exported and imported into an empty datasource
- **THEN** the export is one valid document holding all three models' datasets and the relationship, and the re-import restores the documents `sales` and `support` and the dataset `stores` in the datasource model

#### Scenario: A SLayer row filter exports as a filtered source
- **WHEN** dataset `orders` of the document `sales` carries the `SLAYER` row filter `status <> 'test'`, and the datasource is exported with and without extensions
- **THEN** in both exports the `source` of `orders` is a query returning exactly the rows of its table where the filter holds

#### Scenario: Stored Ossie keys export as stored
- **WHEN** a dataset of the datasource model declares a two-field `unique_keys` entry, a `Date` field with `is_time: false`, and a field whose expression carries a `DUCKDB` entry, and its DuckDB datasource is exported
- **THEN** the exported dataset carries the same `unique_keys` entry, `is_time: false` and the same expression with every dialect entry
