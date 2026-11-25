## MODIFIED Requirements

### Requirement: The Ossie corpus loads, compiles and round-trips
A corpus vendored with provenance pinning the upstream apache/ossie commit SHALL consist of every semantic-model document of upstream `examples/`, `compliance/portability/` (m0–m8), the `compliance/wide_table/` models and `compliance/query/scott`, plus every document upstream's `validation/tests` assert as valid, harvested into fixture files; it SHALL also hold upstream's validator, schema, loader, the core-spec text (`spec.md`, `spec.yaml`, `expression_language.md`, `relational_semantics.md`, `wide_table_semantics.md`, `spec_layers.md`) and the expression fixtures `compliance/expressions/valid.txt` and `invalid.txt`. Re-vendoring SHALL be one command over an apache/ossie checkout that copies every file byte for byte, records each file's upstream blob sha and the commit, removes only files the previous provenance listed and upstream no longer has, and leaves the previous corpus intact when any step fails. For every corpus document: upstream `validation/validate.py` SHALL pass; the native load SHALL succeed; every dataset-scoped and model-scoped metric SHALL bind and compile to SQL in every Tier-1 dialect, except entities SLayer cannot evaluate yet, which SHALL raise their typed error in every dialect; storing, loading, saving unchanged and exporting a single-document datasource SHALL each reproduce the document's text byte for byte.

#### Scenario: Every corpus document round-trips
- **WHEN** each corpus document is imported into its own empty datasource and exported
- **THEN** the export equals the original text byte for byte

#### Scenario: The source-measure document loads and round-trips
- **WHEN** upstream's harvested document with `orders.revenue_total = AGG(source.revenue)`, `orders.revenue_per_order = AGG(source.revenue) / COUNT(DISTINCT order_id)` and `revenue_per_customer = AGG(orders.revenue_total) / COUNT(DISTINCT customers.id)` is stored, loaded, saved unchanged and exported
- **THEN** the native check accepts it, all three metrics raise the typed error for entities SLayer cannot evaluate yet, and each step reproduces its text byte for byte

#### Scenario: A failed re-vendor keeps the corpus
- **WHEN** the re-vendor command runs over a checkout whose validator tests fail during the harvest
- **THEN** the command exits non-zero and every vendored file and the provenance file are unchanged

#### Scenario: Corpus metrics compile in every Tier-1 dialect
- **WHEN** each corpus metric is compiled with a dry run on a datasource of each Tier-1 type
- **THEN** it yields SQL in every dialect, or the same typed error in every dialect

### Requirement: Upstream's wide-table cases are the query oracle
Every case of upstream `compliance/wide_table/cases` SHALL run against its named fixture model, stored as a native document over its SQL data seeded into each Tier-1 database (`compatibility/ossie-compliance`), with its `query.json` submitted verbatim under `conformance="ossie-l3"` (`queries/ossie-l3-binding`). A row case (`a*`) SHALL return exactly the expected rows, compared as a multiset by logical type, with the result columns named and ordered as the expected header; an error case (`e*`) SHALL raise its expected Ossie code. Every row case SHALL also return exactly the expected rows under the default conformance.

#### Scenario: The model-scoped metric components case
- **WHEN** case `a08_model_metric_components` runs on the `retail` fixture
- **THEN** the result equals its expected rows on every Tier-1 database

#### Scenario: Error cases raise their codes under strict
- **WHEN** case `e16_partial_dimension_reachability` runs on the `retail` fixture under `conformance="ossie-l3"`
- **THEN** it raises `E_PARTIAL_DIMENSION_REACHABILITY`, while the same query under the default conformance answers

#### Scenario: Output names label the strict result
- **WHEN** case `a09_aliases` runs under `conformance="ossie-l3"`
- **THEN** the result columns are exactly `area`, `revenue`, in that order

#### Scenario: A NULL group matches on every database
- **WHEN** case `a17_unmatched_home_null_group` runs on SQL Server, ClickHouse and BigQuery
- **THEN** its NULL-keyed row equals the expected row on each
