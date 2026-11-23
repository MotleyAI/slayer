## MODIFIED Requirements

### Requirement: SLayer-only content travels in a SLAYER extension

Unless extensions are disabled, every exported entity SHALL carry exactly one `nonportable_extensions` entry with `vendor_name: SLAYER`, whose `data` is a versioned payload object holding only what Ossie has no place for: a join's type, name, cardinality and declaring models; a measure's formula, model, name, label, type and metadata; a column's hidden flag, format, granularity, allowed aggregations, metadata, and its type and value/filter split; a model's hidden flag, access tags, default time dimension, metadata, custom aggregations, source structure (table or SQL plus row filters, or saved queries plus variables), and its SLayer-only columns and measures. SLayer-only columns are those reading joined models. SLayer-only measures are those not exportable as metrics: transforms, ranked `first`/`last`, windowed or partitioned aggregates, aggregates reading several models, and measures whose route to a model is ambiguous. Each payload SHALL carry a fingerprint of the Ossie content it was exported with. Non-portable extensions of other vendors recorded on an imported entity or document, including on models stored before SLayer recorded them as objects, SHALL be re-emitted under `nonportable_extensions` with their `data` object unchanged, whether or not extensions are disabled, after validation against the schema's extension shape; one whose `data` is not a JSON-representable object SHALL be reported and omitted. No exported document SHALL carry the key `custom_extensions`.

#### Scenario: Transform measure
- **WHEN** `orders` has the measure `revenue_growth: change_pct(revenue)`
- **THEN** no metric `revenue_growth` is exported, `orders`' extension carries it, and the report lists it as SLayer-only

#### Scenario: Cross-model operand
- **WHEN** `orders` has the measure `discounted: sum(amount * customers.rate)`
- **THEN** no metric `discounted` is exported and `orders`' extension carries it

#### Scenario: Column reading a joined model
- **WHEN** `orders` has the column `segment` with `sql: customers.segment`
- **THEN** dataset `orders` has no field `segment`, and its extension carries the column

#### Scenario: Foreign extension preserved
- **WHEN** an imported field carried a non-portable extension `{vendor_name: ACME, data: {k: 1}}`
- **THEN** the exported field carries that extension with `data` unchanged, next to its `SLAYER` extension, and the document validates against the vendored schema

#### Scenario: Document-level foreign extension
- **WHEN** the exported models were imported from a document whose root carried `{vendor_name: DBT, data: {project_name: shop}}`, and the export runs with and without extensions
- **THEN** both exported documents carry that entry at their root and validate against the vendored schema

#### Scenario: Model stored before the shape change
- **WHEN** a model stored by SLayer 1.8 records a field's foreign extension as `{vendor_name: ACME, data: '{"k": 1}'}` and another as `{vendor_name: ACME, data: 'not json'}`
- **THEN** the exported field carries the first with `data: {k: 1}`, omits the second, and the report names the second as malformed

## ADDED Requirements

### Requirement: YAML export quotes strings other loaders would retype

A YAML export SHALL quote every string scalar that a YAML 1.1 or a YAML 1.2 core-schema loader would read as anything other than that string (a number in any sign, case, exponent, octal or hex spelling, a boolean, null, a date or timestamp, infinity or NaN), wherever it occurs in the document, so that every such loader reads back the same strings.

#### Scenario: Retypable strings round-trip
- **WHEN** a column's metadata recorded in its `SLAYER` payload holds the strings `1e3`, `1E3`, `+1e3`, `-1.5E+3`, `0o17`, `0x1F`, `.Inf`, `NULL`, `yes`, `010` and `2024-01-01`, and the export is written as YAML
- **THEN** a YAML 1.1 loader and a YAML 1.2 core-schema loader both read each value back as the same string
