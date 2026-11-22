# importers/osi-export Specification

## Purpose
Exports one datasource's SLayer models as an Ossie (OSI) semantic-model document: everything Ossie defines with SLayer's meaning goes out as Ossie, everything SLayer-only travels in a `SLAYER` custom extension, and anything else is reported, never silently dropped.

## Requirements

### Requirement: Export writes one current-spec document per datasource
`slayer export-osi --datasource X` and the Python export API SHALL write the datasource's semantic model — every stored semantic-model document and every standalone dataset of `X` — as one flat current-spec Ossie document (`version: 0.3.0.dev0`) that validates against the pinned upstream Ossie schema. When `X` holds exactly one semantic-model document and no standalone dataset, no `--datasets` subset is given and extensions are not omitted, the output SHALL be that document's stored text unchanged (converted to JSON only when the output file is JSON). With `--no-extensions`, the output SHALL be that text with only its `SLAYER` entries removed, every other line kept. Otherwise the document SHALL hold every dataset, relationship and model-scoped metric of `X`: document entities as stored, standalone datasets exported by the rules below; the document's `name` and `description` SHALL come from the datasource; and each source document's `name`, `description`, `ai_context`, non-portable extensions and the names of its datasets SHALL be carried in a document-level `SLAYER` entry, so that importing the export into an empty datasource restores each source document. Datasets SHALL be ordered by name, and fields, metrics and relationships in a deterministic order, so that exporting unchanged content twice produces identical documents. Export SHALL NOT access the database directly; any metadata it needs SHALL be read through the query engine.

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
- **WHEN** a datasource holds only the document `sales`, one of whose datasets carries a `SLAYER` entry and a `DBT` entry, and is exported with `--no-extensions`
- **THEN** the output equals the stored text without the `SLAYER` entry, and the `DBT` entry and every other line are kept

#### Scenario: Two documents and a standalone dataset merge
- **WHEN** a datasource holds documents `sales` and `support` and a standalone dataset `stores` with a relationship into `sales`'s dataset `orders`, and is exported and imported into an empty datasource
- **THEN** the export is one valid document holding all three sources' datasets and the relationship, and the re-import restores the documents `sales` and `support` and the standalone dataset `stores`

### Requirement: Model subset

With `--datasets`, only the named models SHALL be exported. A relationship, metric, or SLayer-only entity that needs a model outside the subset SHALL be reported with a workaround naming the model to add.

#### Scenario: Join to an excluded model
- **WHEN** `orders` joins `customers` and the export runs with `--datasets orders`
- **THEN** the document holds dataset `orders` and no relationship to `customers`, and the report names the join and suggests adding `customers` to `--datasets`

### Requirement: Dataset sources

A table-backed model SHALL export with its physical table as `source`. A sql-mode model SHALL export with its SQL as a query `source`. A model with row `filters` that read only its own columns SHALL export with a query `source` selecting all of its source's rows where every filter holds. A query-backed model SHALL export with its generated SQL, under its default variable values, as a query `source`. A model with a row filter that reads a joined model SHALL NOT be exported and SHALL be reported, with the workaround of expressing the restriction in the model's SQL or in query filters. Query `source` SQL SHALL be in the datasource's dialect.

#### Scenario: Filtered table model
- **WHEN** model `active_orders` has `sql_table: orders` and filters `deleted_at IS NULL` and `status <> 'test'`
- **THEN** its dataset `source` is a query returning exactly the rows of `orders` for which both filters hold

#### Scenario: Query-backed model
- **WHEN** model `monthly_revenue` is backed by a saved query with a `{region}` variable defaulting to `US`
- **THEN** its dataset `source` is the query's generated SQL with `region` bound to `US`

#### Scenario: Filter on a joined model
- **WHEN** model `b2b_orders` has the filter `customers.segment = 'b2b'`
- **THEN** no dataset `b2b_orders` is exported and the report names the model, the filter, and the workaround

### Requirement: Fields

Every field whose definition reads only its own dataset SHALL export as a field named by the field's name. Its expression SHALL read the dataset source's physical columns, with each path to another field of the same dataset replaced by that field's definition, on the syntax of the form it appears in. A field with a `filter` SHALL export with the expression `CASE WHEN <filter> THEN <value> END`, so that it is masked in every position, and every filter SHALL be applied exactly once however many definitions are replaced into one another. A field's `label`, `description` and imported Ossie `ai_context` SHALL export as the field's `label`, `description` and `ai_context`; text that the importer appended to a description from `ai_context` SHALL NOT be exported a second time.

#### Scenario: Renamed column
- **WHEN** field `total_amount` has `expression: o_totalprice`
- **THEN** exported field `total_amount` has the expression `o_totalprice`

#### Scenario: Derived column over another derived column
- **WHEN** `net` is `orders.gross - discount` and `gross` is `price * quantity`
- **THEN** field `net`'s expression reads only `price`, `quantity` and `discount`

#### Scenario: Filtered column
- **WHEN** field `completed_amount` has `expression: amount` and `filter: status = 'completed'`, over rows (completed, 10), (open, 5), (completed, 7)
- **THEN** exported field `completed_amount` evaluates to 10, NULL and 7 on those rows

#### Scenario: Imported description is stable
- **WHEN** a document with a field carrying `description` and `ai_context` instructions is imported and then exported
- **THEN** the exported field's `description` equals the original `description`

### Requirement: Field datatypes and time role

A field's `datatype` SHALL be `String` for TEXT, `Integer` for INT, `Float` for DOUBLE (`Decimal` when the column records an exact-numeric database type), `Boolean` for BOOLEAN, `Date` for DATE, and `DateTime` or `DateTimeTz` for TIMESTAMP according to the recorded database type; it SHALL be omitted for UNKNOWN. An imported Ossie datatype recorded on the column SHALL be used when it is consistent with the column's type. `dimension.is_time` SHALL be written only where Ossie's default from `datatype` differs from the column's time role: `false` for a temporal column recorded as not a time dimension, `true` for a non-temporal column recorded as one.

#### Scenario: Exact decimal
- **WHEN** a DOUBLE column records the database type `DECIMAL(18,2)`
- **THEN** its field's `datatype` is `Decimal`

#### Scenario: Temporal opt-out survives
- **WHEN** an imported field declared `datatype: DateTime` and `dimension: {is_time: false}` is exported
- **THEN** the exported field carries `dimension: {is_time: false}`

#### Scenario: Integer year keeps its role
- **WHEN** an imported field declared `datatype: Integer` and `dimension: {is_time: true}` is exported
- **THEN** the exported field carries `dimension: {is_time: true}`

### Requirement: Keys

A model's primary-key columns SHALL export as `primary_key`, and its single-column unique columns and imported unique-key sets as `unique_keys`, deduplicated. Every key member SHALL be an exported field that is a plain, unfiltered column; a key set naming anything else SHALL be omitted and reported individually.

#### Scenario: Key on a filtered column
- **WHEN** a unique-key set names a column that has a `filter`
- **THEN** that set is not exported and the report names it

### Requirement: Relationships

Each join SHALL export as one relationship whose `from` is the many side and `to` is the one side, keyed by field names: a many-to-one or one-to-one join from its model to its target, and a one-to-many join from its target to its model with its key pairs swapped. A many-to-one join and its mirrored one-to-many join on the same keys SHALL export as one relationship. A join whose cardinality is undetermined SHALL export only when the declared keys prove a direction (the target side's keys are its primary key or a unique key, or else the source side's keys are), and SHALL otherwise be reported with the workaround of setting the join's cardinality. Many-to-many joins SHALL be reported. A relationship's `name` SHALL be the join's name, else a generated name; generated names SHALL be unique in the document.

#### Scenario: Many-to-one join
- **WHEN** `orders` joins `customers` many-to-one on `[customer_id, id]`
- **THEN** the document has a relationship `from: orders`, `to: customers`, `from_columns: [customer_id]`, `to_columns: [id]`

#### Scenario: One-to-many join
- **WHEN** `customers` joins `orders` one-to-many on `[id, customer_id]` and `orders` declares no join to `customers`
- **THEN** the document has a relationship `from: orders`, `to: customers`, `from_columns: [customer_id]`, `to_columns: [id]`

#### Scenario: Undetermined cardinality proven by keys
- **WHEN** a join with no cardinality targets `customers` on `customers.id`, its primary key
- **THEN** it exports as a relationship to `customers`

#### Scenario: Undetermined cardinality not provable
- **WHEN** a join with no cardinality keys on columns that are a primary or unique key on neither side
- **THEN** no relationship is exported for it and the report suggests setting its cardinality

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

### Requirement: Exporting without extensions

With extensions disabled, no `SLAYER` extension SHALL be written. Every entity that exists only in an extension SHALL be reported as not exported, and every exported entity whose SLayer meaning a plain Ossie reader cannot restore (for example a one-to-one or one-to-many join, an inner join, or a measure exported away from its model) SHALL be reported as lossy. A model with access tags SHALL NOT be exported and SHALL be reported, because re-importing it without its tags would make it public.

#### Scenario: Access-tagged model without extensions
- **WHEN** model `payroll` has access tags and the export runs with `--no-extensions`
- **THEN** no dataset `payroll` is exported and the report names it

#### Scenario: Inner join without extensions
- **WHEN** an inner join is exported with `--no-extensions`
- **THEN** its relationship is exported and the report lists the join as lossy

### Requirement: Export report

The export SHALL return and print a report in which every entry names its entity, its reason and a workaround, with severity `dropped` for an entity not exported and `info` for an entity carried only in the extension (category `extension_only`) or exported lossily (category `lossy`). Datasource connection details, memories and sampled values SHALL NOT be exported and SHALL NOT be reported.

#### Scenario: Complete report
- **WHEN** a datasource with one many-to-many join and one transform measure is exported
- **THEN** the report holds exactly a `dropped` entry for the join and an `info`/`extension_only` entry for the measure, each with a workaround

### Requirement: Round trip
Importing an exported document with `import-osi` into an empty datasource SHALL yield datasets, relationships and model-scoped metrics on which every exported and extension-carried field and metric returns the same results as the original, grouped and ungrouped, on DuckDB and SQLite; restored metric expressions SHALL be identical to the originals, and stored documents SHALL be restored with their original text.

#### Scenario: Worked example round trip
- **WHEN** `docs/examples/13_osi_import/shop.osi.yaml` is imported, exported and imported again
- **THEN** every field and metric returns the same results after the second import as after the first, and the stored text is unchanged

#### Scenario: Formula restored
- **WHEN** a model with measure `aov: revenue / n` is exported and re-imported
- **THEN** the re-imported metric's expression is `revenue / n`

### Requirement: YAML export quotes strings other loaders would retype

A YAML export SHALL quote every string scalar that a YAML 1.1 or a YAML 1.2 core-schema loader would read as anything other than that string (a number in any sign, case, exponent, octal or hex spelling, a boolean, null, a date or timestamp, infinity or NaN), wherever it occurs in the document, so that every such loader reads back the same strings.

#### Scenario: Retypable strings round-trip
- **WHEN** a column's metadata recorded in its `SLAYER` payload holds the strings `1e3`, `1E3`, `+1e3`, `-1.5E+3`, `0o17`, `0x1F`, `.Inf`, `NULL`, `yes`, `010` and `2024-01-01`, and the export is written as YAML
- **THEN** a YAML 1.1 loader and a YAML 1.2 core-schema loader both read each value back as the same string

### Requirement: Instant comparisons export as standard SQL

A comparison of a temporal value with an instant — a date-time literal or a date-only literal, which is the instant at its midnight — SHALL export inside a metric as a standard-SQL comparison with the same operator and literal, so the metric is not dropped as SLayer-only on that account. A comparison with a period literal other than a date-only one, a single-string `in` / `not in`, or a relative token SHALL remain SLayer-only. Querying the re-imported metric SHALL return the value of the original measure.

#### Scenario: Date-only bound in a filtered measure

- **WHEN** `ev` has the measure `early: count(*) FILTER (WHERE ts <= '2024-04-10')` over rows at `2024-03-05 08:00` and `2024-04-10 09:30`
- **THEN** dataset `ev` has the metric `early` whose `sql` compares `ts <= '2024-04-10'`, and the re-imported measure returns 1, as the original does

#### Scenario: Date-time bound

- **WHEN** a measure is `count(*) FILTER (WHERE ts > '2024-04-10 09:00:00')`
- **THEN** it exports as a metric with that comparison

#### Scenario: Period and relative comparisons stay SLayer-only

- **WHEN** measures are `count(*) FILTER (WHERE ts in '2024-04-10')`, `count(*) FILTER (WHERE ts = '2025-Q1')` and `count(*) FILTER (WHERE ts >= 'last month')`, exported without extensions
- **THEN** none is exported as a metric, and each is reported as SLayer-only

### Requirement: Each expression form exports as itself

A field's portable `sql` SHALL export as the field's `sql`, and each `dialects` entry SHALL export as a `dialects` entry with its text; an entry tagged with the datasource's own dialect, or a field's `sql` not yet converted, SHALL export under the Ossie token for the datasource's type. Every supported datasource type SHALL map to exactly one Ossie dialect token, which the importer reads as that datasource's own dialect. Datasource-dialect text SHALL never be exported as portable `sql`. When a field's definition has another field's definition replaced into it, the result SHALL be portable `sql` only when every replaced part is portable, and otherwise the entry for the datasource's dialect.

#### Scenario: Portable expression
- **WHEN** a DuckDB field is `expression: "price * quantity"`
- **THEN** its exported field has `sql: price * quantity` and no `dialects`

#### Scenario: Dialect-only expression
- **WHEN** a Postgres field is `{dialects: [{dialect: POSTGRES, expression: "amount / qty"}]}`
- **THEN** its exported field has no `sql` and one `dialects` entry `POSTGRES` holding `amount / qty`

#### Scenario: A portable field over a dialect field
- **WHEN** a Postgres field `ratio` is `expression: "orders.part * 2"` and `part` is `{dialects: [{dialect: POSTGRES, expression: "amount / qty"}]}`
- **THEN** exported `ratio` has no `sql` and one `POSTGRES` entry reading `(amount / qty) * 2`

#### Scenario: Export and re-import keep values
- **WHEN** a dataset with a derived, a filtered, a quoted and a dialect-only field is exported and imported again into the same DuckDB datasource
- **THEN** every field returns the same values as before
