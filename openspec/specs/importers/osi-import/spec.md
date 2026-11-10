# importers/osi-import Specification

## Purpose
Imports Ossie (OSI) semantic-model documents into SLayer models: datasets become models, relationships become joins, metrics become measures, and anything that cannot be expressed exactly is reported rather than silently lost.

## Requirements

### Requirement: Current-spec documents import

The importer SHALL accept a flat Ossie document that holds one semantic model at its root (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `custom_extensions`). Each such document SHALL be imported as one semantic model with the current-spec rules, whatever version it declares. The versions `1.0`, `0.1.0`, `0.1.1`, `0.2.0.dev0` and `0.3.0.dev0` SHALL parse without a warning; any other declared version SHALL be warned about and still parsed.

#### Scenario: Flat document imports end to end
- **WHEN** a file holds `version: 0.3.0.dev0`, `name: shop`, two datasets `orders` and `customers` with declared fields, a relationship `orders → customers`, and a model-scoped metric `SUM(orders.amount)`
- **THEN** the import produces models `orders` and `customers`, a many-to-one join from `orders` to `customers`, and the measure on `orders`, with no unconverted entries and no version warning

#### Scenario: Earlier flat document keeps importing
- **WHEN** the same document declares `version: 0.2.0.dev0`
- **THEN** it imports with the same models, join and measure, and no version warning

#### Scenario: Unknown version
- **WHEN** a flat document declares `version: 0.4.0`
- **THEN** a warning names the version and the document imports

### Requirement: Legacy wrapper documents keep importing

A document whose root holds a `semantic_model` key and that does not declare `0.3.0.dev0` SHALL be treated as a legacy document. When `semantic_model` is an array, each element SHALL be imported as its own semantic model; when it is a single object, it SHALL be imported as one semantic model. No model in the wrapper SHALL be dropped.

#### Scenario: Legacy array with two models
- **WHEN** a file holds `semantic_model: [ {name: a, datasets: [...]}, {name: b, datasets: [...]} ]`
- **THEN** the datasets of both `a` and `b` are imported

#### Scenario: Legacy object-valued wrapper
- **WHEN** a file holds `semantic_model: {name: a, datasets: [...]}`
- **THEN** the datasets of `a` are imported

#### Scenario: Wrapper without a version
- **WHEN** a file holds `semantic_model: [...]` and no `version`
- **THEN** its models are imported as legacy and are not reported as declaring any version

### Requirement: Invalid document shapes are skipped with a warning

A file SHALL be skipped with a logged warning, and the rest of the import SHALL continue, when its root mixes both shapes (a `semantic_model` key together with a root `datasets` key), holds neither, holds a `semantic_model` value that is empty, null, or not an array or object, or holds a `semantic_model` key while declaring `version: 0.3.0.dev0`, which admits only flat documents.

#### Scenario: Mixed shape
- **WHEN** a file holds both `semantic_model: [...]` and `datasets: [...]` at its root
- **THEN** that file is skipped with a warning naming it, and other files in the same import are imported

#### Scenario: Empty legacy wrapper
- **WHEN** a file holds `semantic_model: []`
- **THEN** that file is skipped with a warning

#### Scenario: Wrapped 0.3 document
- **WHEN** a file declares `version: 0.3.0.dev0` and holds `semantic_model: [...]`, and another file in the same import is a valid flat document
- **THEN** the wrapped file is skipped with a warning naming it and saying `0.3.0.dev0` documents are flat, and the flat file imports

#### Scenario: Ontology document
- **WHEN** a file is an Ossie ontology or ontology-mapping document (no root `datasets` and no `semantic_model`)
- **THEN** that file is skipped with a warning naming it

### Requirement: Expressions carry an optional portable form and optional dialect entries

An expression SHALL be accepted with a top-level `sql` string, a `dialects` list, or both. A `dialect` SHALL be accepted as any non-empty string. A field or metric whose expression has neither a `sql` nor a usable `dialects` entry SHALL be reported for that entity, and the rest of the document SHALL still import.

#### Scenario: Expression with only top-level sql
- **WHEN** a field is declared as `expression: {sql: "amount"}`
- **THEN** the field imports

#### Scenario: Open dialect token
- **WHEN** a field's only dialect entry is `{dialect: OSSIE_SQL_2026, expression: amount}`
- **THEN** the field imports

#### Scenario: Expression with neither form
- **WHEN** a metric declares `expression: {}`
- **THEN** that metric is reported and the document's other entities import

### Requirement: Deterministic dialect selection

For each expression the importer SHALL select exactly one candidate, taking the first that exists and is readable in this order: the dialect named by the `--dialect` option; the top-level `sql`; an `OSSIE_SQL_2026` entry; an `ANSI_SQL` entry; an entry whose dialect is the target datasource's own; any other readable SQL dialect entry. Within one step the first entry in document order SHALL win. A readable dialect is `OSSIE_SQL_2026`, `ANSI_SQL`, or a token naming a SQL dialect the importer can parse, matched case-insensitively. Selecting a candidate from the last step SHALL add an informational report entry naming the dialect used. When `--dialect` is not given, no override step applies.

#### Scenario: Portable form preferred over vendor dialect
- **WHEN** a field has `sql: "amount"` and a `SNOWFLAKE` entry `amount::NUMBER`, and the target is a Snowflake datasource
- **THEN** the field's definition comes from `sql`

#### Scenario: ANSI preferred over target-native
- **WHEN** a field has an `ANSI_SQL` entry and a `POSTGRES` entry, and the target is a Postgres datasource
- **THEN** the field's definition comes from the `ANSI_SQL` entry

#### Scenario: Override first
- **WHEN** the import runs with `--dialect SNOWFLAKE` and a field has `sql` plus a `SNOWFLAKE` entry
- **THEN** the field's definition comes from the `SNOWFLAKE` entry

#### Scenario: Unreadable override falls through
- **WHEN** the import runs with `--dialect MDX` and a field has an `MDX` entry and an `ANSI_SQL` entry
- **THEN** the field's definition comes from the `ANSI_SQL` entry

#### Scenario: Foreign vendor dialect as last resort
- **WHEN** a field's only entry is `BIGQUERY` and the target is a DuckDB datasource
- **THEN** the field imports from the `BIGQUERY` entry and the report holds an informational entry naming `BIGQUERY`

#### Scenario: Datasource type aliases
- **WHEN** the target datasource type is `postgresql` and a field's only entries are `MDX` and `POSTGRES`
- **THEN** the `POSTGRES` entry is treated as the target's own dialect and selected

### Requirement: Unreadable expressions are reported, never raised

When no candidate of an expression is readable, the entity SHALL be reported as not converted, naming the dialects found, and the import SHALL NOT raise.

#### Scenario: Non-SQL dialects only
- **WHEN** a metric's only entries are `MDX` and `DAX`
- **THEN** the metric is reported as unconverted with a reason naming `MDX` and `DAX`, and the import completes

### Requirement: Selected expressions are rendered for the target datasource

The selected expression SHALL be read with the conventions of its own dialect, where `OSSIE_SQL_2026` and `ANSI_SQL` read Ossie's portable function set (including `IFF`, `ZEROIFNULL`, `DATEADD`, `STARTSWITH` and `||`), and its unquoted identifiers SHALL be read with the target datasource's identifier folding. Every raw-SQL definition the import stores (a column's SQL, a column's filter) SHALL be rendered in the target datasource's SQL dialect. An expression whose dialect is the target datasource's own SHALL be stored verbatim, apart from the identifier folding of "Imported SQL reads with the source's identifier folding" (`importers/meaning-preservation`). An expression that cannot be parsed SHALL be reported for its entity.

#### Scenario: Portable function rendered for Postgres
- **WHEN** a field is `sql: "IFF(amount > 0, amount, 0)"` and the target is a Postgres datasource
- **THEN** the stored column SQL is valid Postgres and querying the column returns `amount` for positive amounts and `0` otherwise

#### Scenario: Concatenation on MySQL
- **WHEN** a field is `sql: "first_name || ' ' || last_name"` and the target is a MySQL datasource
- **THEN** the stored column SQL concatenates the strings on MySQL

#### Scenario: Target-native kept verbatim
- **WHEN** the selected entry is `SNOWFLAKE` with expression `amount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the stored column SQL is exactly `amount::NUMBER(10,2)`

#### Scenario: Target-native mixed-case identifier folded
- **WHEN** the selected entry is `SNOWFLAKE` with expression `OrderAmount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the stored column SQL is exactly `orderamount::NUMBER(10,2)`

#### Scenario: Portable expression folded by the target
- **WHEN** a field is `sql: "IFF(OrderAmount > 0, OrderAmount, 0)"` and the target is a Postgres datasource
- **THEN** the stored column SQL reads the physical column `orderamount`

#### Scenario: Unparseable expression
- **WHEN** a field's selected expression is `amount +* 2`
- **THEN** that field is reported and the dataset's other fields import

### Requirement: Current-spec models expose their declared fields

For a current-spec document, each dataset's model SHALL expose exactly its declared fields as visible columns (plus any hidden helper columns the import needs: metric operands). Source columns that no field declares SHALL NOT appear as visible columns of the model. A field expression SHALL still be able to read any column of the dataset's source, with the source's meaning, unless another field holds that column's name, in which case the reading field is dropped with a report. A dataset that declares no fields SHALL still import, with a warning that it exposes no columns.

#### Scenario: Renamed field hides its source column
- **WHEN** dataset `orders` has source columns `o_orderkey` and `o_totalprice` and declares fields `order_id: o_orderkey` and `total_amount: o_totalprice`
- **THEN** model `orders` has columns `order_id` and `total_amount` and no column `o_orderkey` or `o_totalprice`, and querying `total_amount` returns the values of `o_totalprice`

#### Scenario: Derived field over an undeclared source column
- **WHEN** a dataset declares only `total_usd: total_cents / 100.0` and `total_cents` is a source column
- **THEN** model column `total_usd` imports and returns `total_cents / 100.0`

#### Scenario: Field reading a source column another field's name has taken
- **WHEN** a dataset declares fields `status: state` and `status_upper: UPPER(status)` over source columns `status` and `state`
- **THEN** `status_upper` is dropped with a report naming `status`, and the visible columns are exactly `status`

#### Scenario: Dataset without fields
- **WHEN** a current-spec dataset declares no fields
- **THEN** its model imports with no visible columns and the report holds a warning for that dataset

### Requirement: Field expressions are validated against the live source

Every unqualified column a field expression reads SHALL exist in the dataset's live source; a field reading a column the source does not have SHALL be reported and skipped.

#### Scenario: Typo in a field expression
- **WHEN** a field is `total_usd: total_cnets / 100.0` and the source has no column `total_cnets`
- **THEN** the field is reported as referencing an unknown column and is not on the model

### Requirement: Keys and relationships name declared fields

For a current-spec document, every name in `primary_key`, `unique_keys`, `from_columns` and `to_columns` SHALL resolve to a declared field of the owning dataset (for relationships, the `from` and `to` dataset respectively). A name that is not a declared field SHALL be reported, and the key or relationship SHALL NOT be applied. A relationship whose key field is a computed expression or a filtered column SHALL be reported as not representable. A relationship SHALL resolve its `from` and `to` datasets only within its own document.

#### Scenario: Relationship on a renamed field
- **WHEN** field `customer_id: o_custkey` is declared on `orders`, field `id: c_custkey` on `customers`, and a relationship uses `from_columns: [customer_id]`, `to_columns: [id]`
- **THEN** the join is created and a query across it joins `o_custkey` to `c_custkey`

#### Scenario: Relationship on a source column
- **WHEN** a relationship uses `from_columns: [o_custkey]` and no field named `o_custkey` is declared
- **THEN** the relationship is reported and no join is created

#### Scenario: Computed join key
- **WHEN** a relationship's `from_columns` names a field declared as `UPPER(cust_code)`
- **THEN** the relationship is reported as not representable

#### Scenario: Primary key on a source column
- **WHEN** a current-spec dataset declares `primary_key: [o_orderkey]` and no field named `o_orderkey`
- **THEN** the primary key is reported and not applied

#### Scenario: Cross-document relationship
- **WHEN** document 1 declares dataset `orders` and a relationship to `customers`, and `customers` is declared only in document 2 of the same import
- **THEN** the relationship is reported and no join is created

### Requirement: Primary keys

A valid declared `primary_key` SHALL replace the introspected primary key. When a dataset declares no `primary_key` at all, a field that renames an introspected primary-key column SHALL keep the primary-key flag. A `primary_key` or a `unique_keys` entry that is present but empty SHALL be reported and ignored, and SHALL NOT count as absent.

#### Scenario: Renamed introspected key without declaration
- **WHEN** the source's primary key is `o_orderkey`, the dataset declares field `order_id: o_orderkey`, and declares no `primary_key`
- **THEN** column `order_id` is the model's primary key

#### Scenario: Empty primary key
- **WHEN** a dataset declares `primary_key: []` and the source's primary key is `o_orderkey` renamed by field `order_id`
- **THEN** the empty key is reported and the model has no primary key

#### Scenario: Empty unique key entry
- **WHEN** a dataset declares `unique_keys: [[], [email]]`
- **THEN** the empty entry is reported and `[email]` is kept

### Requirement: Metric name resolution

For a current-spec document, a model-scoped metric SHALL reference fields only as `dataset.field`, and a dataset-scoped metric SHALL reference unqualified fields of its own dataset and, as `source.column`, columns of its own dataset's source (see "Source columns in dataset-scoped metrics"). A reference that is unqualified in a model-scoped metric, qualified or foreign in a dataset-scoped metric (other than a `source.column` reference), names an undeclared field, or names a source column without the `source` qualifier SHALL make the metric reported as unconverted.

#### Scenario: Unqualified reference in a model-scoped metric
- **WHEN** a model-scoped metric is `SUM(amount)` and only `orders` declares `amount`
- **THEN** the metric is reported as unconverted

#### Scenario: Source column in a metric
- **WHEN** a model-scoped metric is `SUM(orders.o_totalprice)` and `orders` declares `total_amount: o_totalprice` but no field `o_totalprice`
- **THEN** the metric is reported as unconverted

#### Scenario: Unqualified source column in a dataset-scoped metric
- **WHEN** dataset `orders` declares `total_amount: o_totalprice` but no field `o_totalprice`, and its metric is `SUM(o_totalprice)`
- **THEN** the metric is reported as unconverted

#### Scenario: Foreign reference in a dataset-scoped metric
- **WHEN** dataset `orders` declares metric `SUM(net_amount) / COUNT(DISTINCT customers.id)`
- **THEN** the metric is reported as unconverted

#### Scenario: Metric reading a renamed field
- **WHEN** dataset `orders` declares `total_amount: o_totalprice` and a model-scoped metric is `SUM(orders.total_amount)`
- **THEN** the measure imports and returns the sum of `o_totalprice`

### Requirement: Identifier case in metric references

An unquoted identifier in a metric expression SHALL match a declared field or dataset name case-insensitively; a double-quoted identifier SHALL match exactly. When an unquoted identifier matches more than one declared name, the metric SHALL be reported as ambiguous. The stored measure SHALL reference fields by their declared names.

#### Scenario: Case-insensitive match
- **WHEN** dataset `orders` declares field `amount` and a dataset-scoped metric is `SUM(Amount)`
- **THEN** the measure imports and its formula references `amount`

#### Scenario: Quoted exact match
- **WHEN** dataset `orders` declares field `Amount` and a dataset-scoped metric is `SUM("amount")`
- **THEN** the metric is reported as unconverted

#### Scenario: Case-only ambiguity
- **WHEN** dataset `orders` declares fields `id` and `ID` and a dataset-scoped metric is `COUNT(DISTINCT id)`
- **THEN** the metric is reported as ambiguous

### Requirement: Legacy documents keep lenient resolution

For a legacy-wrapper document, each model SHALL expose all introspected source columns plus its fields, and metric, key and relationship names SHALL resolve against both, with an unqualified metric reference resolving when exactly one dataset of its semantic model owns that name. Expression, dialect, `FILTER`, datatype and dataset-scoped metric handling SHALL be the same as for current-spec documents, except that a `source.column` reference SHALL NOT be read as a source column.

#### Scenario: Legacy unqualified metric
- **WHEN** a legacy document's metric is `SUM(amount)` and only `orders` has a column `amount`
- **THEN** the measure imports on `orders`

#### Scenario: Legacy relationship on a source column
- **WHEN** a legacy document's relationship uses `from_columns: [customer_id]`, a source column of `orders` that no field declares
- **THEN** the join is created

#### Scenario: Legacy source qualifier
- **WHEN** a legacy document's dataset `orders` declares metric `SUM(source.amount)` and no dataset is named `source`
- **THEN** the metric is reported as unconverted

### Requirement: Dataset-scoped metrics become measures on their dataset

Each metric in `datasets[].metrics` SHALL become a measure on that dataset's model. A dataset-scoped metric whose name collides with a column or another measure on that model SHALL be reported as unconverted.

#### Scenario: Dataset-scoped metric
- **WHEN** dataset `orders` declares field `amount` and metric `total_amount: SUM(amount)`
- **THEN** model `orders` has measure `total_amount` and querying it returns the sum of `amount`

#### Scenario: Name collision with a field
- **WHEN** dataset `orders` declares field `revenue` and dataset-scoped metric `revenue: SUM(amount)`
- **THEN** the metric is reported as unconverted

### Requirement: FILTER aggregates

`agg(x) FILTER (WHERE p)` SHALL import as the aggregation over only the rows where `p` holds, for every supported aggregation including `COUNT(*)`, `COUNT(DISTINCT x)` and percentile aggregates. The predicate SHALL be row-level and SHALL read only the dataset of the aggregate's arguments; a predicate containing an aggregate, a window or a subquery, or reading another dataset, SHALL make the metric reported as unconverted. `SUM`, `AVG`, `MIN` and `MAX` over `DISTINCT` SHALL remain reported as unconverted, with or without `FILTER`. A `FILTER` SHALL NOT remove result groups.

#### Scenario: Filtered sum
- **WHEN** a dataset-scoped metric is `SUM(amount) FILTER (WHERE status = 'completed')` over rows (completed, 10), (open, 5), (completed, 7)
- **THEN** the measure returns 17

#### Scenario: Filtered row count
- **WHEN** a dataset-scoped metric is `COUNT(*) FILTER (WHERE status = 'completed')` over the same rows
- **THEN** the measure returns 2

#### Scenario: Filtered distinct count
- **WHEN** a dataset-scoped metric is `COUNT(DISTINCT customer_id) FILTER (WHERE status = 'completed')` over rows (completed, c1), (completed, c1), (open, c2), (completed, c3)
- **THEN** the measure returns 2

#### Scenario: Groups are not removed
- **WHEN** the filtered sum above is queried grouped by `status`
- **THEN** the result holds a row for `open` whose value is NULL

#### Scenario: Aggregate inside the predicate
- **WHEN** a metric is `SUM(amount) FILTER (WHERE amount > AVG(amount))`
- **THEN** the metric is reported as unconverted

#### Scenario: Predicate on another dataset
- **WHEN** a model-scoped metric is `SUM(orders.amount) FILTER (WHERE customers.segment = 'b2b')`
- **THEN** the metric is reported as unconverted

#### Scenario: Sum over distinct
- **WHEN** a metric is `SUM(DISTINCT amount) FILTER (WHERE status = 'completed')`
- **THEN** the metric is reported as unconverted

### Requirement: Aggregate operands

A non-bare aggregate operand (arithmetic, `CASE`, scalar functions, or, in a model-scoped metric, an operand combining fields of several datasets) SHALL be carried in the measure's formula when the formula language can express it. Otherwise the importer SHALL create a hidden helper column holding the operand on its single owning dataset, typed from the database. An operand that is neither expressible nor confined to one dataset SHALL make the metric reported as unconverted. A string literal in a metric SHALL keep its value exactly, including any `'` (written `''`) and backslash characters.

#### Scenario: Arithmetic operand
- **WHEN** a dataset-scoped metric is `SUM(quantity * amount)`
- **THEN** the measure returns the sum of `quantity * amount` and the model gains no hidden column for it

#### Scenario: Cross-dataset operand
- **WHEN** a model-scoped metric is `SUM(orders.amount - customers.discount)` and `orders` joins `customers` many-to-one
- **THEN** the measure imports and returns the sum over orders of `amount - discount` of each order's customer

#### Scenario: Operand needing a database-specific function
- **WHEN** a dataset-scoped metric aggregates a function the formula language does not offer
- **THEN** the model gains a hidden column holding the operand and the measure aggregates it

#### Scenario: Quote and backslash in a literal
- **WHEN** a dataset-scoped metric is `COUNT(*) FILTER (WHERE note = 'O''Brien\x')` and one row's `note` is `O'Brien\x`
- **THEN** the measure imports and returns 1

### Requirement: Field datatypes

A field's `datatype` SHALL map to SLayer types as: `String`→TEXT, `Integer`→INT, `Decimal` and `Float`→DOUBLE, `Boolean`→BOOLEAN, `Date`→DATE, `DateTime` and `DateTimeTz`→TIMESTAMP, `Time` and `Opaque`→UNKNOWN. A column's type SHALL be the type the database reports for it; when the database cannot report one, the mapped `datatype` SHALL be used, then the type of a source column the field shares its name with, and DOUBLE only when none exists. When both exist and disagree, the report SHALL hold an informational entry. The declared value SHALL be kept in the column's metadata. A column whose database type is exact-numeric or opaque SHALL keep the database's type name.

#### Scenario: Declared type when the database cannot report one
- **WHEN** a derived field `UPPER(name)` declares `datatype: String` and the database type probe fails
- **THEN** the column is TEXT

#### Scenario: Database type wins
- **WHEN** a field over an INTEGER source column declares `datatype: String`
- **THEN** the column is INT and the report holds an informational entry naming the field

#### Scenario: Time of day
- **WHEN** a field declares `datatype: Time` and the database reports no type
- **THEN** the column is UNKNOWN

#### Scenario: Integer expression
- **WHEN** a derived field `LENGTH(name)` over a DuckDB text column declares `datatype: Integer`, and a dataset-scoped metric aggregates a function the formula language does not offer over an integer operand
- **THEN** the field's column and the metric's hidden operand column are INT, and the report holds no datatype entry for the field

#### Scenario: Exact decimal keeps its database type
- **WHEN** a query-sourced dataset selects a DuckDB `DECIMAL(18,2)` column
- **THEN** the model's column is DOUBLE and records `DECIMAL(18,2)` as its database type

### Requirement: Time-dimension role

A field SHALL be a time dimension when `dimension.is_time` is `true`, or when `is_time` is unset and its `datatype` is `Date`, `Time`, `DateTime` or `DateTimeTz`. An explicit `is_time: false` SHALL win. The time-dimension role SHALL NOT change a column's type. Only a time-dimension field whose column type is DATE or TIMESTAMP SHALL be eligible as the model's default time dimension; a time-dimension field of any other type SHALL keep its type and carry the role in its metadata.

#### Scenario: Role defaulted from datatype
- **WHEN** a field over a DATE source column declares `datatype: Date` and no `dimension`
- **THEN** it becomes the model's default time dimension

#### Scenario: Explicit opt-out
- **WHEN** a TIMESTAMP field declares `datatype: DateTime` and `dimension: {is_time: false}`
- **THEN** it does not become the default time dimension

#### Scenario: Integer year
- **WHEN** a field over an INTEGER column declares `datatype: Integer` and `dimension: {is_time: true}`
- **THEN** the column stays INT, carries the time role in its metadata, and is not the default time dimension

### Requirement: Metric datatypes

A metric's `datatype` SHALL be kept in the measure's metadata and SHALL NOT change the measure's result type.

#### Scenario: Decimal metric
- **WHEN** a metric `SUM(amount)` over a DECIMAL column declares `datatype: Decimal`
- **THEN** the measure's metadata holds `Decimal` and querying it returns the database's exact decimal sum

### Requirement: The shipped example is a valid current-spec document

The worked example `docs/examples/13_osi_import/shop.osi.yaml` SHALL validate against the pinned upstream Ossie schema and SHALL import with no unconverted entries.

#### Scenario: Example validates and imports
- **WHEN** the worked example is validated against the pinned schema and imported into the demo DuckDB
- **THEN** validation passes and the import report holds no unconverted entries

### Requirement: List aggregation inputs

An aggregate over a list-valued field other than `COUNT(field)` (for example `COUNT(DISTINCT tags)`, `MAX(tags)`, `MIN`, `SUM` or `AVG` over a list) SHALL make the metric reported as unconverted, naming the aggregate and the list field; it SHALL NOT be rewritten to an aggregate over the list's elements. `COUNT(tags)` SHALL import and count the rows whose list is not NULL.

#### Scenario: Maximum of a list
- **WHEN** a dataset-scoped metric is `MAX(tags)` and `tags` is a list field
- **THEN** the metric is reported as unconverted, naming `MAX` and `tags`

#### Scenario: Distinct count of a list
- **WHEN** a dataset-scoped metric is `COUNT(DISTINCT tags)`
- **THEN** the metric is reported as unconverted

#### Scenario: Count of a list
- **WHEN** a dataset-scoped metric is `COUNT(tags)` over the rows `['a', 'b']`, `['b']` and NULL
- **THEN** the measure returns 2

### Requirement: Array functions map to the list library

In metrics and in field expressions, `ARRAY_AGG(DISTINCT x)` SHALL import as `collect(x)`, `ARRAY_CONTAINS(a, v)` and `v = ANY(a)` as `contains(a, v)`, and `ARRAY_LENGTH(a)` and `CARDINALITY(a)` as `length(a)`, with SLayer's NULL conventions: `collect` drops NULL inputs and yields an empty list for a cell without inputs; `contains(a, v)` is NULL when `a` or `v` is NULL and FALSE when `v` is absent, even if `a` holds a NULL element; `length` counts non-NULL elements and is NULL for a NULL list. A field expression using these functions, unless it is the target's own dialect and kept verbatim, SHALL be stored as the SQL SLayer itself emits for the library function on the target datasource; a field whose list element type the target needs but the import cannot determine SHALL be reported.

#### Scenario: Membership in a metric
- **WHEN** a dataset-scoped metric is `COUNT(*) FILTER (WHERE 'b' = ANY(tags))` over the rows `['a', 'b']`, `['b']` and NULL
- **THEN** the measure returns 2

#### Scenario: Membership in a field
- **WHEN** a field is `has_a: ARRAY_CONTAINS(tags, 'a')` and the target is a DuckDB datasource
- **THEN** grouping by `has_a` returns TRUE for one row, FALSE for one row and NULL for one row

#### Scenario: Length in a field
- **WHEN** a field is `n_tags: CARDINALITY(tags)`
- **THEN** it returns 2, 1 and NULL

#### Scenario: Distinct values collected
- **WHEN** a model-scoped metric is `ARRAY_AGG(DISTINCT orders.status)` over statuses `paid`, `paid`, `open`
- **THEN** the measure returns `['open', 'paid']`

### Requirement: SLAYER extensions restore SLayer structure

The importer SHALL read custom extensions with `vendor_name: SLAYER` and apply the SLayer content they carry: join type, name, cardinality and declaring model; measure formula, model, name, label, type and metadata; column display attributes, metadata, type and value/filter split; model hidden flag, access tags, default time dimension, metadata, custom aggregations, source structure, and SLayer-only columns and measures. Content that duplicates Ossie content (a measure's formula and model, a column's type and value/filter split, a model's source structure) SHALL be restored only when the payload's fingerprint matches the entity's Ossie content as found in the document; otherwise the Ossie content SHALL be imported as it would be without the extension, and the report SHALL hold an informational entry naming the entity. A payload that is malformed, of an unknown version, or attached to an entity it does not describe SHALL be reported and ignored, and the import SHALL NOT raise. Extensions of other vendors SHALL keep being stored on the entity as before.

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

### Requirement: Single-field unique keys mark the column unique

A `unique_keys` entry naming exactly one declared field SHALL set that field's column `unique`.

#### Scenario: Single-field unique key
- **WHEN** a dataset declares `unique_keys: [[email]]` and `email` is a declared field
- **THEN** column `email` has `unique: true`

### Requirement: Explicit time role is recorded

A field that declares `dimension.is_time` explicitly SHALL keep the declared value, `true` or `false`, in its column's metadata.

#### Scenario: Explicit opt-out recorded
- **WHEN** a TIMESTAMP field declares `dimension: {is_time: false}`
- **THEN** its column's metadata records the time role as false

### Requirement: Source columns in dataset-scoped metrics

In a dataset-scoped metric of a current-spec document, a two-part reference whose qualifier is `source`, matched case-insensitively whether quoted or not, SHALL read the named column of that dataset's source (a table's columns, or a query source's output columns), matched with the target datasource's identifier folding (a quoted name exactly, an unquoted name as the database folds it). The measure SHALL return the same values as aggregating that physical column, in every position of the metric: aggregate arguments, `FILTER` predicates and percentile orderings. The model's visible columns SHALL remain exactly its declared fields; a metric whose import adds a column for the reference SHALL add it hidden, and a metric reported as unconverted SHALL add no column. The metric SHALL be reported as unconverted, naming the reason, when:

- the dataset declares a field whose name matches `source`, which takes precedence over the qualifier;
- the name matches no column of the source, or several;
- a field of the dataset holds the source column's name but does not read that column unchanged, so the source column cannot be read past it;
- the reference appears in a model-scoped metric.

#### Scenario: Source column no field declares
- **WHEN** dataset `orders` declares fields `order_id` and `amount` and metric `units: SUM(source.quantity)` over rows with `quantity` 2 and 3
- **THEN** measure `units` returns 5 and the visible columns of `orders` are exactly `order_id` and `amount`

#### Scenario: Metric named like the source column
- **WHEN** dataset `orders` declares metric `quantity: SUM(source.quantity)` and no field `quantity`
- **THEN** measure `quantity` imports and returns the sum of the physical `quantity`

#### Scenario: Source column a field passes through
- **WHEN** dataset `orders` declares field `amount: amount` and metric `SUM(source.amount)`
- **THEN** the measure returns the sum of `amount` and the model gains no hidden column

#### Scenario: Qualifier case
- **WHEN** a dataset-scoped metric is `SUM(SOURCE.quantity)`
- **THEN** it imports as `SUM(source.quantity)` does

#### Scenario: Source column inside an operand
- **WHEN** dataset `orders` declares field `price` and metric `SUM(source.quantity * price)` over rows (2, 10), (3, 5)
- **THEN** the measure returns 35

#### Scenario: Source column in a FILTER predicate
- **WHEN** dataset `orders` declares field `amount` but no field reading `status`, and metric `SUM(amount) FILTER (WHERE source.status = 'completed')` over rows (completed, 10), (open, 5), (completed, 7)
- **THEN** the measure returns 17

#### Scenario: Filtered row count over a source column
- **WHEN** the same dataset declares metric `COUNT(*) FILTER (WHERE source.status = 'completed')`
- **THEN** the measure returns 2

#### Scenario: Percentile over a source column
- **WHEN** a dataset-scoped metric is `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY source.quantity)` over `quantity` values 1, 2, 3
- **THEN** the measure returns 2

#### Scenario: Query source output column
- **WHEN** a dataset's source is a query selecting `qty AS units` and its metric is `SUM(source.units)`
- **THEN** the measure returns the sum of `qty`

#### Scenario: Folded source column
- **WHEN** the target is a Postgres datasource whose table has column `quantity` and a dataset-scoped metric is `SUM(source.Quantity)`
- **THEN** the measure returns the sum of `quantity`

#### Scenario: Quoted source column in the wrong case
- **WHEN** the source has column `quantity` and a dataset-scoped metric is `SUM(source."Quantity")`
- **THEN** the metric is reported as unconverted

#### Scenario: Unknown source column
- **WHEN** a dataset-scoped metric is `SUM(source.quantty)` and the source has no such column
- **THEN** the metric is reported as unconverted and the model gains no column

#### Scenario: Field named source
- **WHEN** dataset `orders` declares a field `source` and metric `SUM(source.quantity)`
- **THEN** the metric is reported as unconverted with a reason saying the field `source` takes precedence

#### Scenario: Source column behind a renamed field
- **WHEN** dataset `orders` has source columns `status` and `state`, declares field `status: state`, and its metric is `COUNT(*) FILTER (WHERE source.status = 'open')`
- **THEN** the metric is reported as unconverted with a reason naming the field `status`

#### Scenario: Source qualifier in a model-scoped metric
- **WHEN** a model-scoped metric is `SUM(source.quantity)` and no dataset is named `source`
- **THEN** the metric is reported as unconverted

### Requirement: Duplicate output labels in query sources

When a dataset's source is a query whose output holds two or more columns with the same label, every field, and every `source.column` reference, reading that label SHALL be reported (the field as dropped, the metric as unconverted) as reading an ambiguous source column. The dataset's other fields and metrics SHALL import.

#### Scenario: Field reading a duplicated label
- **WHEN** a dataset's source is `SELECT o.id, c.id, o.amount FROM o JOIN c ON …` and it declares fields `id: id` and `amount: amount`
- **THEN** field `id` is reported as reading an ambiguous source column and field `amount` imports

#### Scenario: Source column reading a duplicated label
- **WHEN** the same dataset declares metric `COUNT(DISTINCT source.id)`
- **THEN** the metric is reported as unconverted

### Requirement: Upstream examples import

Every file of the upstream Ossie `examples/` directory at the pinned commit SHALL be vendored unchanged. Each core semantic-model example SHALL validate against the pinned schema and import into tables matching its sources with the expected models; its metrics SHALL all import, except those using window functions, which SHALL be reported as unconverted. Each ontology or ontology-mapping example SHALL be skipped with a warning naming it.

#### Scenario: Renamed-fields example
- **WHEN** `renamed_fields.yaml` is imported
- **THEN** models `orders` and `customers` import with no unconverted entries, and `gross_revenue` returns the sum of `o_totalprice`

#### Scenario: TPC-DS example
- **WHEN** `tpcds_semantic_model.yaml` is imported
- **THEN** its five datasets import, and the unconverted entries are exactly its three window-function metrics

#### Scenario: Flights semantic model
- **WHEN** `flights.semantic_model.yaml` is imported
- **THEN** its six datasets import with no unconverted entries

#### Scenario: Ontology examples
- **WHEN** `flights.yaml`, `flights.ontology.yaml` and `flights.mapping.yaml` are imported
- **THEN** each is skipped with a warning naming it
