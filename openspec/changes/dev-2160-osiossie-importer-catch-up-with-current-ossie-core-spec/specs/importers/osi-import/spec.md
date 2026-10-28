## Purpose

Imports Ossie (OSI) semantic-model documents into SLayer models: datasets become models, relationships become joins, metrics become measures, and anything that cannot be expressed exactly is reported rather than silently lost.

## ADDED Requirements

### Requirement: Current-spec documents import

The importer SHALL accept a flat Ossie document that holds one semantic model at its root (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `custom_extensions`). Each such document SHALL be imported as one semantic model.

#### Scenario: Flat document imports end to end
- **WHEN** a file holds `version: 0.2.0.dev0`, `name: shop`, two datasets `orders` and `customers` with declared fields, a relationship `orders → customers`, and a model-scoped metric `SUM(orders.amount)`
- **THEN** the import produces models `orders` and `customers`, a many-to-one join from `orders` to `customers`, and the measure on `orders`, with no unconverted entries

### Requirement: Legacy wrapper documents keep importing

A document whose root holds a `semantic_model` key SHALL be treated as a legacy document. When `semantic_model` is an array, each element SHALL be imported as its own semantic model; when it is a single object, it SHALL be imported as one semantic model. No model in the wrapper SHALL be dropped.

#### Scenario: Legacy array with two models
- **WHEN** a file holds `semantic_model: [ {name: a, datasets: [...]}, {name: b, datasets: [...]} ]`
- **THEN** the datasets of both `a` and `b` are imported

#### Scenario: Legacy object-valued wrapper
- **WHEN** a file holds `semantic_model: {name: a, datasets: [...]}`
- **THEN** the datasets of `a` are imported

### Requirement: Invalid document shapes are skipped with a warning

A file SHALL be skipped with a logged warning, and the rest of the import SHALL continue, when its root mixes both shapes (a `semantic_model` key together with a root `datasets` key), holds neither, or holds a `semantic_model` value that is empty, null, or not an array or object.

#### Scenario: Mixed shape
- **WHEN** a file holds both `semantic_model: [...]` and `datasets: [...]` at its root
- **THEN** that file is skipped with a warning naming it, and other files in the same import are imported

#### Scenario: Empty legacy wrapper
- **WHEN** a file holds `semantic_model: []`
- **THEN** that file is skipped with a warning

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

The selected expression SHALL be read with the conventions of its own dialect, where `OSSIE_SQL_2026` and `ANSI_SQL` read Ossie's portable function set (including `IFF`, `ZEROIFNULL`, `DATEADD`, `STARTSWITH` and `||`). Every raw-SQL definition the import stores (a column's SQL, a column's filter) SHALL be rendered in the target datasource's SQL dialect. An expression whose dialect is the target datasource's own SHALL be stored verbatim. An expression that cannot be parsed SHALL be reported for its entity.

#### Scenario: Portable function rendered for Postgres
- **WHEN** a field is `sql: "IFF(amount > 0, amount, 0)"` and the target is a Postgres datasource
- **THEN** the stored column SQL is valid Postgres and querying the column returns `amount` for positive amounts and `0` otherwise

#### Scenario: Concatenation on MySQL
- **WHEN** a field is `sql: "first_name || ' ' || last_name"` and the target is a MySQL datasource
- **THEN** the stored column SQL concatenates the strings on MySQL

#### Scenario: Target-native kept verbatim
- **WHEN** the selected entry is `SNOWFLAKE` with expression `amount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the stored column SQL is exactly `amount::NUMBER(10,2)`

#### Scenario: Unparseable expression
- **WHEN** a field's selected expression is `amount +* 2`
- **THEN** that field is reported and the dataset's other fields import

### Requirement: Current-spec models expose their declared fields

For a current-spec document, each dataset's model SHALL expose exactly its declared fields as columns (plus any hidden helper columns the metric conversion needs). Source columns that no field declares SHALL NOT appear on the model. A field expression SHALL still be able to read any column of the dataset's source. A dataset that declares no fields SHALL still import, with a warning that it exposes no columns.

#### Scenario: Renamed field hides its source column
- **WHEN** dataset `orders` has source columns `o_orderkey` and `o_totalprice` and declares fields `order_id: o_orderkey` and `total_amount: o_totalprice`
- **THEN** model `orders` has columns `order_id` and `total_amount` and no column `o_orderkey` or `o_totalprice`, and querying `total_amount` returns the values of `o_totalprice`

#### Scenario: Derived field over an undeclared source column
- **WHEN** a dataset declares only `total_usd: total_cents / 100.0` and `total_cents` is a source column
- **THEN** model column `total_usd` imports and returns `total_cents / 100.0`

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

For a current-spec document, a model-scoped metric SHALL reference fields only as `dataset.field`, and a dataset-scoped metric SHALL reference only unqualified fields of its own dataset. A reference that is unqualified in a model-scoped metric, qualified or foreign in a dataset-scoped metric, names an undeclared field, or names a source column that no field declares SHALL make the metric reported as unconverted.

#### Scenario: Unqualified reference in a model-scoped metric
- **WHEN** a model-scoped metric is `SUM(amount)` and only `orders` declares `amount`
- **THEN** the metric is reported as unconverted

#### Scenario: Source column in a metric
- **WHEN** a model-scoped metric is `SUM(orders.o_totalprice)` and `orders` declares `total_amount: o_totalprice` but no field `o_totalprice`
- **THEN** the metric is reported as unconverted

#### Scenario: Foreign reference in a dataset-scoped metric
- **WHEN** dataset `orders` declares metric `SUM(net_amount) / COUNT(DISTINCT customers.id)`
- **THEN** the metric is reported as unconverted

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

For a legacy-wrapper document, each model SHALL expose all introspected source columns plus its fields, and metric, key and relationship names SHALL resolve against both, with an unqualified metric reference resolving when exactly one dataset of its semantic model owns that name. Expression, dialect, `FILTER`, datatype and dataset-scoped metric handling SHALL be the same as for current-spec documents.

#### Scenario: Legacy unqualified metric
- **WHEN** a legacy document's metric is `SUM(amount)` and only `orders` has a column `amount`
- **THEN** the measure imports on `orders`

#### Scenario: Legacy relationship on a source column
- **WHEN** a legacy document's relationship uses `from_columns: [customer_id]`, a source column of `orders` that no field declares
- **THEN** the join is created

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

A non-bare aggregate operand (arithmetic, `CASE`, scalar functions, or, in a model-scoped metric, an operand combining fields of several datasets) SHALL be carried in the measure's formula when the formula language can express it. Otherwise the importer SHALL create a hidden helper column holding the operand on its single owning dataset, typed from the database. An operand that is neither expressible nor confined to one dataset SHALL make the metric reported as unconverted.

#### Scenario: Arithmetic operand
- **WHEN** a dataset-scoped metric is `SUM(quantity * amount)`
- **THEN** the measure returns the sum of `quantity * amount` and the model gains no hidden column for it

#### Scenario: Cross-dataset operand
- **WHEN** a model-scoped metric is `SUM(orders.amount - customers.discount)` and `orders` joins `customers` many-to-one
- **THEN** the measure imports and returns the sum over orders of `amount - discount` of each order's customer

#### Scenario: Operand needing a database-specific function
- **WHEN** a dataset-scoped metric aggregates a function the formula language does not offer
- **THEN** the model gains a hidden column holding the operand and the measure aggregates it

### Requirement: Field datatypes

A field's `datatype` SHALL map to SLayer types as: `String`→TEXT, `Integer`→INT, `Decimal` and `Float`→DOUBLE, `Boolean`→BOOLEAN, `Date`→DATE, `DateTime` and `DateTimeTz`→TIMESTAMP, `Time` and `Opaque`→UNKNOWN. A column's type SHALL be the type the database reports for it; when the database cannot report one, the mapped `datatype` SHALL be used, and DOUBLE only when neither exists. When both exist and disagree, the report SHALL hold an informational entry. The declared value SHALL be kept in the column's metadata.

#### Scenario: Declared type when the database cannot report one
- **WHEN** a derived field `UPPER(name)` declares `datatype: String` and the database type probe fails
- **THEN** the column is TEXT

#### Scenario: Database type wins
- **WHEN** a field over an INTEGER source column declares `datatype: String`
- **THEN** the column is INT and the report holds an informational entry naming the field

#### Scenario: Time of day
- **WHEN** a field declares `datatype: Time` and the database reports no type
- **THEN** the column is UNKNOWN

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
