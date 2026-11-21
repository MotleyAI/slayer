## ADDED Requirements

### Requirement: Invalid documents are refused with every violation

Every file of an import SHALL be checked against the Ossie core spec at the vendored pin before any database is read. When any document breaks a rule that the document alone decides, the import SHALL fail with one error that lists every violation found in every file of the import, each naming its file, its document, the entity (dataset, field, metric, relationship, key, or the document itself) and the rule broken, in file order and then document order, and SHALL save nothing. The document rules are: the document validates against the pinned schema (every schema error listed); no YAML or JSON mapping repeats a key; every value under a non-portable extension's `data` is a JSON value; names are unique and at most 128 characters ("Names are unique and at most 128 characters"); keys and relationships name declared fields and datasets ("Keys and relationships name declared fields"); `from_columns` and `to_columns` have the same length; Ossie-dialect expressions follow the pinned Ossie SQL language ("Ossie-dialect expressions follow the pinned Ossie SQL language"); field and metric expressions reference only what their scope allows ("Field expressions are validated against the live source", "Metric name resolution", "Source columns in dataset-scoped metrics"). A file that cannot be read, does not parse as YAML or JSON, or whose root is not a mapping SHALL be a violation. A document converted without going through the file reader SHALL be refused the same way before any database is read. An Ossie ontology or ontology-mapping document (a root `ontology` or `concept_mappings` and no root `datasets`) SHALL be skipped with a warning naming its file. SLayer and environment limits SHALL NOT refuse a document: they stay per-entity reports as their own requirements define.

#### Scenario: Several violations listed
- **WHEN** a document declares two relationships named `r`, a `primary_key` naming an undeclared field, and a model-scoped metric `SUM(amount)`
- **THEN** the import fails with one error naming all three violations, each with its entity and rule, and no dataset is saved

#### Scenario: Two invalid files
- **WHEN** a directory holds two documents that each break a rule, and a third valid document
- **THEN** the import fails listing the violations of both invalid files, and nothing is saved, not even the valid document's datasets

#### Scenario: Repeated mapping key
- **WHEN** a dataset mapping holds the key `source` twice, in a YAML file or in a JSON file
- **THEN** the import fails naming the file, the line and the repeated key

#### Scenario: Unparseable file
- **WHEN** a file of the import is not valid YAML
- **THEN** the import fails naming that file

#### Scenario: Schema violation
- **WHEN** a field carries an unknown key `format`, or a document has no `name`
- **THEN** the import fails naming the entity and the schema rule

#### Scenario: Document converted directly
- **WHEN** a program hands a document with an undeclared `primary_key` field straight to the converter
- **THEN** the conversion fails with the same violation before any database is read

#### Scenario: Ontology document skipped
- **WHEN** a directory holds an ontology document and a valid semantic-model document
- **THEN** the ontology document is skipped with a warning naming it and the semantic model imports

#### Scenario: Limits stay reports
- **WHEN** a valid document has a metric only in `MDX`, a relationship keyed on a computed field, and a field reading a column the live source lacks
- **THEN** the document imports, and each of the three is reported for its entity

### Requirement: Names are unique and at most 128 characters

Every declared name (document, dataset, field, metric, relationship) SHALL be at most 128 characters; any non-empty name within that length SHALL be valid, whether it reads as a regular identifier or only as a quoted one. Within a document, dataset names, relationship names and model-scoped metric names SHALL each be unique; within a dataset, field names and dataset-scoped metric names SHALL be unique across the two. Names SHALL be compared by Ossie's normalized identifier form (a name that reads as a regular identifier upper-cased, any other as written); names that differ only in case SHALL also clash, since SLayer refuses case collisions. A violation SHALL refuse the document. Dataset names repeated, in any case, across the documents of one import SHALL refuse the import, since one datasource holds one namespace.

#### Scenario: Repeated relationship name
- **WHEN** a document declares two relationships named `orders_to_customers`
- **THEN** the import fails naming the relationship name

#### Scenario: Repeated model-scoped metric name
- **WHEN** a document declares model-scoped metrics `revenue` and `Revenue`
- **THEN** the import fails naming both

#### Scenario: Repeated dataset name in any case
- **WHEN** a document declares datasets `orders` and `Orders`
- **THEN** the import fails naming both

#### Scenario: Name too long
- **WHEN** a field's name is 129 characters long
- **THEN** the import fails naming the field and the 128-character limit

#### Scenario: Names that are not regular identifiers
- **WHEN** fields are named `order id`, `say "hi"` and `größe`
- **THEN** the document passes the name rules

#### Scenario: Dataset name repeated across documents
- **WHEN** two documents of one import each declare a dataset `orders`
- **THEN** the import fails naming `orders` and both documents

### Requirement: Ossie-dialect expressions follow the pinned Ossie SQL language

Every Ossie-dialect form of a field or metric expression — its top-level `sql`, and each `OSSIE_SQL_2026` dialect entry when the expression has no top-level `sql` — SHALL parse as exactly one expression (not empty, not several statements) and SHALL stay within the pinned Ossie SQL language: it SHALL NOT call `AGG` in any capitalization, SHALL NOT hold `SELECT`, `FROM`, `JOIN`, a subquery other than `EXISTS_IN`, a CTE, `UNION`, `INTERSECT`, `EXCEPT` or a DDL or DML statement, and a `FILTER (WHERE …)` predicate SHALL reference only fields of the dataset of its aggregate's arguments. A form that breaks this SHALL refuse the document. An `OSSIE_SQL_2026` entry beside a top-level `sql` SHALL NOT refuse the document: when it does not parse or differs from the `sql`, the report SHALL hold an informational entry saying the `sql` is authoritative; when it equals the `sql`, nothing is reported. Expressions in any other dialect SHALL NOT be judged by these rules.

#### Scenario: Unparseable portable expression
- **WHEN** a field's `sql` is `amount +* 2`
- **THEN** the import fails naming the field

#### Scenario: Several statements
- **WHEN** a metric's `sql` is `SUM(amount); DROP TABLE orders`
- **THEN** the import fails naming the metric

#### Scenario: Reserved AGG
- **WHEN** a model-scoped metric's `sql` is `agg(orders.revenue)`
- **THEN** the import fails naming the metric and the reserved `AGG`

#### Scenario: Subquery
- **WHEN** a field's `sql` is `(SELECT MAX(amount) FROM orders)`
- **THEN** the import fails naming the field

#### Scenario: Vendor dialect is not judged
- **WHEN** a metric's only form is a `SNOWFLAKE` entry `SUM(amount); SELECT 1`
- **THEN** the document passes this rule and the metric is reported for its entity

#### Scenario: Superseded entry that does not parse
- **WHEN** a field has `sql: amount` and an `OSSIE_SQL_2026` entry `amount +* 2`
- **THEN** the field imports with its `sql` and the report holds an informational entry for the entry

#### Scenario: Superseded entry that differs
- **WHEN** a field has `sql: amount` and an `OSSIE_SQL_2026` entry `amount * 2`
- **THEN** the field imports with its `sql` and the report holds an informational entry for the entry

#### Scenario: Superseded entry that repeats the sql
- **WHEN** a field has `sql: amount` and an `OSSIE_SQL_2026` entry `amount`
- **THEN** the field imports and the report holds nothing for it

### Requirement: References resolve as Ossie identifiers

A name in `primary_key`, `unique_keys`, `from_columns`, `to_columns`, a relationship's `from` and `to`, and an unquoted reference in a metric expression SHALL match a declared name when their normalized forms are equal (a name that reads as a regular identifier upper-cased, any other as written), and the import SHALL store the declared spelling. A quoted reference SHALL match a declared name whose normalized form equals the quoted text.

#### Scenario: Key names in another case
- **WHEN** dataset `orders` declares field `order_id` and `primary_key: [Order_ID]`, and a relationship declares `from: Orders` and `from_columns: [CUSTOMER_ID]` over field `customer_id`
- **THEN** the primary key is `order_id`, the relationship joins from `orders` on `customer_id`, and the import stores those spellings

#### Scenario: Quoted upper-case reference to a regular name
- **WHEN** dataset `orders` declares field `Amount` and a dataset-scoped metric is `SUM("AMOUNT")`
- **THEN** the measure imports and its formula references `Amount`

#### Scenario: Quoted reference in the declared spelling
- **WHEN** dataset `orders` declares field `Amount` and a dataset-scoped metric is `SUM("Amount")`
- **THEN** the import fails naming the metric and `"Amount"`

## MODIFIED Requirements

### Requirement: Current-spec documents import

The importer SHALL accept a flat Ossie document that holds one semantic model at its root (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `nonportable_extensions`) and declares `version: 0.3.0.dev0`, and SHALL import it as one semantic model. A flat document declaring `1.0`, `0.1.0`, `0.1.1` or `0.2.0.dev0` SHALL be upgraded to `0.3.0.dev0`, with one informational report entry naming the document and the upgrade, and then imported the same way. A flat document declaring any other version, or none, SHALL be refused.

#### Scenario: Flat document imports end to end
- **WHEN** a file holds `version: 0.3.0.dev0`, `name: shop`, two datasets `orders` and `customers` with declared fields, a relationship `orders → customers`, and a model-scoped metric `SUM(orders.amount)`
- **THEN** the import produces datasets `orders` and `customers`, a many-to-one join from `orders` to `customers`, and the measure on `orders`, with no unconverted entries and no version entry

#### Scenario: Earlier flat document keeps importing
- **WHEN** the same document declares `version: 0.2.0.dev0`
- **THEN** it imports with the same datasets, join and measure, and the report holds one informational entry naming the upgrade to `0.3.0.dev0`

#### Scenario: Unknown version
- **WHEN** a flat document declares `version: 0.4.0`, or no version
- **THEN** the import fails naming the document and its version

#### Scenario: Document-level extensions kept
- **WHEN** a document carries `nonportable_extensions: [{vendor_name: DBT, data: {project_name: tpcds_analytics}}]` at its root
- **THEN** every imported dataset records that entry, with `data` as the object `{project_name: tpcds_analytics}`

### Requirement: Legacy wrapper documents keep importing

A document whose root holds a `semantic_model` key and declares no version, or `1.0`, `0.1.0`, `0.1.1` or `0.2.0.dev0`, SHALL be upgraded as Ossie's migration section prescribes and then validated and imported under the same rules as a current-spec document. Each element of a `semantic_model` array, or a single object-valued `semantic_model`, SHALL become one flat document declaring `0.3.0.dev0`; no model SHALL be dropped. Root `dialects` and `vendors` declarations SHALL be dropped; any other root key SHALL be carried onto each model, and a root key that the model also sets SHALL refuse the document. The report SHALL hold one informational entry per upgraded file naming the upgrade. The document SHALL be refused when its `semantic_model` value is empty, null, or not an array or object, when an element is not a non-empty object, when its root also holds `datasets`, when it declares `0.3.0.dev0`, or when it declares any other version.

#### Scenario: Legacy array with two models
- **WHEN** a file holds `semantic_model: [ {name: a, datasets: [...]}, {name: b, datasets: [...]} ]` whose datasets declare their fields
- **THEN** the datasets of both `a` and `b` are imported, and the report holds one informational entry naming the upgrade

#### Scenario: Legacy object-valued wrapper
- **WHEN** a file holds `semantic_model: {name: a, datasets: [...]}`
- **THEN** the datasets of `a` are imported

#### Scenario: Wrapper without a version
- **WHEN** a file holds `semantic_model: [...]` and no `version`
- **THEN** its models are upgraded and imported

#### Scenario: Wrapper relying on source columns
- **WHEN** a wrapper dataset declares no fields and its metric is `SUM(amount)` over a source column `amount`
- **THEN** the import fails naming the metric and `amount`

#### Scenario: Root key the model also sets
- **WHEN** a wrapper's root holds `description: x` and its model holds `description: y`
- **THEN** the import fails naming the document and `description`

#### Scenario: Mixed shape
- **WHEN** a file holds both `semantic_model: [...]` and `datasets: [...]` at its root
- **THEN** the import fails naming that file

#### Scenario: Empty legacy wrapper
- **WHEN** a file holds `semantic_model: []`
- **THEN** the import fails naming that file

#### Scenario: Wrapped 0.3 document
- **WHEN** a file declares `version: 0.3.0.dev0` and holds `semantic_model: [...]`
- **THEN** the import fails naming that file and saying `0.3.0.dev0` documents are flat

### Requirement: Expressions carry an optional portable form and optional dialect entries

An expression SHALL be accepted with a top-level `sql` string, a `dialects` list, or both. A `dialect` SHALL be accepted as any non-empty string. An expression with neither a `sql` nor a `dialects` entry SHALL refuse the document; a field or metric whose expression has no form SLayer can use SHALL be reported for that entity, and the rest of the document SHALL still import.

#### Scenario: Expression with only top-level sql
- **WHEN** a field is declared as `expression: {sql: "amount"}`
- **THEN** the field imports

#### Scenario: Open dialect token
- **WHEN** a field's only dialect entry is `{dialect: OSSIE_SQL_2026, expression: amount}`
- **THEN** the field imports

#### Scenario: Expression with neither form
- **WHEN** a metric declares `expression: {}`
- **THEN** the import fails naming the metric

### Requirement: Unreadable expressions are reported, never raised

When an expression has no Ossie-dialect form and none of its dialect entries is readable, the entity SHALL be reported as not converted, naming the dialects found, and the import SHALL NOT fail for it. An Ossie-dialect form SLayer cannot parse refuses the document ("Ossie-dialect expressions follow the pinned Ossie SQL language").

#### Scenario: Non-SQL dialects only
- **WHEN** a metric's only entries are `MDX` and `DAX`
- **THEN** the metric is reported as unconverted with a reason naming `MDX` and `DAX`, and the import completes

### Requirement: Selected expressions are rendered for the target datasource

An expression SHALL be read with the conventions of its own dialect, where `OSSIE_SQL_2026` and `ANSI_SQL` read Ossie's portable function set (including `IFF`, `ZEROIFNULL`, `DATEADD`, `STARTSWITH` and `||`), and its unquoted identifiers SHALL be read with the target datasource's identifier folding. A field's portable `sql` SHALL mean on the target what Ossie defines; every raw-SQL definition the import adds by translation (a field's dialect entry, a field's filter) SHALL be rendered in the target datasource's SQL dialect. An expression whose dialect is the target datasource's own SHALL be stored verbatim, apart from the identifier folding of "Imported SQL reads with the source's identifier folding" (`importers/meaning-preservation`). An Ossie-dialect form that cannot be parsed SHALL refuse the document; an expression whose only forms are vendor dialects none of which can be parsed SHALL be reported for its entity.

#### Scenario: Portable function rendered for Postgres
- **WHEN** a field is `sql: "IFF(amount > 0, amount, 0)"` and the target is a Postgres datasource
- **THEN** querying the field returns `amount` for positive amounts and `0` otherwise

#### Scenario: Concatenation on MySQL
- **WHEN** a field is `sql: "first_name || ' ' || last_name"` and the target is a MySQL datasource
- **THEN** querying the field on MySQL returns the concatenated strings

#### Scenario: Target-native kept verbatim
- **WHEN** a field's `SNOWFLAKE` entry is `amount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the imported field's `SNOWFLAKE` entry is exactly `amount::NUMBER(10,2)`

#### Scenario: Target-native mixed-case identifier folded
- **WHEN** a field's `SNOWFLAKE` entry is `OrderAmount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the imported field's `SNOWFLAKE` entry is exactly `orderamount::NUMBER(10,2)`

#### Scenario: Portable expression folded by the target
- **WHEN** a field is `sql: "IFF(OrderAmount > 0, OrderAmount, 0)"` and the target is a Postgres datasource
- **THEN** querying the field reads the physical column `orderamount`

#### Scenario: Unparseable expression
- **WHEN** a field's only form is a `BIGQUERY` entry `amount +* 2`, and another field's only form is `sql: amount +* 2`
- **THEN** the import fails naming the second field; with the second field removed, the first field is reported and the dataset's other fields import

### Requirement: Field expressions are validated against the live source

A field expression reads only columns of its own dataset's source. A column qualified by the name of another dataset of the same document SHALL refuse the document, unless that qualifier is also the last part of the dataset's own table `source`. A column qualified by the dataset's own name SHALL read the source column. Every other column a field expression reads SHALL exist in the dataset's live source; a field reading a column the source does not have SHALL be reported and skipped.

#### Scenario: Typo in a field expression
- **WHEN** a field is `total_usd: total_cnets / 100.0` and the source has no column `total_cnets`
- **THEN** the field is reported as referencing an unknown column and is not on the dataset

#### Scenario: Another dataset's name as a qualifier
- **WHEN** a field of dataset `orders` is `UPPER(customers.segment)` and `customers` is a dataset of the same document
- **THEN** the import fails naming the field and `customers.segment`

#### Scenario: Own dataset's name as a qualifier
- **WHEN** a field of dataset `orders` is `orders.amount * 2` and the source has a column `amount`
- **THEN** the field imports and reads the source column `amount`

#### Scenario: Unknown qualifier
- **WHEN** a field of dataset `orders` is `t.amount` and `t` is neither a dataset of the document nor the source's table
- **THEN** the field is reported and skipped, and the document's other fields import

### Requirement: Keys and relationships name declared fields

Every name in `primary_key`, `unique_keys`, `from_columns` and `to_columns` SHALL resolve to a declared field of the owning dataset (for relationships, the `from` and `to` dataset respectively), and a relationship's `from` and `to` SHALL name datasets of its own document, matched as "References resolve as Ossie identifiers" defines. A name that does not resolve SHALL refuse the document. A relationship whose key field is a computed expression or a filtered column SHALL be reported as not representable.

#### Scenario: Relationship on a renamed field
- **WHEN** field `customer_id: o_custkey` is declared on `orders`, field `id: c_custkey` on `customers`, and a relationship uses `from_columns: [customer_id]`, `to_columns: [id]`
- **THEN** the join is created and a query across it joins `o_custkey` to `c_custkey`

#### Scenario: Relationship on a source column
- **WHEN** a relationship uses `from_columns: [o_custkey]` and no field named `o_custkey` is declared
- **THEN** the import fails naming the relationship and `o_custkey`

#### Scenario: Computed join key
- **WHEN** a relationship's `from_columns` names a field declared as `UPPER(cust_code)`
- **THEN** the relationship is reported as not representable

#### Scenario: Primary key on a source column
- **WHEN** a dataset declares `primary_key: [o_orderkey]` and no field named `o_orderkey`
- **THEN** the import fails naming the dataset and `o_orderkey`

#### Scenario: Cross-document relationship
- **WHEN** document 1 declares dataset `orders` and a relationship to `customers`, and `customers` is declared only in document 2 of the same import
- **THEN** the import fails naming the relationship and `customers`

### Requirement: Primary keys

A declared `primary_key` SHALL replace the introspected primary key. When a dataset declares no `primary_key` at all, a field that renames an introspected primary-key column SHALL keep the primary-key flag. A `primary_key` or a `unique_keys` entry that is present but empty SHALL refuse the document.

#### Scenario: Renamed introspected key without declaration
- **WHEN** the source's primary key is `o_orderkey`, the dataset declares field `order_id: o_orderkey`, and declares no `primary_key`
- **THEN** field `order_id` is the dataset's primary key

#### Scenario: Empty primary key
- **WHEN** a dataset declares `primary_key: []`
- **THEN** the import fails naming the dataset's `primary_key`

#### Scenario: Empty unique key entry
- **WHEN** a dataset declares `unique_keys: [[], [email]]`
- **THEN** the import fails naming the dataset's `unique_keys`

### Requirement: Metric name resolution

A model-scoped metric SHALL reference fields only as `dataset.field`, the dataset a dataset of its document and the field declared on it. A dataset-scoped metric SHALL reference unqualified declared fields of its own dataset and, as `source.column`, columns of its own dataset's source (see "Source columns in dataset-scoped metrics"). No reference SHALL have more than two parts. A reference that is unqualified in a model-scoped metric, qualified or foreign in a dataset-scoped metric (other than a `source.column` reference), names an undeclared field, names another metric, or names a source column without the `source` qualifier SHALL refuse the document, naming the metric and the reference.

#### Scenario: Unqualified reference in a model-scoped metric
- **WHEN** a model-scoped metric is `SUM(amount)` and only `orders` declares `amount`
- **THEN** the import fails naming the metric and `amount`

#### Scenario: Source column in a metric
- **WHEN** a model-scoped metric is `SUM(orders.o_totalprice)` and `orders` declares `total_amount: o_totalprice` but no field `o_totalprice`
- **THEN** the import fails naming the metric and `orders.o_totalprice`

#### Scenario: Unqualified source column in a dataset-scoped metric
- **WHEN** dataset `orders` declares `total_amount: o_totalprice` but no field `o_totalprice`, and its metric is `SUM(o_totalprice)`
- **THEN** the import fails naming the metric and `o_totalprice`

#### Scenario: Foreign reference in a dataset-scoped metric
- **WHEN** dataset `orders` declares metric `SUM(net_amount) / COUNT(DISTINCT customers.id)`
- **THEN** the import fails naming the metric and `customers.id`

#### Scenario: Metric reading a renamed field
- **WHEN** dataset `orders` declares `total_amount: o_totalprice` and a model-scoped metric is `SUM(orders.total_amount)`
- **THEN** the measure imports and returns the sum of `o_totalprice`

### Requirement: Identifier case in metric references

An unquoted identifier in a metric expression SHALL match a declared field or dataset name case-insensitively; a double-quoted identifier SHALL match a declared name whose normalized form equals it ("References resolve as Ossie identifiers"). The stored measure SHALL reference fields by their declared names.

#### Scenario: Case-insensitive match
- **WHEN** dataset `orders` declares field `amount` and a dataset-scoped metric is `SUM(Amount)`
- **THEN** the measure imports and its formula references `amount`

#### Scenario: Quoted exact match
- **WHEN** dataset `orders` declares field `Amount` and a dataset-scoped metric is `SUM("amount")`
- **THEN** the import fails naming the metric and `"amount"`

#### Scenario: Case-only ambiguity
- **WHEN** dataset `orders` declares fields `id` and `ID`
- **THEN** the import fails naming `orders`, `id` and `ID`

### Requirement: Source columns in dataset-scoped metrics

In a dataset-scoped metric, a two-part reference whose qualifier is `source`, matched case-insensitively whether quoted or not, SHALL read the named column of that dataset's source (a table's columns, or a query source's output columns), matched with the target datasource's identifier folding (a quoted name exactly, an unquoted name as the database folds it), whatever fields the dataset declares. The measure SHALL return the same values as aggregating that physical column, in every position of the metric: aggregate arguments, `FILTER` predicates and percentile orderings. The metric SHALL be stored with the reference as written, and its import SHALL add no field; the dataset's visible fields SHALL remain exactly its declared fields. The document SHALL be refused when the dataset declares a field whose name matches `source`, which takes precedence over the qualifier, or when a `source.` reference appears in a model-scoped metric and no dataset of the document is named `source`. The metric SHALL be reported as unconverted, naming the reason, when the name matches no column of the source, or several.

#### Scenario: Source column no field declares
- **WHEN** dataset `orders` declares fields `order_id` and `amount` and metric `units: SUM(source.quantity)` over rows with `quantity` 2 and 3
- **THEN** measure `units` returns 5 and the visible fields of `orders` are exactly `order_id` and `amount`

#### Scenario: Metric named like the source column
- **WHEN** dataset `orders` declares metric `quantity: SUM(source.quantity)` and no field `quantity`
- **THEN** measure `quantity` imports and returns the sum of the physical `quantity`

#### Scenario: Source column a field passes through
- **WHEN** dataset `orders` declares field `amount: amount` and metric `SUM(source.amount)`
- **THEN** the measure returns the sum of `amount` and the dataset gains no hidden field

#### Scenario: Qualifier case
- **WHEN** a dataset-scoped metric is `SUM(SOURCE.quantity)`
- **THEN** it imports and returns what `SUM(source.quantity)` returns

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
- **THEN** the metric is reported as unconverted and the dataset gains no field

#### Scenario: Field named source
- **WHEN** dataset `orders` declares a field `source` and metric `SUM(source.quantity)`
- **THEN** the import fails naming the metric and saying the field `source` takes precedence

#### Scenario: Source column behind a renamed field
- **WHEN** dataset `orders` has source columns `status` and `state`, declares field `status: state`, and its metric is `COUNT(*) FILTER (WHERE source.status = 'open')`
- **THEN** the metric imports and returns the number of rows whose physical `status` is `open`

#### Scenario: Source qualifier in a model-scoped metric
- **WHEN** a model-scoped metric is `SUM(source.quantity)` and no dataset is named `source`
- **THEN** the import fails naming the metric and `source.quantity`

### Requirement: FILTER aggregates

`agg(x) FILTER (WHERE p)` SHALL import as the aggregation over only the rows where `p` holds, for every supported aggregation including `COUNT(*)`, `COUNT(DISTINCT x)` and percentile aggregates. The predicate SHALL be row-level and SHALL read only the dataset of the aggregate's arguments: in an Ossie-dialect form, a predicate reading another dataset or holding a subquery SHALL refuse the document ("Ossie-dialect expressions follow the pinned Ossie SQL language"); a predicate containing an aggregate or a window, or a vendor-dialect predicate that breaks these rules, SHALL make the metric reported as unconverted. `SUM`, `AVG`, `MIN` and `MAX` over `DISTINCT` SHALL remain reported as unconverted, with or without `FILTER`. A `FILTER` SHALL NOT remove result groups.

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
- **WHEN** a model-scoped metric's `sql` is `SUM(orders.amount) FILTER (WHERE customers.segment = 'b2b')`
- **THEN** the import fails naming the metric and `customers.segment`

#### Scenario: Sum over distinct
- **WHEN** a metric is `SUM(DISTINCT amount) FILTER (WHERE status = 'completed')`
- **THEN** the metric is reported as unconverted

### Requirement: Legacy custom_extensions keep importing

The importer SHALL read the legacy key `custom_extensions`, whose entries carry `data` as a JSON-encoded string, at every level that carries `nonportable_extensions` (document, dataset, field, metric, relationship), whatever version the document declares, as part of upgrading the document. Each legacy entry's string SHALL be decoded and then treated exactly as a `nonportable_extensions` entry with that object; an entity carrying both keys SHALL have the entries of both read. A legacy entry whose string does not decode to a JSON object SHALL refuse the document. A document that uses the legacy key anywhere SHALL add exactly one informational entry to the report, naming the document and suggesting a re-export.

#### Scenario: SLayer 1.8 export restores
- **WHEN** a document declaring `0.3.0.dev0` carries its `SLAYER` payloads under `custom_extensions` as JSON strings, with matching fingerprints
- **THEN** the import restores the same SLayer structure as for the same document written with `nonportable_extensions`, and the report holds one informational entry naming the document

#### Scenario: Legacy key at every level
- **WHEN** a document carries a legacy `ACME` entry with `data: '{"k": 1}'` on its root, a dataset, a field, a metric and a relationship
- **THEN** each entity records an `ACME` entry with `data` equal to `{k: 1}`, and the report holds exactly one informational entry for the legacy key

#### Scenario: Both keys on one entity
- **WHEN** a field carries `nonportable_extensions: [{vendor_name: ACME, data: {a: 1}}]` and `custom_extensions: [{vendor_name: OTHER, data: '{"b": 2}'}]`
- **THEN** the imported field records both entries

#### Scenario: Legacy string that is not an object
- **WHEN** a legacy entry's `data` is `'[1, 2]'` or `'not json'`
- **THEN** the import fails naming the entity and the vendor

### Requirement: Extension data must be a JSON object

A non-portable extension entry's `data` SHALL be an object whose keys are strings and whose values, at every depth, are JSON values: strings, finite numbers, booleans, null, arrays and objects. An entry whose `data` is not an object, or holds a date, a timestamp, a non-finite number or a non-string key at any depth, SHALL refuse the document, naming the entity and the vendor.

#### Scenario: Unquoted date under data
- **WHEN** a dataset's entry carries `data: {released: 2024-01-01}` with the date unquoted in YAML
- **THEN** the import fails naming the dataset and the vendor

#### Scenario: Nested non-finite number
- **WHEN** an entry carries `data: {stats: {ratio: .inf}}`, `{stats: {ratio: -.inf}}` or `{stats: {ratio: .nan}}`
- **THEN** the import fails naming the entity and the vendor

#### Scenario: Nested timestamp or non-string key
- **WHEN** an entry carries `data: {log: {at: 2024-01-01T00:00:00Z}}` or `data: {codes: {1: one}}`
- **THEN** the import fails naming the entity and the vendor

#### Scenario: Data that is not an object
- **WHEN** an entry carries `data: [1, 2]` or `data: text`
- **THEN** the import fails naming the entity and the vendor

## REMOVED Requirements

### Requirement: Invalid document shapes are skipped with a warning
**Reason**: An invalid Ossie document is refused with every reason instead of being skipped (DEV-2264 ruling).
**Migration**: The shape checks are now refusals under "Legacy wrapper documents keep importing" and "Invalid documents are refused with every violation"; ontology and mapping documents are still skipped with a warning. Fix the reported shape, or split a mixed document.

### Requirement: Legacy documents keep lenient resolution
**Reason**: Legacy wrapper documents are upgraded to the current shape and validated under the current name-resolution rules; Ossie's version history says converters must now resolve these names as fields.
**Migration**: Declare a field for every source column a metric, key or relationship names, and reference the field; a dataset-scoped metric may read a source column as `source.column`.
