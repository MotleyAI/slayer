## MODIFIED Requirements

### Requirement: Datasets are written in Ossie's vocabulary

A dataset SHALL hold its fields under `fields`, its metrics under `metrics` and its relationships under `relationships`. A field and a metric SHALL carry their type under `datatype`, a metric and a custom aggregation their definition under `expression`, and a relationship its target dataset under `to` and its keys under `from_columns` and `to_columns`. A dataset's `filters`, `aggregations`, `sql`, `sql_table`, `source_queries`, and a field's `sql` SHALL keep their names.

#### Scenario: A dataset is dumped
- **WHEN** a dataset with a field, a metric, a custom aggregation and a relationship is serialized
- **THEN** it carries `fields`, `metrics`, `relationships`, `datatype`, `expression`, `to`, `from_columns` and `to_columns`, and none of `columns`, `measures`, `joins`, `type`, `formula`, `target_model` or `join_pairs`

#### Scenario: Kept keys are unchanged
- **WHEN** a dataset with row filters is serialized
- **THEN** it carries `filters` under that name

### Requirement: Queries are written in Ossie's vocabulary

A query SHALL name its population `population`, its metrics `metrics` and its row filters `filter`. A query's metric entry SHALL be a string or carry `metric` and an optional `alias`, a dimension entry SHALL be a string or carry `field` and an optional `alias`, a time dimension SHALL name what it reads `field` and its result `alias`, an order item SHALL name what it sorts by `field`, and a query extension SHALL name its target `dataset` and carry `metrics` and `relationships`. This SHALL hold for a query, every stage of a multi-stage query, a nested query, a refinement and a REST query request.

#### Scenario: A query is dumped
- **WHEN** a two-stage query with a population, an aliased metric entry, an aliased dimension entry, a filter, an aliased time dimension, an order item and an extension is serialized
- **THEN** every stage carries `population`, `metrics` with `metric` and `alias`, `dimensions` with `field` and `alias`, `filter`, the time dimension's `field` and `alias`, the order item's `field` as text, and the extension's `dataset`, `metrics` and `relationships`, and none of `source_model`, `measures`, `filters`, `formula`, `expression`, `name`, `model`, `dimension`, `column`, `source_name` or `joins` outside a dataset metric definition

#### Scenario: A refinement uses the new keys
- **WHEN** a saved query is run by name with a refinement carrying `metrics` entries with `metric` and `alias`, and `filter`
- **THEN** the refinement is applied as before this change

### Requirement: Pre-2.0 spellings in fresh input convert with a deprecation

Fresh input carrying a pre-2.0 spelling SHALL be converted to its Ossie twin, with exactly one `FutureWarning` and one structured `deprecated_input` warning per distinct pre-2.0 spelling per input document, however often that spelling recurs across the document's nested parts (fields, metrics, relationships, extensions, stages, a refinement), each naming the old spelling, its replacement and SLayer 3.0. One request to an MCP tool SHALL count as one input document. The converted input SHALL behave, and be stored, exactly as the new spelling. This covers every dataset key and query key renamed in 2.0, a metric entry's `formula` and `name`, a dimension entry's `name` and `model` and an expression dimension's `expression` and `name`, a time dimension's `dimension` and `column`, an order item's `column`, an extension's `source_name`, a relationship's `join_pairs` and the pre-2.0 datatype values. A spelling that only a pre-release build of 2.0 accepted SHALL NOT be converted.

#### Scenario: An old query key converts
- **WHEN** a query is submitted with `source_model: "orders"` and `measures: ["sum(amount)"]`
- **THEN** it runs as the query with `population: "orders"` and `metrics: ["sum(amount)"]`
- **AND** the response carries one `deprecated_input` warning for `source_model` and one for `measures`, and one `FutureWarning` is emitted for each

#### Scenario: An old key used many times warns once
- **WHEN** a dataset given as fresh input has five fields that each spell `type`
- **THEN** exactly one `FutureWarning` and one `deprecated_input` warning name `type`

#### Scenario: An old key used across nested parts warns once
- **WHEN** a fresh two-stage query spells `measures` in both stages and in a query extension
- **THEN** the response carries exactly one `deprecated_input` warning naming `measures`, and exactly one `FutureWarning` names it

#### Scenario: Two old forms across nested parts warn once each
- **WHEN** a fresh dataset spells `columns` at its root and `type` on three of its fields
- **THEN** exactly two `FutureWarning`s and two `deprecated_input` warnings are emitted, one naming `columns` and one naming `type`

#### Scenario: An old key repeated in a refinement warns once
- **WHEN** a saved query is run by name with a refinement whose two metric entries each spell `formula`
- **THEN** the response carries exactly one `deprecated_input` warning naming `formula`, and exactly one `FutureWarning` names it

#### Scenario: An old key repeated in one MCP request warns once
- **WHEN** `edit_dataset` is called with three relationships that each spell `join_pairs`
- **THEN** the reply carries exactly one `deprecated_input` warning naming `join_pairs`, and exactly one `FutureWarning` names it

#### Scenario: An old field reference converts
- **WHEN** a query has the dimension entry `{"name": "region", "model": "customers"}`
- **THEN** it runs as `{"field": "customers.region"}` with a `deprecated_input` warning naming `model`

#### Scenario: A pre-release spelling is refused
- **WHEN** a fresh query has the metric entry `{"expression": "sum(amount)", "name": "total"}`
- **THEN** it is refused with a typed error naming the unsupported key `expression` and the key `metric`, and nothing runs

#### Scenario: A converted dataset is saved in the new vocabulary
- **WHEN** a dataset written with `columns`, `target_model` and `join_pairs` is saved through any surface
- **THEN** the stored document has `fields`, `to`, `from_columns` and `to_columns`, and the save reports the `deprecated_input` warnings

#### Scenario: A save surface reports the warning
- **WHEN** a dataset with `measures` is created through the MCP tool, the REST route and the Python client
- **THEN** each reports one `deprecated_input` warning naming `measures` and `metrics`

### Requirement: Python names follow the vocabulary

The public Python names SHALL follow the vocabulary, among them `SlayerDataset`, `DatasetField`, `DatasetMetric`, `DatasetRelationship`, `DatasetExtension`, `QueryMetric`, `QueryDimension`, `DatasetFieldCycleError` and `PopulationRecommendation`, and the public methods of the engine, the storage backends and the HTTP client SHALL say `dataset`, `field` and `population` where they said `model`, `column` and `root model`. Until 3.0 each pre-2.0 class, method and attribute name on SLayer's published list of deprecated names SHALL keep working: a class name SHALL resolve to its replacement, a method SHALL delegate to its replacement, and an attribute SHALL read and write its replacement, each emitting one `FutureWarning` naming the replacement and SLayer 3.0. `ColumnRef` and `ComputedDimension` SHALL resolve to `QueryDimension` and accept their pre-2.0 constructor keywords. Each listed name SHALL also be declared deprecated to type checkers, typed as its replacement, and SLayer's own type check SHALL fail on any use of one. A module path renamed here SHALL raise an `ImportError` naming its new path.

#### Scenario: An old class name still imports
- **WHEN** code imports `SlayerModel` from `slayer.core.models`
- **THEN** it receives `SlayerDataset`, and one `FutureWarning` names `SlayerDataset` and SLayer 3.0

#### Scenario: An old entry class still builds an entry
- **WHEN** code builds `ColumnRef(name="region", model="customers")` and `ComputedDimension(expression="upper(status)", name="s")` and puts both in a query's dimensions
- **THEN** the query holds the entries `{"field": "customers.region"}` and `{"field": "upper(status)", "alias": "s"}`, with one `FutureWarning` per class name

#### Scenario: An old method still works
- **WHEN** code calls the storage backend's `get_model` and the engine's `recommend_root_model`
- **THEN** each returns what `get_dataset` and `recommend_population` return, and each emits one `FutureWarning`

#### Scenario: Old attribute names still read
- **WHEN** code reads `.columns` on a dataset, `.source_model` on a query and `.join_pairs` on a relationship
- **THEN** it receives the values of `.fields`, `.population` and the relationship's key pairs, each with one `FutureWarning`

#### Scenario: Old attribute names still write
- **WHEN** code assigns to `.columns` on a dataset and to `.join_pairs` on a relationship
- **THEN** `.fields` holds the assigned value and the relationship's `from_columns` and `to_columns` hold the pairs' sides in order, one `FutureWarning` is emitted for each, and the dumps carry neither `columns` nor `join_pairs`

#### Scenario: Every listed name works
- **WHEN** every class, method and attribute name on the published list is used
- **THEN** each reaches its replacement with one `FutureWarning`

#### Scenario: A type checker flags old names
- **WHEN** a module that imports `SlayerModel`, reads `.columns` on a dataset and calls `get_model` on a storage backend is type-checked under SLayer's configuration
- **THEN** each of the three uses is reported as deprecated, naming its replacement

#### Scenario: A renamed module path names its new location
- **WHEN** `slayer.core.join_walker` is imported
- **THEN** an `ImportError` is raised whose message names `slayer.core.join_walker`, its new path and SLayer 2.0

### Requirement: Stored documents migrate to the new vocabulary

A document is stored when it carries a `version` key or is nested in a stored document; any other input is fresh. A stored dataset, saved query or memory written by a pre-2.0 release SHALL load in its Ossie form, shapes and datatype values included, with no deprecation warning, including queries nested in a dataset's `source_queries` or in a memory, whether or not the nested query carries a version of its own. A migrated stored document SHALL be persisted as every other migrated document of its kind is. A stored embedding row of kind `model`, `column` or `measure` SHALL become kind `dataset`, `field` or `metric` without being re-embedded and with its content hash unchanged, in both storage backends, and SHALL stay so across restarts.

#### Scenario: A pre-2.0 dataset loads
- **WHEN** a store holding a dataset with `columns`, `measures`, `joins` (with `target_model` and `join_pairs`), `type` values `TIMESTAMP` and `DOUBLE`, and `formula` is opened
- **THEN** the dataset loads with `fields`, `metrics`, `relationships` (with `to`, `from_columns` and `to_columns`), `datatype` values `DateTime` and `Float`, and `expression`, and no `FutureWarning` is emitted

#### Scenario: A nested stored query without a version
- **WHEN** a stored dataset's `source_queries` holds a query with `source_model` and `measures` and no `version` key
- **THEN** it loads with `population` and `metrics`, and no `FutureWarning` is emitted

#### Scenario: A memory's query migrates
- **WHEN** a stored memory holds a query with `filters`, a metric entry `{"formula": …, "name": …}` and a dimension entry `{"name": …, "model": …}`
- **THEN** the query loads with `filter`, a metric entry `{"metric": …, "alias": …}` and a dimension entry `{"field": …}`, and no `FutureWarning` is emitted

#### Scenario: Stored embedding kinds are renamed in place
- **WHEN** a YAML and a SQLite store whose embeddings hold rows of kinds `model`, `column` and `measure` are opened, closed and opened again
- **THEN** every row reads with kind `dataset`, `field` or `metric`, no entity is re-embedded, and every content hash is unchanged

#### Scenario: The 1.x upgrade stores still open
- **WHEN** the stored fixtures written by 0.10.2 and by 1.8.2 are opened
- **THEN** every document loads in its Ossie form and its queries return the results they returned before

### Requirement: The Ossie export payload carries the vocabulary

The `SLAYER` vendor payload of an exported Ossie document SHALL be written at payload version 2, in the Ossie vocabulary and shapes. The importer SHALL read payload versions 1 and 2 with the same result: a version-1 payload's fingerprint SHALL be checked against its content as written, and its pre-2.0 keys and shapes SHALL then convert with no deprecation warning.

#### Scenario: An export uses the new keys
- **WHEN** a dataset with a relationship, a SLayer-only field and a SLayer-only metric is exported to Ossie
- **THEN** every `SLAYER` payload is version 2 and carries no pre-2.0 key or shape, and every relationship carries `from_columns` and `to_columns`

#### Scenario: A pre-2.0 export still imports
- **WHEN** an Ossie document exported before this change (payload version 1) is imported
- **THEN** every payload's fingerprint matches, no `FutureWarning` is emitted, and the imported datasets equal those imported from the same datasets exported at version 2

## ADDED Requirements

### Requirement: Query entries take Ossie's form

A query's metric entry SHALL be a string or an object with `metric` (a saved metric's reference or an aggregate expression) and optional `alias`, `label`, `description`, `datatype` and `meta`. A dimension entry SHALL be a string or an object with `field` (a field reference or a row-level expression) and optional `alias` and `label`. An entry object with any other key SHALL be refused with a typed error naming the key. A dimension entry that is a bare reference with no `alias` or `label` SHALL be serialized as its string. An entry's result key SHALL be its `alias` when given, and otherwise the key the same entry received before this change.

#### Scenario: An Ossie query runs as written
- **WHEN** the query `{"dimensions": ["customers.region", {"field": "status", "alias": "s"}], "metrics": ["revenue", {"metric": "count(*)", "alias": "n"}], "filter": "status = 'paid'"}` is run with `population: "orders"` added
- **THEN** it returns the rows, values and result keys of the same query written in the pre-2.0 spelling

#### Scenario: An alias names a reference's column
- **WHEN** a dimension entry is `{"field": "customers.region", "alias": "r"}`
- **THEN** the result column is named `r` and the entry reads `customers.region` along its relationship path

#### Scenario: An alias equal to the default name changes nothing
- **WHEN** a query has the dimension entry `{"field": "region", "alias": "region"}` and the same query has `"region"`
- **THEN** both return identical rows, values and result keys

#### Scenario: A bare reference dumps as text
- **WHEN** a query with the dimension entry `{"field": "customers.region"}` is serialized
- **THEN** that entry is the string `"customers.region"`

#### Scenario: An unknown entry key is refused
- **WHEN** a query has the metric entry `{"metric": "sum(amount)", "nmae": "total"}`
- **THEN** it is refused with a typed error naming `nmae`

#### Scenario: Results are unchanged by the new form
- **WHEN** a battery of single- and multi-stage queries with named and unnamed metric entries, reference and expression dimensions, order items and refinements is run in the pre-2.0 spelling and in the Ossie form
- **THEN** both give rows, values, result keys and column order identical to those recorded before this change

### Requirement: Time dimensions, order items and extensions use Ossie's words

A time dimension SHALL name its result `alias`. An order item's `field` SHALL be a text reference or expression. A query extension SHALL name the dataset or stage it extends `dataset`. A time dimension's `name` and an order item's `{"name", "dataset"}` reference object, which only pre-release builds of 2.0 accepted, SHALL be refused with a typed error. An extension's `source_name` SHALL convert with a deprecation.

#### Scenario: A time dimension is aliased
- **WHEN** a query has the time dimension `{"field": "ordered_at", "granularity": "month", "alias": "m"}`
- **THEN** its result column is named `m`

#### Scenario: An order item sorts by dotted text
- **WHEN** a query is ordered by `{"field": "customers.region", "direction": "desc"}`
- **THEN** it sorts by that field along its relationship path, and the order item serializes with `field` as that text

#### Scenario: An extension names its dataset
- **WHEN** a query has the extension `{"dataset": "orders", "metrics": [{"name": "aov", "expression": "avg(amount)"}]}`
- **THEN** the metric `aov` is available on `orders` for that query

#### Scenario: An old extension key converts
- **WHEN** a fresh query has the extension `{"source_name": "orders", "metrics": [...]}`
- **THEN** it runs as `{"dataset": "orders", "metrics": [...]}` with one `deprecated_input` warning naming `source_name`

### Requirement: Relationships carry parallel key lists

A relationship SHALL carry its keys as `from_columns` and `to_columns`: lists of field names of equal, non-zero length, the n-th entries forming one key pair. The order the author wrote SHALL be kept in storage, dumps, exports and generated SQL. A relationship with empty key lists SHALL be refused with the typed error that names the aggregate remedy, one whose lists differ in length SHALL be refused with a typed error naming both lengths, and one carrying `join_pairs` beside either list SHALL be refused with a typed error naming both spellings. Two relationships to the same dataset whose key pairs differ only in order SHALL be the same relationship. Relationship references on MCP tools and in drift payloads SHALL carry `to`, an optional `name`, `from_columns` and `to_columns`, and a query extension's relationships SHALL take the same shape.

#### Scenario: A composite key keeps its order
- **WHEN** a relationship with `from_columns: ["a", "b"]` and `to_columns: ["x", "y"]` is saved, dumped and exported to Ossie, and a query joins along it
- **THEN** the dump and the export list `["a", "b"]` and `["x", "y"]` in that order, and the generated join condition pairs `a` with `x` and `b` with `y`

#### Scenario: Unequal key lists are refused
- **WHEN** a dataset is saved with a relationship whose `from_columns` has two entries and `to_columns` one
- **THEN** it is refused with a typed error naming both lengths

#### Scenario: Both key spellings are refused
- **WHEN** a relationship carries `join_pairs` and `from_columns`
- **THEN** it is refused with a typed error naming `from_columns` and `join_pairs`

#### Scenario: Reordered pairs are the same relationship
- **WHEN** a dataset declares a relationship with `from_columns: ["a", "b"]` and `to_columns: ["x", "y"]`, and `edit_dataset` removes the relationship edge `{"to": …, "from_columns": ["b", "a"], "to_columns": ["y", "x"]}`
- **THEN** that relationship is removed

### Requirement: A query filter is a string or a list

A query's and a refinement's `filter` SHALL be one condition string or a list of condition strings, all of which must hold. Omitted, `null` and `[]` SHALL all mean no filter and SHALL be omitted when the query is serialized. A filter SHALL otherwise be serialized in the shape it was written. A refinement's conditions SHALL be appended to the saved query's, skipping any the saved query already has; when a refinement adds a condition, the merged filter SHALL be a list.

#### Scenario: A string filter runs and dumps as written
- **WHEN** a query with `filter: "status = 'paid'"` is run and serialized
- **THEN** it returns the rows of the list form `["status = 'paid'"]`, and it serializes with `filter` as that string

#### Scenario: An empty filter means none
- **WHEN** queries are run with `filter` omitted, `null` and `[]`
- **THEN** all three return the same rows, and none serializes a `filter` key

#### Scenario: A refinement appends to a string filter
- **WHEN** a saved query with `filter: "a > 1"` is run by name with a refinement whose `filter` is `"b < 2"`
- **THEN** the run applies both conditions, and the merged query's `filter` is `["a > 1", "b < 2"]`

#### Scenario: A refinement adding nothing keeps the saved filter
- **WHEN** a saved query with `filter: "a > 1"` is run by name with a refinement whose `filter` is `"a > 1"`
- **THEN** the merged query's `filter` is the string `"a > 1"`

### Requirement: Datasets refuse unknown keys

A fresh dataset carrying a key that SLayer does not read, on the dataset or on any nested field, metric, relationship, aggregation or aggregation parameter, SHALL be refused with a typed error naming the key, its path and the dataset, after any pre-2.0 spelling has converted. This SHALL hold for nested parts built as Python objects before the dataset, on every save surface, and for Ossie keys SLayer does not read yet. A stored dataset carrying such keys SHALL load without them, with one structured warning per document, emitted as a Python warning and carried on the response or save that loaded it, naming the dataset and every dropped key's path.

#### Scenario: A fresh unknown key is refused
- **WHEN** a dataset is created through the MCP tool, the REST route and the Python client with a field carrying `primaryKey: true`
- **THEN** each refuses it with a typed error naming `primaryKey`, the field's path and the dataset

#### Scenario: A prebuilt nested object keeps its stray key for the refusal
- **WHEN** a Python `DatasetMetric` is built with a stray keyword and placed in a fresh dataset
- **THEN** building it or the dataset is refused with a typed error naming the stray key

#### Scenario: An Ossie key SLayer does not read yet is refused
- **WHEN** a fresh dataset carries `ai_context`
- **THEN** it is refused with a typed error naming `ai_context`

#### Scenario: A stored unknown key is dropped with one warning
- **WHEN** a store holding a current-version dataset with an unknown key at its root and another on one of its relationships is opened and queried
- **THEN** the dataset loads without both keys, and exactly one structured warning names the dataset and both key paths

### Requirement: Datatype values are Ossie's names

A `datatype` or list element type SHALL be one of `String`, `Integer`, `Decimal`, `Float`, `Boolean`, `Date`, `Time`, `DateTime`, `DateTimeTz`, `Opaque` and `Array` (a list), matched by exact case, and SHALL be stored, dumped, exported and displayed as written. Each value SHALL behave in queries as the type it corresponds to before this change: `Decimal` and `Float` as the double type, `DateTime` and `DateTimeTz` as the timestamp type, `Time` and `Opaque` as the opaque type. A pre-2.0 value SHALL convert: `TEXT`, `INT`, `BOOLEAN`, `DATE`, `UNKNOWN` and `ARRAY` (and their lowercase legacy spellings) to `String`, `Integer`, `Boolean`, `Date`, `Opaque` and `Array`; `DOUBLE` and `number` to `Decimal` when the field's database type is exact numeric, else `Float`; `TIMESTAMP` and lowercase `time` to `DateTimeTz` when the field's database type carries a time zone, else `DateTime`. Ingestion and drift SHALL assign the finer value from the database type by the same rule, and the Ossie importer SHALL keep an Ossie datatype as written.

#### Scenario: Ossie's types are kept as written
- **WHEN** a dataset with fields typed `Decimal`, `DateTimeTz` and `Time` is saved, loaded, dumped and exported to Ossie
- **THEN** each field still carries its type as written

#### Scenario: Behaviour follows the old kind
- **WHEN** a query aggregates a `Decimal` field, filters a `DateTimeTz` field by a date range and selects a `Time` field
- **THEN** the results equal those of the same dataset typed `DOUBLE`, `TIMESTAMP` and `UNKNOWN` before this change

#### Scenario: A pre-2.0 type converts by database type
- **WHEN** a fresh dataset has a field typed `DOUBLE` with database type `NUMERIC(18, 2)`, a field typed `TIMESTAMP` with database type `TIMESTAMPTZ`, a field typed `DOUBLE` with no database type and a metric typed `TIMESTAMP`
- **THEN** they are stored as `Decimal`, `DateTimeTz`, `Float` and `DateTime`, with one `deprecated_input` warning for `DOUBLE` and one for `TIMESTAMP`

#### Scenario: Case matters
- **WHEN** fresh datasets type a field `Time` and another `time`
- **THEN** the first is stored as `Time`, and the second as `DateTime` with a `deprecated_input` warning naming `time`

#### Scenario: A list type shows its element
- **WHEN** a list field of strings is inspected
- **THEN** its type is shown as `Array<String>`
