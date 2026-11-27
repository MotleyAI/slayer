# models/semantic-model-documents Specification

## Purpose
Stores Ossie semantic-model documents natively, as written, and decomposes each into the native datasets, relationships and model-scoped metrics of its datasource, so that every valid Ossie document is valid SLayer configuration with Ossie's meaning.

## Requirements

### Requirement: A semantic-model document is a stored entity bound to its datasource by location
Storage SHALL persist a semantic-model document per datasource, identified by `(data_source, name)`: in YAML storage at `semantic_models/<data_source>/<name>.yaml` beside `models/<data_source>/`, and in SQLite storage in a `semantic_models` table holding the document text verbatim. The document's datasource SHALL be the one it is stored under; an Ossie document carries no connection. The document's `name` SHALL equal its storage name. A document SHALL be listable, readable as its stored text, replaceable and deletable through storage. The path components SHALL obey the existing datasource and dataset path rules.

#### Scenario: A document file binds to its directory's datasource
- **WHEN** `semantic_models/shop/sales.yaml` holds a valid Ossie document named `sales` declaring datasets `orders` and `customers`
- **THEN** the datasource `shop` lists the datasets `orders` and `customers`, and the document `sales` is listed for `shop`

#### Scenario: SQLite stores the text verbatim
- **WHEN** a document whose YAML text carries comments is saved to SQLite storage and read back
- **THEN** the text read back equals the text saved, comments included

### Requirement: Only Ossie's current document shape is accepted natively
A stored document SHALL be a flat Ossie document of version `0.3.0.dev0`: `version`, `name`, a non-empty `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics` and `nonportable_extensions`, with every nested entity in Ossie's shape. A key Ossie does not define, at any level, SHALL make the document unloadable with a typed error naming the document and the key; so SHALL an unknown `version`, the legacy `semantic_model` wrapper, and the legacy `custom_extensions` key. The error for an earlier shape SHALL name `slayer import-osi` as the upgrade path. A document whose text repeats a key within one mapping, or holds a scalar its YAML tag cannot construct (such as a malformed timestamp), SHALL be invalid with a typed reason naming the key or the value, whichever SLayer surface reads the text (storage, import, export); a YAML merge key (`<<`) SHALL keep YAML 1.1 precedence (earlier merge sources win, explicit keys beat merged ones) as upstream's loader does. An unloadable document SHALL follow `models/document-isolation`.

#### Scenario: An unknown key refuses the document
- **WHEN** a stored document's dataset carries the key `grain`
- **THEN** loading the datasource reports the document as unloadable, naming `grain`, and none of its datasets are visible

#### Scenario: A legacy wrapper names the upgrade path
- **WHEN** a file in `semantic_models/shop/` holds a `semantic_model:` wrapper
- **THEN** the document is unloadable with an error naming `slayer import-osi`

#### Scenario: A repeated key refuses the document
- **WHEN** a stored document's dataset `orders` declares `source: db.s.orders` and then `source: db.s.other`
- **THEN** the document is unloadable with an error naming the repeated key `source`, and none of its datasets are visible

#### Scenario: A malformed timestamp refuses the document
- **WHEN** a stored document holds the value `!!timestamp not-a-date`
- **THEN** the document is unloadable with a typed error naming the value, and loading the datasource does not crash

#### Scenario: Merge keys keep their precedence
- **WHEN** a document's field mapping is `<<: [*first, *second]` where both anchors set `description`
- **THEN** the field loads with the description from `*first`

### Requirement: Ossie's version and SLayer's stored version never collide
A document's `version` SHALL be read only as the Ossie specification version. SLayer's stored-version counter for a document SHALL live in a document-level `nonportable_extensions` entry with `vendor_name: SLAYER`; a document without such an entry SHALL be read as current. Migrations SHALL apply only to `SLAYER` payloads, and a load SHALL never write the document back.

#### Scenario: A hand-written document needs no SLayer entry
- **WHEN** a document with `version: 0.3.0.dev0` and no `SLAYER` entry is loaded
- **THEN** it loads as current and its file is unchanged afterwards

### Requirement: Loading a document never introspects and never writes
Reading a document from storage SHALL NOT contact the datasource and SHALL NOT write to storage.

#### Scenario: Loading with the database unreachable
- **WHEN** the datasource's database is unreachable and the datasource's datasets are listed
- **THEN** the document's datasets are listed and no connection is attempted

### Requirement: Datasets decompose into native datasets exposing exactly their declared fields
Each Ossie dataset SHALL be visible as a native dataset of the document's datasource, through every operation that reads datasets (get, load, list, inspect, query, search), exposing exactly its declared fields. Its `source` SHALL be read as a table reference or a query as Ossie defines; each field's `expression` as the field's expression (`models/field-expressions`); `primary_key` as the primary-key flag on each named field (a composite key marks every member); a `unique_keys` entry naming one field as that field's `unique` flag; `dimension.is_time` and Ossie's default from `datatype` as the field's time role; dataset-scoped `metrics` as the dataset's metrics. A `unique_keys` entry naming several fields SHALL be kept in the document and SHALL NOT be used as a uniqueness proof. A dataset declaring no fields SHALL load exposing nothing. An entity's `description`, `label`, `ai_context` and non-portable extensions of any vendor SHALL be kept in the document unchanged. Each dataset's, field's, dataset metric's and relationship's `ai_context` and non-`SLAYER` non-portable extensions SHALL also be read into its metadata (`meta.osi_ai_context`, `meta.osi_nonportable_extensions`), as the 1.x import recorded them, so `inspect` and search show them; a save SHALL NOT write these keys back.

#### Scenario: A field's ai_context reaches inspect and search
- **WHEN** document field `orders.amount` carries `ai_context` synonyms `revenue` and `gross` and a `SNOWFLAKE` non-portable extension
- **THEN** `inspect` of `orders.amount` shows the synonyms, a search for `gross` finds `orders.amount`, its metadata holds the `SNOWFLAKE` entry, and saving `orders` unchanged leaves the document text unchanged

#### Scenario: Undeclared source columns stay invisible
- **WHEN** dataset `orders` has source `sales.public.orders` with columns `o_orderkey` and `o_totalprice`, and declares only the field `order_id` with expression `o_orderkey`
- **THEN** `orders` exposes the field `order_id` only, and a query reading `orders.o_totalprice` fails as an unknown field

#### Scenario: A composite primary key marks every member
- **WHEN** a dataset declares `primary_key: [order_id, line_number]`
- **THEN** both fields carry the primary-key flag and neither is a sole identifier

### Requirement: Relationships decompose into named many-to-one relationships
Each top-level relationship SHALL become a relationship on its `from` dataset, named by the relationship's `name`, targeting `to`, keyed by `from_columns` and `to_columns` (names of declared fields), with cardinality `many_to_one` and a LEFT join unless its `SLAYER` entry declares otherwise. The declared cardinality SHALL prove the hop to-one as `models/join-cardinality` specifies. Relationships of standalone datasets SHALL keep their own declared cardinality. A relationship SHALL name only datasets of its own document.

#### Scenario: A declared relationship proves determination
- **WHEN** a document relates `orders` to `customers` by `orders_to_customers` on `[customer_id]` → `[id]`, and `customers` declares no primary key
- **THEN** a query of `sum(orders.amount)` by `customers.region` evaluates each order once, as attributable

### Requirement: SLAYER entries are read on every load
An entity's `SLAYER` entry SHALL be applied on every load. Its content that duplicates Ossie core content SHALL apply only when the payload's fingerprint matches the entity's core content as stored; otherwise the core content SHALL apply alone. A payload that is malformed, of an unknown version, attached to an entity it does not describe, or one of several `SLAYER` entries on one entity SHALL be ignored; so SHALL a payload whose content would make its dataset invalid or names an entity the document does not hold. Each ignored payload SHALL produce a structured load warning naming the entity and the reason, visible on `inspect`, and SHALL affect no other entity. A relationship's `SLAYER` entry declaring a cardinality or join type SHALL override the many-to-one, LEFT default. A relationship's `SLAYER` entry SHALL restore exactly one relationship; an entry declaring several SHALL be ignored.

#### Scenario: A hand edit makes its payload stale
- **WHEN** a document dataset metric's core `sql` is edited by hand after its `SLAYER` entry recorded the formula `revenue / n`
- **THEN** the metric evaluates its edited core `sql`, and `inspect` shows a load warning naming the metric

#### Scenario: A relationship keeps its declared cardinality
- **WHEN** `edit_dataset` adds a `one_to_many` relationship from document dataset `customers` to document dataset `orders`
- **THEN** the relationship reads back as `one_to_many`

#### Scenario: An entry declaring several relationships is ignored
- **WHEN** a document relationship `orders_to_customers`'s `SLAYER` entry declares both `buyer` on `orders` and `placed` on `customers`
- **THEN** it loads as one many-to-one LEFT relationship `orders_to_customers` on `orders`, and `inspect` shows a load warning naming it

### Requirement: Top-level metrics decompose into model-scoped metrics
Each top-level metric SHALL become a model-scoped metric of the document's datasource (`queries/model-scoped-metrics`), named by its `name`, with its `expression`, `description`, `datatype` and `ai_context` kept.

#### Scenario: A document metric answers by bare name
- **WHEN** a document declares the top-level metric `revenue_per_customer` with expression `SUM(orders.amount) / COUNT(DISTINCT customers.id)`
- **THEN** a query naming the metric `revenue_per_customer` returns that metric's value

### Requirement: Every construct the 1.x importer converted keeps its meaning natively
Each Ossie construct `slayer import-osi` converted in 1.x SHALL evaluate natively, from the stored expression, with the meaning the 1.x conversion gave it. These constructs are:
- `agg(x) FILTER (WHERE p)` for every supported aggregation (including `COUNT(*)`, `COUNT(DISTINCT x)` and percentiles), as the aggregation over the rows where `p` holds, never removing result groups;
- `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`, `CORR`, `COVAR_SAMP`, `COVAR_POP` and `APPROX_COUNT_DISTINCT`;
- `ARRAY_AGG(DISTINCT x)` as `collect(x)`, `ARRAY_CONTAINS(a, v)` and `v = ANY(a)` as `contains(a, v)`, `ARRAY_LENGTH(a)` and `CARDINALITY(a)` as `length(a)`, all with SLayer's NULL conventions. So `ARRAY_AGG(DISTINCT x)` drops NULL inputs, returns an empty list for a cell with no input, and sorts its elements. On a database without lists it is refused with the typed list error;
- non-bare aggregate operands;
- `COUNT(list_field)` counting non-NULL lists;
- date literals compared with temporal fields as instants (a date-only literal at its midnight);
- `source.column` in dataset-scoped metrics.

A field or metric expression with no form SLayer can run on the datasource SHALL be translated in memory at load from the first readable candidate, and SHALL NOT be written back. The candidates are tried in this order: top-level `sql`, an `OSSIE_SQL_2026` entry, an `ANSI_SQL` entry, then any readable SQL dialect entry, in document order within each step.

A construct the 1.x importer reported as unconverted SHALL load per "Entities SLayer cannot evaluate yet still load".

#### Scenario: A FILTER aggregate keeps every group
- **WHEN** a dataset metric `paid` has expression `SUM(amount) FILTER (WHERE status = 'paid')` and a query groups it by `orders.region` where one region has no paid orders
- **THEN** that region's row is present with NULL for `orders.paid`

#### Scenario: An array function maps to the list library
- **WHEN** a field `tag_count` has expression `CARDINALITY(tags)`
- **THEN** it returns the count of non-NULL elements of `tags`, and NULL for a NULL list

#### Scenario: A metric written only as a dialect entry runs
- **WHEN** a document's model-scoped metric `gross_revenue` has only the entry `{dialect: ANSI_SQL, expression: SUM(orders.total_amount)}`
- **THEN** a query naming `gross_revenue` returns the sum of `orders.total_amount`, and the document's text is unchanged

#### Scenario: ARRAY_AGG DISTINCT follows collect's NULL conventions
- **WHEN** an imported dataset metric `statuses` has expression `ARRAY_AGG(DISTINCT status)` over the statuses `paid`, `paid`, NULL and `open`, and a query reads it once with no dimension and once grouped by a field that gives one group no rows with a non-NULL status
- **THEN** on DuckDB it returns `['open', 'paid']` with no NULL element, and the empty group gets `[]`; on SQLite the query fails with the typed list error

### Requirement: Entities SLayer cannot evaluate yet still load
A field or metric of a valid document whose expression SLayer cannot evaluate yet SHALL load with a structured load warning naming the entity and the reason, visible on `inspect`, `datasets_summary` and search results, and reported by `slayer import-osi`. These are: a model-scoped metric reading a field unqualified or outside an aggregate; an expression whose runnable form SLayer's expression reader cannot parse (for example a subquery); a dataset-scoped metric that evaluates a measure of its dataset's source with `AGG(source.measure)` (SLayer's sources expose no measure columns, and Ossie requires a consumer to reject `AGG` on a column that is not a measure and never to replace it with an ordinary aggregate); and any model-scoped metric that reaches such an entity, directly through `AGG(d.m)` or transitively through other model-scoped metrics, whose warning SHALL name the dependency chain. Every query reading the entity SHALL fail with a typed error naming it and the reason; no other entity SHALL be affected. A save SLayer authors that adds or changes a definition to one SLayer cannot evaluate SHALL be refused with that typed error; an unchanged entity that loaded with a warning SHALL survive saves of other parts of its document. A window function SHALL NOT make an entity one SLayer cannot evaluate: a windowed field, a windowed metric and a metric reading either SHALL load without a warning and SHALL be evaluated (`queries/window-functions`).

#### Scenario: A windowed field and metric load and evaluate
- **WHEN** a document declares a field `running_total` with expression `SUM(amount) OVER (ORDER BY order_date)` beside a field `amount`, and a model-scoped metric `running` with expression `SUM(SUM(sales.amount)) OVER ()`
- **THEN** the document loads with no warning on either, `slayer import-osi` reports neither as not evaluable, and queries reading `sales.running_total` and `running` return their values; neither fails

#### Scenario: An expression SLayer cannot parse loads not evaluable
- **WHEN** a document declares a field `flag` with expression `(SELECT 1)` beside a field `amount`
- **THEN** the document loads, `inspect` shows a warning on `flag` naming the parse failure, `slayer import-osi` reports `flag` as not evaluable, a query of `sum(sales.amount)` succeeds, and a query reading `sales.flag` fails with a typed error naming `flag`

#### Scenario: SQL keywords inside literals stay evaluable
- **WHEN** a document declares a field `kind` with expression `CASE WHEN status IN ('select', 'from') THEN 'where' END`
- **THEN** `kind` loads with no load warning and a query reading it succeeds
#### Scenario: A source-measure metric loads and fails only when read
- **WHEN** a document's dataset `orders` declares the metrics `revenue_total` (`AGG(source.revenue)`) and `order_count` (`COUNT(*)`)
- **THEN** the document loads, `inspect` shows on `orders.revenue_total` a warning that it evaluates a measure of its source, which SLayer cannot evaluate yet, a query of `orders.order_count` succeeds, and a query reading `orders.revenue_total` fails with a typed error naming `revenue_total` and that reason

#### Scenario: A model-scoped metric over a source-measure metric fails with its chain
- **WHEN** the same document declares the model-scoped metrics `revenue_per_customer` (`AGG(orders.revenue_total) / COUNT(DISTINCT customers.id)`) and `doubled` (`revenue_per_customer * 2`, stored in the `SLAYER` extension)
- **THEN** both load with warnings, and a query naming `doubled` fails with a typed error naming `doubled`, the chain through `revenue_per_customer` to `orders.revenue_total`, and the reason

#### Scenario: Authoring a source-measure metric is refused
- **WHEN** a dataset tool adds the dataset metric `revenue` with expression `AGG(source.revenue)` to dataset `orders`
- **THEN** the save fails with the typed error naming `revenue` and the reason, and nothing is stored

#### Scenario: An unrelated edit keeps a warned metric
- **WHEN** a dataset tool edits the description of field `orders.order_id` in a document whose `orders.revenue_total` is `AGG(source.revenue)`
- **THEN** the save succeeds, and the document text keeps `revenue_total` unchanged

### Requirement: Dataset and model-scoped metric names are unique per datasource
Within one datasource, dataset names SHALL be unique across all documents and standalone datasets together, and model-scoped metric names SHALL be unique across all documents; a model-scoped metric name SHALL NOT equal a dataset name. Names SHALL compare as `queries/identifier-case` specifies. A save that would introduce a duplicate SHALL be refused with a typed error naming both owners. When stored files collide, the colliding entity SHALL be unloadable in every owner, and so SHALL every entity of the same document that depends on it (a relationship to or from it, a metric reading it), each with a diagnostic naming the cause; every other entity SHALL load. Nothing SHALL silently win a collision.

#### Scenario: A colliding save is refused
- **WHEN** the datasource holds a standalone dataset `orders` and a document declaring a dataset `ORDERS` is saved
- **THEN** the save fails with a typed error naming the standalone dataset and the document

#### Scenario: Hand-placed collisions quarantine dependents
- **WHEN** two stored documents both declare a dataset `orders`, and the first also declares a relationship from `orders` to `customers` and a metric reading `orders.amount`
- **THEN** `orders` is unloadable in both documents, the relationship and the metric are unloadable with diagnostics naming the collision, and `customers` loads

### Requirement: Dataset tools edit a document's datasets through the document
Every operation that writes a dataset SHALL determine whether the dataset is standalone or owned by a document, and for an owned dataset SHALL patch only the affected entities of the owning document: `create_dataset` with a `semantic_model`, `edit_dataset`, `delete_dataset`, the REST and CLI dataset writes, field sampling updates, drift cleanup, cardinality persistence and ingestion upserts. Content Ossie can express SHALL be written as Ossie core; SLayer-only content (row filters, `hidden`, access tags, custom aggregations, query-backed or `sql`-mode sources, field formats and granularities, and any definition that is not valid Ossie core in its position) SHALL be written in a `SLAYER` entry of that entity's `nonportable_extensions`, never approximated in core. A relationship edited on a document dataset SHALL be written to the document's top-level `relationships`. Reading the patched document SHALL yield the dataset exactly as written through the tool. A dataset created without a `semantic_model` SHALL be standalone.

#### Scenario: Adding a SLayer-only filter to a document dataset
- **WHEN** `edit_dataset` adds the row filter `status <> 'test'` and `hidden: true` to the document dataset `orders`
- **THEN** the document's `orders` entry gains a `SLAYER` extension entry carrying the filter and the flag, its Ossie fields are unchanged, and `orders` reads back with the filter applied

#### Scenario: A new relationship lands at the document's top level
- **WHEN** `edit_dataset` adds a relationship from document dataset `orders` to document dataset `stores`
- **THEN** the document's top-level `relationships` gains that relationship and no relationship is written inside the `orders` entry

### Requirement: Saves keep the document's text outside what was edited
A save SHALL change only the nodes of the document that the edit changes. Comments, key order, quoting, blank lines and formatting of every untouched part SHALL be kept, in YAML and SQLite storage alike; a JSON document SHALL keep its key order.

#### Scenario: A comment survives an unrelated edit
- **WHEN** a document carries the comment `# owned by finance` above dataset `customers`, and `edit_dataset` changes the description of dataset `orders`
- **THEN** the saved text differs from the original only in the description of `orders`

### Requirement: Concurrent writes to one document never lose an update
Writes to one document SHALL be serialised: each write SHALL read, patch and store the document as one atomic step against the latest stored text, in YAML storage under a per-document lock and in SQLite storage in one immediate transaction.

#### Scenario: Two edits to different datasets of one document
- **WHEN** two `edit_dataset` calls change the descriptions of datasets `orders` and `customers` of one document concurrently
- **THEN** the stored document carries both new descriptions

### Requirement: Document writes invalidate every graph cache
Any change to a document's content or membership, by SLayer or by an external edit of the stored file, SHALL change the storage's `graph_fingerprint`, so engine bundles and access-filtered views rebuild. A custom backend implementing the semantic-model hooks SHALL meet the same contract.

#### Scenario: An external edit is seen on the next query
- **WHEN** a document file is edited outside SLayer to add a field, and a query reads the new field
- **THEN** the query resolves the new field without restarting the engine

### Requirement: Deleting document entities runs the dataset cascades
Deleting a document dataset SHALL run the same cascade as deleting a standalone dataset (its embeddings, its memory entities, relationships of other datasets that target it) and SHALL remove its relationships from the document; a delete that would break a model-scoped metric SHALL be refused as "Explicit changes never break a model-scoped metric" specifies. Deleting a document SHALL delete each of its datasets through that cascade, plus the embeddings of the document and of its model-scoped metrics.

#### Scenario: Deleting a document dataset strips memory entities
- **WHEN** a memory links the entity `shop.orders`, no model-scoped metric reads `orders`, and the document dataset `orders` is deleted
- **THEN** the memory no longer lists `shop.orders` and the document no longer declares `orders` or its relationships

### Requirement: Access filtering covers documents
An access-filtered view SHALL read every document dataset the caller cannot access as deleted, prune relationships into it, and prune every model-scoped metric that reads it through any aggregate component or through other model-scoped metrics, transitively. A pruned metric SHALL fail as an unknown metric with no suggestion naming a hidden entity. A write through the view SHALL merge onto the full document, never dropping or exposing hidden entities. A stored document whose text does not read SHALL be refused to a caller without bypass on read, edit and delete, with the typed error and reasons of "A document whose text repeats a mapping key is refused", since the access of its datasets cannot be decided; a bypass caller SHALL read and delete it as stored.

#### Scenario: A metric over a hidden dataset is invisible
- **WHEN** a caller cannot access dataset `payroll`, and the model-scoped metric `cost_per_head` reads `payroll.salary`
- **THEN** the caller's query naming `cost_per_head` fails as an unknown metric that suggests no hidden name

#### Scenario: An unreadable document fails closed through the filter
- **WHEN** a stored document repeats the key `source`, and a caller without bypass reads, edits or deletes it through an access-filtered view
- **THEN** each call fails with a typed error naming `source` and its line, the stored text is unchanged, and a bypass caller can read and delete it

### Requirement: Fields without a datatype load with an unknown type
A field that declares no `datatype` SHALL load with an unknown type that no check or coercion reads. The engine SHALL resolve unknown types before binding a query that reads them, once per engine and storage fingerprint, without blocking the event loop, from the live type of the field's expression. The resolved type SHALL NOT be persisted. A save-time check that needs an unknown type SHALL be deferred to the query-time check.

On SQLite, whose values carry their own types, the live type SHALL be the one SLayer's SQLite ingestion gives, from one classification shared with ingestion:
- A field that reads one source column SHALL get the column's declared type, refined by the bounded sample of its stored values exactly as ingestion refines it. The declared type is kept when the sample is empty, all NULL or saturated, or the probe fails.
- Any other field SHALL get its type from the same bounded census over a sample of its values. With no non-NULL value in the sample it SHALL be String.

#### Scenario: An untyped numeric field aggregates
- **WHEN** a document field `amount` with expression `amount` declares no `datatype` and its source column is a decimal
- **THEN** `sum(orders.amount)` returns the column's sum and the document's text is unchanged

#### Scenario: An untyped SQLite column aggregates
- **WHEN** on SQLite a document declares the fields `amount` (expression `amount`, over a REAL column), `qty` (expression `qty`, over an INTEGER column) and `doubled` (expression `amount * 2`), none with a `datatype`
- **THEN** `sum(orders.amount)`, `sum(orders.qty)` and `sum(orders.doubled)` return the sums the source table gives, and the document's text is unchanged

#### Scenario: SQLite typing agrees with ingestion
- **WHEN** on SQLite an untyped document field reads one source column, and that table is also ingested, for each of these columns: declared INTEGER with integer values, declared INTEGER holding a real value, declared REAL in an empty table, declared INTEGER holding only NULLs, declared TEXT holding numeric text, declared TEXT holding non-numeric text, and holding a blob
- **THEN** the field's resolved type equals the type ingestion gives that column in every case

#### Scenario: An untyped SQLite expression over no rows is String
- **WHEN** on SQLite an untyped document field has expression `amount * 2` and the source table has no rows
- **THEN** the field resolves to String, as it does today

### Requirement: Surfaces create, list, inspect, edit and delete documents
MCP SHALL offer `create_semantic_model` (from Ossie document content, or from `datasets` naming standalone datasets to move into the new document), `edit_semantic_model` (document fields, model-scoped metrics, relationships, and `add_datasets` moving standalone datasets in) and `delete_semantic_model`; `inspect` SHALL accept `entity_type="semantic_model"`, and `datasets_summary` SHALL show each dataset's document and each datasource's model-scoped metrics. REST SHALL expose `/semantic-models` (list, get by `{data_source}/{name}`, create, replace, delete); the Python client SHALL offer the same operations, sync and async; the CLI SHALL offer `slayer semantic-models list|show|delete`. Moving a standalone dataset into a document SHALL keep its content exactly, through the mapping of "Dataset tools edit a document's datasets through the document".

#### Scenario: Native authoring without an Ossie file
- **WHEN** a user with standalone datasets `orders` and `customers` calls `create_semantic_model(name="sales", data_source="shop", datasets=["orders", "customers"])` and then `edit_semantic_model` adding the metric `revenue_per_customer`
- **THEN** both datasets answer queries exactly as before, now from document `sales`, and the metric answers by bare name

### Requirement: Search indexes documents and model-scoped metrics
Search SHALL index each document (its name, `description` and `ai_context`) and each model-scoped metric (its name, description and `ai_context`, canonical id `<data_source>.<metric>`), and SHALL refresh them on the same events that refresh datasets.

#### Scenario: A model-scoped metric is found by its synonym
- **WHEN** model-scoped metric `revenue_per_customer` carries the `ai_context` synonym `ARPU` and a search asks for `ARPU`
- **THEN** the results include `shop.revenue_per_customer`

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
Every upstream case script `compliance/wide_table/<case>.ost` SHALL run against the model its script loads, stored as a native document over the SQL data its script connects to, seeded into each Tier-1 database (`compatibility/ossie-compliance`), with the query of its `executeLower` submitted verbatim under `conformance="ossie-l3"` (`queries/ossie-l3-binding`). A row case (`a*`) SHALL return exactly the script's expected rows, compared as a multiset by logical type, with the result columns named and ordered as the expected header; an error case (`e*`) SHALL raise the Ossie code its expected error starts with. The script's lowering SQL is not part of the oracle. Every row case SHALL also return exactly the expected rows under the default conformance.

#### Scenario: The model-scoped metric components case
- **WHEN** case `a08_model_metric_components` runs on the `retail` data and `retail_model`
- **THEN** the result equals its expected rows on every Tier-1 database

#### Scenario: Error cases raise their codes under strict
- **WHEN** case `e16_partial_dimension_reachability` runs on the `retail` data and `retail_model` under `conformance="ossie-l3"`
- **THEN** it raises `E_PARTIAL_DIMENSION_REACHABILITY`, while the same query under the default conformance answers

#### Scenario: Output names label the strict result
- **WHEN** case `a09_aliases` runs under `conformance="ossie-l3"`
- **THEN** the result columns are exactly `area`, `revenue`, in that order

#### Scenario: A NULL group matches on every database
- **WHEN** case `a17_unmatched_home_null_group` runs on SQL Server, ClickHouse and BigQuery
- **THEN** its NULL-keyed row equals the expected row on each

#### Scenario: A quoted cell keeps its text
- **WHEN** an expected table cell is written quoted, such as `"a|b"` or `""`
- **THEN** it is compared as the string it encodes, `a|b` or the empty string, not split or read as NULL

### Requirement: SLayer's round trip grades portable on upstream's ladder
`slayer ossie-roundtrip <in> <out>` SHALL import the input document natively into scratch storage and export it to `<out>`. Upstream's portability grader, vendored with the corpus, SHALL grade every element of rungs m0–m8 `PORTABLE` through it.

#### Scenario: The ladder grades portable
- **WHEN** the grader runs over m0–m8 through `slayer ossie-roundtrip`
- **THEN** every element is graded `PORTABLE`

### Requirement: A document whose text repeats a mapping key is refused
A stored document whose YAML or JSON text repeats an explicit mapping key, at any depth, SHALL be unloadable with a typed error naming the document and every repeated key with its line; repeated YAML merge directives SHALL be allowed. A scalar the YAML loader cannot build SHALL make the document unloadable naming the value and its line. A save or edit whose text repeats a key SHALL be refused with the same reasons, storing nothing. An unloadable document SHALL follow `models/document-isolation`.

#### Scenario: A hand-placed document with a repeated key
- **WHEN** a file in `semantic_models/shop/` declares the key `source` twice in dataset `orders`
- **THEN** loading the datasource reports the document as unloadable naming `source` and its line, none of its datasets are visible, and the datasource's other documents load

#### Scenario: A save with a repeated key
- **WHEN** a document's text saved through SLayer repeats the key `name` in a field
- **THEN** the save fails with a typed error naming `name` and its line, and the stored document is unchanged

### Requirement: The native validity check agrees with upstream's validator
SLayer's validity check SHALL report a document invalid exactly when the vendored upstream `validation/validate.py` reports an error for it, on every sqlglot version SLayer supports, including:
- In a dataset-scoped `OSSIE_SQL_2026` metric, `AGG` SHALL be valid only as `AGG(source.measure)`: a single column argument qualified by `source` in any case, with no further qualifier, no `*`, a non-empty name and no `FILTER` clause, in a dataset that declares no field named `source`. Any other `AGG` call there SHALL be an error stating that `AGG` in a dataset-scoped metric takes a single `source.measure` reference; a `FILTER` clause and a field named `source` SHALL each be an error stating that cause.
- `AGG` in a field expression SHALL be an error stating that `AGG` is allowed only in metrics, as `AGG(dataset.metric)` in a model-scoped metric and `AGG(source.measure)` in a dataset-scoped metric.
- In `OSSIE_SQL_2026` (the top-level `sql`, or an `OSSIE_SQL_2026` entry it does not supersede), `INTERVAL`, an `IN` list with no value, an alias (`a b`), and `DISTINCT` without an argument SHALL each be an error; other dialects SHALL be unaffected.
- In an `OSSIE_SQL_2026` field expression, an aggregate call without an `OVER` clause SHALL be an error naming each such function once, in order, and stating that a field expression is scalar. An aggregate in a window's `PARTITION BY` or `ORDER BY`, or in the windowed call's argument, counts as having no `OVER` clause. `AGG` stays with the `AGG` rule, and a call inside a subquery is not checked. Other dialects SHALL be unaffected.
- Every line of the vendored `compliance/expressions/valid.txt` SHALL pass the syntax check of an `OSSIE_SQL_2026` expression, and every line of `invalid.txt` SHALL fail it, except a construct upstream's validator still accepts (a top-level `SELECT`), which SHALL be accepted as upstream accepts it.

#### Scenario: A source-measure call is accepted
- **WHEN** a dataset-scoped metric's `sql` is `COALESCE(AGG(source.revenue), 0) - AGG(source.refunds)`
- **THEN** the document is valid

#### Scenario: AGG of a field in a dataset metric is refused
- **WHEN** a dataset-scoped metric `orders.revenue_total` has `sql: AGG(total_usd)` where `total_usd` is a field
- **THEN** the document is invalid with an error naming `orders.revenue_total` and stating that `AGG` in a dataset-scoped metric takes a single `source.measure` reference

#### Scenario: A field named source blocks source measures
- **WHEN** dataset `orders` declares a field named `source` and a metric with `sql: AGG(source.revenue)`
- **THEN** the document is invalid with an error stating that the dataset declares a field named `source`

#### Scenario: Vendor dialects are unaffected
- **WHEN** a dataset-scoped metric has only the entry `{dialect: SNOWFLAKE, expression: "AGG(total_usd)"}` and a field has only `{dialect: POSTGRES, expression: "order_date + INTERVAL '7 days'"}`
- **THEN** the document is valid

#### Scenario: Portable INTERVAL is refused
- **WHEN** a field has `sql: order_date + INTERVAL '7 days'`
- **THEN** the document is invalid with an error stating that `INTERVAL` is not part of `OSSIE_SQL_2026`

#### Scenario: An aggregate in a field is refused
- **WHEN** a field `revenue` has `sql: SUM(amount)`, and separately `sql: amount / SUM(amount) OVER ()`
- **THEN** the first document is invalid with an error stating that `revenue` calls SUM without an OVER clause in a field expression, and the second is valid

#### Scenario: Both sqlglot shapes of AGG are checked
- **WHEN** the same document with a model-scoped metric `AGG(nope)` is checked with `AGG` parsed as a generic function call and as sqlglot's dedicated `AGG` node
- **THEN** both checks report the same error for `AGG(nope)`

### Requirement: Portable SQL SLayer writes is valid OSSIE_SQL_2026
Every `OSSIE_SQL_2026` text SLayer writes — a field or metric expression saved through the dataset tools or `edit_semantic_model`, a model-scoped metric saved through SLayer, a document `slayer import-osi` upgrades from an earlier shape, and every portable `sql` the exporter emits — SHALL pass the native validity check. `INTERVAL` arithmetic SHALL be written as `DATEADD(<unit>, <n>, <expr>)`, a subtraction as a negative `<n>`, and a chain as nested calls, with the same meaning; queries SHALL keep accepting `INTERVAL`. Document text SLayer does not rewrite SHALL be kept byte for byte.

#### Scenario: A saved INTERVAL field is stored as DATEADD
- **WHEN** a dataset tool saves the field `due` with expression `order_date + INTERVAL '7 days'`
- **THEN** the stored expression is `DATEADD(day, 7, order_date)`, a later save without edits stores it unchanged, and `due` returns the same values as before the change

#### Scenario: A model-scoped metric filter with INTERVAL is stored as DATEADD
- **WHEN** `edit_semantic_model` adds the metric `recent` with expression `SUM(orders.amount) FILTER (WHERE orders.ordered_at > CURRENT_DATE - INTERVAL '30 days')`
- **THEN** the document text holds `DATEADD(day, -30, CURRENT_DATE)` in that position and the document passes the native validity check

### Requirement: Trust, enforced keys and an unset cardinality round-trip
A relationship's `trusted` flag and unset cardinality, and a dataset's `enforced_keys`, SHALL
be written to the entity's SLAYER entry and restored on load. A relationship whose
cardinality is unset SHALL NOT be written as a plain Ossie relationship. An Ossie
relationship without a SLAYER entry SHALL load untrusted with cardinality `many_to_one`, and a
dataset without one with no enforced keys.

#### Scenario: An imported document is never trusted
- **WHEN** the Ossie retail model is stored and queried on data with customer C1 duplicated
- **THEN** EAST is 350

#### Scenario: A trusted relationship round-trips
- **WHEN** a document dataset's relationship is saved with `trusted: true`, the document is
  exported and imported again
- **THEN** the relationship is trusted and the duplicated-C1 query returns EAST 650

#### Scenario: An unset cardinality round-trips
- **WHEN** a document dataset's relationship is saved with no cardinality
- **THEN** it loads with no cardinality, not `many_to_one`

#### Scenario: Enforced keys round-trip
- **WHEN** a document dataset is saved with `enforced_keys: [[customer_id]]`
- **THEN** it loads with the same enforced keys

### Requirement: Explicit changes never break a model-scoped metric
A model-scoped metric's references are the datasets it reads, each field `d.f` it reads, each dataset metric it evaluates as `AGG(d.m)`, and each model-scoped metric it reads by name. A change breaks a stored model-scoped metric when, after it, the metric reaches, directly or through other model-scoped metrics, an unresolved reference it did not reach before. An explicit change SHALL be refused, writing nothing, when it breaks a stored model-scoped metric: saving, editing, deleting or moving a dataset to another datasource, saving a document dataset's SLayer extensions, and saving, editing or deleting a semantic-model document. The refusal SHALL list every model-scoped metric the change breaks, with its semantic model and the reference or dependency chain that breaks it, together with every stored ontology mapping the change breaks (`ontologies/ontology-documents`), in one error. A reference already unresolved before the change SHALL NOT cause a refusal. When a broken metric is hidden from the caller by access tags, the refusal SHALL be a hidden-content conflict that names no hidden content. Database-driven changes (ingest, drift cleanup, live-schema refinement) SHALL apply, warn naming each model-scoped metric they break and why, and move each broken metric written in the document's Ossie core into the document's `SLAYER` entry, so the stored document stays Ossie-valid. A model-scoped metric with an unresolved reference, or reading such a metric through other model-scoped metrics, SHALL be unloadable, with a diagnostic naming the reference or the dependency chain; every other entity SHALL load. Every surface SHALL report a refusal with every broken metric; REST SHALL answer it with 400.

#### Scenario: Deleting a dataset a metric reads is refused
- **WHEN** document `sales` declares the model-scoped metric `revenue_per_customer` reading `customers.customer_id`, and dataset `customers` is deleted
- **THEN** the delete is refused naming `revenue_per_customer` of `sales` and `customers`, and the document is unchanged

#### Scenario: Removing a field a metric reads is refused
- **WHEN** the model-scoped metric `amount_total` (`SUM(orders.amount)`) reads `orders.amount`, and a save of dataset `orders` drops field `amount`
- **THEN** the save is refused naming `amount_total` and `orders.amount`, and nothing is stored

#### Scenario: Renaming a field a metric reads is refused
- **WHEN** a save of dataset `orders` renames field `amount`, which `amount_total` reads, to `amount_usd`
- **THEN** the save is refused naming `amount_total` and `orders.amount`, and nothing is stored

#### Scenario: Removing a dataset metric a metric evaluates is refused
- **WHEN** `revenue_per_customer` reads `AGG(orders.total_revenue)`, and a save of dataset `orders` removes or renames the dataset metric `total_revenue`
- **THEN** the save is refused naming `revenue_per_customer` and `orders.total_revenue`, and nothing is stored

#### Scenario: Removing a metric another metric reads is refused
- **WHEN** the model-scoped metric `doubled` (`revenue_per_customer * 2`) reads `revenue_per_customer`, and `edit_semantic_model` removes `revenue_per_customer`
- **THEN** the edit is refused naming `doubled` and `revenue_per_customer`, and the document is unchanged

#### Scenario: A dependency chain is named
- **WHEN** `doubled` reads `revenue_per_customer`, and dataset `customers` is deleted
- **THEN** the delete is refused naming `revenue_per_customer`, and `doubled` with the chain through `revenue_per_customer` to `customers`

#### Scenario: Replacing a document's text without a read dataset is refused
- **WHEN** the text of document `sales` is replaced by one that keeps `revenue_per_customer` and drops dataset `customers`
- **THEN** the save is refused naming `revenue_per_customer`, and the stored text is unchanged

#### Scenario: Removing a SLayer-only field a metric reads is refused
- **WHEN** a model-scoped metric reads the field `orders.net`, held only in the dataset's `SLAYER` entry, and saving the dataset's SLayer extensions drops `net`
- **THEN** the save is refused naming the metric and `orders.net`, and nothing is stored

#### Scenario: One refusal lists a mapping and a metric
- **WHEN** a stored mapping maps `customers.region` and `revenue_per_customer` reads `customers`, and dataset `customers` is deleted
- **THEN** the delete is refused in one error naming both the mapping and `revenue_per_customer`

#### Scenario: A metric already unresolved blocks nothing new
- **WHEN** a stored model-scoped metric reads `orders.discount`, which `orders` does not declare, and a save of `orders` changes the description of field `status`
- **THEN** the save succeeds

#### Scenario: A further break of a partly unresolved metric is refused
- **WHEN** a stored model-scoped metric reads `orders.discount`, which `orders` does not declare, and `orders.amount`, and a save of `orders` drops field `amount`
- **THEN** the save is refused naming the metric and `orders.amount`

#### Scenario: Making a metric read an unresolved metric is refused
- **WHEN** a stored model-scoped metric `a` reads `orders.discount`, which `orders` does not declare, and `edit_semantic_model` changes the metric `b` to read `a`
- **THEN** the edit is refused naming `b` and the chain through `a` to `orders.discount`, and the document is unchanged

#### Scenario: A refused move writes nothing
- **WHEN** dataset `customers`, which `revenue_per_customer` reads, is moved to another datasource
- **THEN** the move is refused naming `revenue_per_customer`, the other datasource holds no `customers`, and `shop` still holds it

#### Scenario: A break of a hidden metric is a hidden-content conflict
- **WHEN** a caller cannot access dataset `payroll`, the model-scoped metric `cost_per_head` reads `payroll.salary` and `staff.head_count`, and the caller deletes the visible dataset `staff`
- **THEN** the delete is refused with a hidden-content conflict naming neither `cost_per_head` nor `payroll`, and nothing is deleted

#### Scenario: A drift cleanup that drops a field applies with a warning
- **WHEN** column `amount` is dropped from the database, `amount_total` (`SUM(orders.amount)`) is written in the document's Ossie core, `twice` (`amount_total * 2`) reads it, and the drift cleanup removes field `orders.amount`
- **THEN** the cleanup applies with a warning naming `amount_total` and `twice`, both metrics are held in the document's `SLAYER` entry, the stored document is Ossie-valid, and on load both are unloadable with diagnostics naming `orders.amount` and, for `twice`, the chain through `amount_total`

#### Scenario: A drift cleanup that drops a dataset applies with a warning
- **WHEN** table `customers` is dropped from the database and the drift cleanup deletes dataset `customers`
- **THEN** the cleanup applies with a warning naming `revenue_per_customer`, and on load `revenue_per_customer` is unloadable with a diagnostic naming `customers`, while `orders` loads

#### Scenario: REST reports a refused delete
- **WHEN** `DELETE /datasets/customers` would break `revenue_per_customer`
- **THEN** the response is a 400 naming `revenue_per_customer`, and `customers` is kept

### Requirement: Scalar functions and CASE around aggregates evaluate as written
An Ossie document's metric whose expression applies a scalar function or `CASE` to aggregates SHALL load with no load warning, SHALL NOT be reported as not evaluable by `slayer import-osi`, and SHALL return the value the same SQL returns over the source rows of each result group. This includes:
- `IFF`, `IF`, `ZEROIFNULL`, `NVL`, `NVL2`, `NULLIFZERO`, `COALESCE`, `NULLIF`, `ROUND` and `ABS` applied to aggregates or arithmetic over them;
- `CASE` with or without `ELSE`, whose conditions or branches are aggregates;
- the same around an aggregate with `FILTER (WHERE …)`.

This SHALL hold for dataset-scoped and model-scoped metrics alike. A field whose expression is a `CASE` or `IFF` over its row's values SHALL likewise load without a warning and return the row's value.

#### Scenario: Scalars around aggregates match SQL per group
- **WHEN** a document's dataset `orders` has the metrics `IFF(SUM(amount) > 0, SUM(amount), 0)`, `ZEROIFNULL(SUM(amount))`, `NVL2(MAX(amount), 1, 0)`, `NULLIFZERO(COUNT(amount))`, `ROUND(AVG(amount), 1)`, `CASE WHEN COUNT(*) > 1 THEN SUM(amount) END` and `ZEROIFNULL(SUM(amount) FILTER (WHERE status = 'paid'))`, and a group has only NULL amounts
- **THEN** grouped by `orders.status`, on DuckDB and on SQLite, each metric equals the same expression evaluated as SQL over that group's rows, and the import reports nothing for them

#### Scenario: A model-scoped CASE over aggregates evaluates
- **WHEN** a document has the model-scoped metric `CASE WHEN SUM(orders.amount) > 15 THEN 'hi' ELSE 'lo' END`
- **THEN** a query naming it, with no dimension and grouped by `orders.status`, returns the label the same SQL gives per group on DuckDB and on SQLite

#### Scenario: A CASE field evaluates per row
- **WHEN** a document field `tier` has expression `CASE WHEN amount > 10 THEN 'big' WHEN amount IS NULL THEN 'none' ELSE 'small' END`
- **THEN** grouping by `orders.tier` returns the labels the same SQL gives over the source rows, on DuckDB and on SQLite

#### Scenario: Imported first and last windows keep their window form
- **WHEN** a document declares the fields `FIRST_VALUE(id) OVER (ORDER BY at)` and `LAST_VALUE(id) OVER (ORDER BY at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`, each also written with `DESC` and with an explicit `NULLS FIRST`, over rows whose `at` includes NULL
- **THEN** the stored text keeps every window form, the import reports none as not evaluable, and each field returns the value the window gives under Ossie's NULL placement on DuckDB and on SQLite, which differs from `first(id, at)` / `last(id, at)` on the NULL-keyed row

### Requirement: The corpus import reports only known unevaluable entities
Importing each Ossie corpus document into its own empty datasource SHALL report as not evaluable exactly the entities a committed manifest lists, each with its reason category. Any entity the import newly reports as not evaluable, or no longer reports, SHALL fail the corpus test until the manifest is updated.

#### Scenario: A new not-evaluable entity fails the corpus test
- **WHEN** every corpus document is imported
- **THEN** the set of (document, entity, reason category) the import reports as not evaluable equals the committed manifest
