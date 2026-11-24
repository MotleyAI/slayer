## ADDED Requirements

### Requirement: A load never writes

Loading a stored dataset, semantic-model document, saved query, memory or datasource SHALL NOT write to the store, on the YAML and the SQLite store alike. Every migration, repair and conversion a stored document needs SHALL apply in memory on each load. This includes schema migrations, join-key and join-target repairs, legacy alias rewrites, field `sql` conversion, query upgrades and the library-granularity settle. The upgraded form SHALL be persisted only by a save of that document or by `slayer storage upgrade`. Datasource priority and embeddings SHALL be neither read as documents to upgrade nor rewritten. Opening a store SHALL keep its one-time layout and schema moves (the YAML pre-v4 flat-layout move, the legacy memory-file split, the SQLite table rebuilds, the embedding sidecar's kind rename), which change no document's content; they are the only writes opening a store performs.

#### Scenario: Loading an old store leaves it unchanged
- **WHEN** the committed 0.10.2 and 1.8.2 stores, as YAML and as SQLite, are opened and every dataset, saved query, memory and datasource is loaded twice
- **THEN** every document loads with its upgraded content both times, the logical content of every stored document is unchanged, and no storage write hook is called

#### Scenario: A save persists the upgraded form
- **WHEN** a dataset stored below the current version is loaded and then saved
- **THEN** the stored document is at the current version with the upgraded content

#### Scenario: Opening a pre-v4 flat-layout YAML store
- **WHEN** a YAML store in the pre-v4 flat layout is opened
- **THEN** its files move into the per-datasource layout and no document's content changes

### Requirement: A load warns when a document needs upgrading

Loading a stored document that the upgrade would change SHALL emit one structured warning per document per storage instance. The warning SHALL name the entity kind, the document's identity and `slayer storage upgrade`. It is emitted whether the change comes from the document's own version or from a nested stage, removed form or repair. A document the upgrade would not change SHALL load without that warning.

#### Scenario: An old dataset warns once
- **WHEN** a dataset stored at version 15 is loaded three times through one storage instance
- **THEN** exactly one warning names the dataset, its datasource and `slayer storage upgrade`

#### Scenario: A current document with an old nested stage warns
- **WHEN** a query-backed dataset stored at the current version holds a stage written in a form the upgrade converts
- **THEN** loading it warns naming the dataset

#### Scenario: An upgraded store is quiet
- **WHEN** a store that `slayer storage upgrade` has upgraded is loaded
- **THEN** no upgrade warning is emitted

### Requirement: slayer storage upgrade persists the current format

`slayer storage upgrade` SHALL bring every stored document that needs it to the current format, through the same upgrade a load applies. The documents are datasets, semantic-model documents, saved queries, memories and datasources, optionally limited by `--data-source`. Each changed document SHALL be written with its upgraded content and without the semantic checks of a client save, so a legacy document that loads (for instance one holding a cycle or a fanning reference) is upgraded rather than refused. A document SHALL be written only if its stored form is unchanged since the upgrade read it. Otherwise it SHALL be reported as a conflict and left as it is. The library-granularity settle SHALL persist dependent documents before their datasource. `--dry-run` SHALL print, per document, the difference the upgrade would make and SHALL write nothing.

A document the upgrade cannot complete SHALL be reported with its identity and the reason, and SHALL be left untouched, while every other document is upgraded. The command SHALL then exit 1. Such documents include:
- one that fails to load;
- one with field `sql` waiting for a datasource dialect that neither the store nor `--datasource-type NAME=TYPE` supplies;
- one below version 8 whose datasource cannot be reached for type refinement.

`--datasource-type NAME=TYPE`, repeatable, SHALL supply the dialect of a datasource the store holds no config for.

#### Scenario: An old store upgrades
- **WHEN** `slayer storage upgrade` runs on a store holding v15 datasets, a v15 memory and a datasource
- **THEN** every document is stored at the current version, loading the store afterwards emits no upgrade warning, every query returns what it returned before, and the command exits 0

#### Scenario: A dry run shows the change and writes nothing
- **WHEN** `slayer storage upgrade --dry-run` runs on the same store
- **THEN** it prints each document's difference, every stored document's content is unchanged, and the command exits 0

#### Scenario: A legacy cycle is upgraded, not refused
- **WHEN** the store holds a dataset written before the current version whose derived fields form a cycle that a client save would refuse
- **THEN** the upgrade stores it at the current version

#### Scenario: A dataset waiting for its dialect
- **WHEN** the store holds a v15 dataset with field `sql` `UPPER(status)` and no config for its datasource
- **THEN** the upgrade reports the dataset and leaves it untouched, upgrades every other document and exits 1; run again with `--datasource-type warehouse=duckdb`, it upgrades the dataset and exits 0

#### Scenario: A corrupt document does not stop the others
- **WHEN** one stored dataset is corrupt and the others are valid
- **THEN** the valid ones are upgraded, the corrupt one is reported and untouched, embeddings are preserved, and the command exits 1

#### Scenario: A concurrent edit is not overwritten
- **WHEN** a document is changed in the store after the upgrade read it and before it would write it
- **THEN** the upgrade reports a conflict for it, leaves the newer content in place and exits 1

#### Scenario: An interrupted settle converges
- **WHEN** an upgrade that renames a datasource's library-colliding granularity is interrupted after writing some dependent documents
- **THEN** a second run completes the rename and leaves one consistent store

### Requirement: Documents SLayer does not own upgrade without a store

A public entry point SHALL take one datasource's stored dataset documents, an optional datasource type and an optional datasource config, and return each document in the current format. It SHALL apply the same upgrade a load applies, open no store and write nothing. The result of each document SHALL be the same as `slayer storage upgrade` would store. A document the entry point cannot complete without a live datasource (below version 8) or without a dialect SHALL be reported with its identity and the reason, and SHALL not be returned upgraded.

#### Scenario: A v15 model set upgrades in memory
- **WHEN** two v15 dataset documents of one datasource, one with field `sql` `UPPER(status)`, are passed with the datasource type `duckdb`
- **THEN** both come back at the current version, every key is one the dataset model declares at its level, they validate to the same datasets a store would load, and nothing is written

#### Scenario: A pre-v8 document is refused
- **WHEN** a v5 dataset document with a refineable DOUBLE field is passed
- **THEN** it is reported as needing live type refinement through `slayer storage upgrade`

## MODIFIED Requirements

### Requirement: Live type refinement applies only to models written before refining ingest

On load, a stored model below schema version 8 SHALL have its DOUBLE and SQLite INT base columns refined against the live datasource. At version 8 and above, ingest already wrote refined types. A stored model at version 8 or above SHALL load without consulting the datasource, keeping its stored column types. Refinement SHALL read only the model's own table, through a client of the query engine that is loading the model. A load that needs refinement while the model's datasource has a stored entry but no query engine is loading it SHALL fail with an error stating that the model must be loaded through a query engine. On SQLite, refinement SHALL keep its sampling rules: a column is narrowed to INT only when the sample positively certifies integer storage, a saturated or failed sample leaves the stored type unchanged, and stored INT is widened only on evidence of non-integer storage.

#### Scenario: A v10 model loads with no datasource entry
- **WHEN** a raw v10 model with a DOUBLE base column is loaded and its datasource has no stored entry
- **THEN** it loads with the column still DOUBLE, the stored document is unchanged, and a later `slayer storage upgrade` persists it at the current version

#### Scenario: A v10 model loads with an unreachable datasource
- **WHEN** a raw v10 model with a DOUBLE base column is loaded and its datasource is a Postgres server that refuses connections
- **THEN** it loads, and no connection to the datasource is attempted

#### Scenario: A pre-v8 model still requires its datasource
- **WHEN** a raw v5 model with a refineable DOUBLE base column is loaded and its datasource has no stored entry
- **THEN** loading fails with the error stating that the datasource is unavailable for type refinement

#### Scenario: A v7 model is still refined
- **WHEN** a raw v7 model with a DOUBLE base column backed by a live integer column is loaded through a query engine with its datasource present
- **THEN** the column loads as INT

#### Scenario: Refinement reads only the model's table
- **WHEN** a raw v7 model is loaded through a query engine whose datasource holds several tables
- **THEN** exactly one table's columns are read

#### Scenario: Refinement without a query engine is refused
- **WHEN** a raw v5 model with a refineable DOUBLE base column is loaded directly from storage, with its datasource entry present
- **THEN** loading fails with the error stating that the model must be loaded through a query engine

#### Scenario: Refinement through a gateway
- **WHEN** a raw v7 model is loaded through a query engine whose client factory returns a gateway client, with every direct connection path failing
- **THEN** the column is refined from the gateway's metadata

#### Scenario: SQLite refinement keeps its sampling rules
- **WHEN** raw v7 SQLite models are loaded through a query engine with base columns holding integers only, mixed REAL values, BLOBs, coercible text, non-coercible text, non-finite text, more rows than the sample cap, and in an attached schema with a quoted identifier
- **THEN** only the integers-only column is narrowed, stored INT columns with non-integer evidence are widened, and the saturated column keeps its stored type

#### Scenario: The 0.10.2 corpus opens through storage and through an engine
- **WHEN** the committed 0.10.2 store is opened offline, both directly from storage and through a query engine
- **THEN** every model and memory loads both ways, without any connection being attempted

### Requirement: Stored query-backed models drop their cached backing SQL

A model SHALL NOT store the rendered SQL of its backing query. On load, a stored model below schema version 15 SHALL drop its `backing_query_sql` field. Inspect and REST SHALL NOT return backing SQL; a query-backed model's SQL SHALL be obtained by a `dry_run` query against it.

#### Scenario: A v14 query-backed model loads without backing SQL

- **WHEN** a stored v14 query-backed model holding `backing_query_sql` is loaded
- **THEN** it loads without the field, the stored document is unchanged, and a save or `slayer storage upgrade` persists it at the current version without it

#### Scenario: Inspect shows no backing SQL

- **WHEN** a query-backed model is inspected with `show_sql=True`
- **THEN** the output has no backing-SQL section, and a `dry_run` query on the model returns its SQL

### Requirement: Stored time dimensions are upgraded to expression text

On load, every time dimension of a stored model's `source_queries`, a saved query or a memory's bundled query written before this change, whose `dimension` (or `column`) is stored as a column-reference object `{"name": …, "model": …}`, SHALL be rewritten to the dotted text `model.name` (`name` when `model` is absent), recursively through nested stored queries, and persisted in that form only by a save or `slayer storage upgrade`; the query SHALL return the rows it returned before. A time dimension already stored as text, and a client query without a version, SHALL NOT be rewritten.

#### Scenario: Stored object form becomes text

- **WHEN** a stored query-backed model's source query holds the time dimension `{"dimension": {"name": "created_at", "model": "customers"}, "granularity": "month"}`
- **THEN** it loads with `field` = `customers.created_at`, dumps it as that string, and returns the same rows as before

#### Scenario: Memory-bundled query

- **WHEN** a memory stored as YAML frontmatter or as a SQLite row bundles a query whose time dimension is stored in the object form
- **THEN** it loads with the dotted text and the bundled query returns the same rows

#### Scenario: The released corpus still opens

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened
- **THEN** every stored time dimension loads as text and every stored query returns its release's results

### Requirement: Stored granularities colliding with a library name are renamed

On every load, whatever the document's version, a stored datasource custom granularity whose name case-insensitively matches any reserved library name SHALL be renamed to `<name>_granularity`, or `<name>_granularity_<n>` with the smallest `n` ≥ 2 that is free, and every reference to it in that datasource's stored models, saved queries, query-backed models and memories SHALL be rewritten to the new name in every position a granularity name is accepted. Each rename SHALL be logged as a warning naming the datasource and both names. The rename SHALL be a function of the stored datasource alone and SHALL be applied in memory on every load, which writes nothing. `slayer storage upgrade` SHALL persist the dependent documents before the datasource, so an upgrade interrupted at any point completes on its next run. This SHALL hold on both the YAML and the SQLite store. A client saving a custom granularity with a reserved name SHALL still be refused.

#### Scenario: Granularity named left

- **WHEN** a stored datasource defines the granularity `left` and a stored model's query-backed source and a memory's query both bucket `created_at` by it
- **THEN** after load the granularity is `left_granularity`, both documents reference `left_granularity`, a warning names the rename, both queries return the rows they returned before, and no stored document changes

#### Scenario: Granularity named collect

- **WHEN** a stored datasource defines the granularity `collect`
- **THEN** it loads as `collect_granularity`

#### Scenario: The new name is taken

- **WHEN** a stored datasource defines the granularities `left` and `left_granularity`
- **THEN** `left` is renamed to `left_granularity_2` and references to `left_granularity` are untouched

#### Scenario: An interrupted load completes

- **WHEN** `slayer storage upgrade` is interrupted after some dependent documents were persisted with the new name and before the datasource was
- **THEN** a load in between reads every document with the new name, and the next upgrade renames the datasource's granularity to the same name and persists the remaining documents, leaving one consistent store

#### Scenario: A rename converges

- **WHEN** a store whose granularity `contains` was renamed by `slayer storage upgrade` is loaded and upgraded again, on the YAML and on the SQLite store
- **THEN** no document changes and no rename warning is logged

#### Scenario: A granularity colliding with a later library addition

- **WHEN** a datasource stored at the current version defines the granularity `left`, which a later library release reserves, and a stored query buckets by `left(created_at)`
- **THEN** on load the granularity is `left_granularity`, the query reads `left_granularity(created_at)`, and it returns the rows it returned before

#### Scenario: A new reserved-name granularity is refused

- **WHEN** a client saves a datasource defining the granularity `left`
- **THEN** the save fails naming the reserved name

### Requirement: Stored extension columns are inlined

On load, every stored query written before this change — in a stored query-backed model's `source_queries`, a saved query or a memory's query, recursively through nested stages — whose extensions carry `columns` SHALL be converted so that no extension carries `columns`, with the result keys it returned before, except that a column reached through a join keeps its name as a stage output column while its top-level result key takes the flattened spelling. The conversion SHALL bind the query against the models of its datasource, as the engine resolves them, with the extension columns present, and SHALL replace every reference that resolves to an extension column by that column's expression, qualified by the canonical path the reference resolved through:
- a dimension SHALL become a computed dimension named after the column, and a time dimension an expression time dimension named after it, the column's join path flattened with `__` when it is reached through a join (`customers__tier_label`);
- a measure reading the column SHALL carry an explicit name equal to the name it was generated before, its join path flattened with `__`;
- filters, order items, partition and ranking keys, `main_time_dimension`, extension measures and other extension columns SHALL take the expression in place, extension columns expanding in dependency order;
- an expression is the column's `sql` read as Mode B, wrapped as `iif(<filter>, <sql>, NULL)` when the column has a `filter`, and as `CAST(<expression> AS <type>)` when it declares a type other than `TEXT` or `UNKNOWN`;
- a column without `sql` SHALL become a reference to the extended model's column of its name;
- a column's `label` and `format` SHALL carry over where the converted position holds them.

Each converted column SHALL emit one structured warning naming the document and the column, stating that its text is now read as Mode B. The converted query SHALL return the rows it returned before, except where a construct's Mode-B meaning differs from the dialect's native one (`queries/string-matching`, `queries/regex`, `queries/mixed-type-coercion`).

A stored query SHALL be unconvertible, and the document holding it SHALL fail to load with a typed error naming the document and the construct, through the stored-document load boundary, and stay unchanged in storage, when it holds:
- an inline `source_model`;
- an extension column whose text is not valid Mode B;
- an extension column of type `ARRAY`;
- an extension column without `sql` whose name is not a visible column of the extended model;
- an extension column without `sql` that has a `filter`;
- an extension column with `sql` named like a column of the extended model;
- a dependency cycle among extension columns;
- an extension join whose `join_pairs` names an extension column on either side;
- a reference to an extension column in a position that holds no expression, or one the binder cannot attribute.

A converted document SHALL be converted on every load, which writes nothing; a save or `slayer storage upgrade` SHALL persist a converted query-backed model, saved query or memory at the current version.

#### Scenario: A dimension keeps its result key
- **WHEN** a stored query-backed model's stage rooted at `orders` extends `customers` with the column `tier_label` (`sql` `upper(tier)`) and selects the dimension `customers.tier_label` and `sum(amount)`
- **THEN** it loads with the computed dimension `upper(customers.tier)` named `customers__tier_label`, no extension columns, one warning naming the model and `tier_label`, and returns the rows recorded before under the stage column `customers__tier_label`

#### Scenario: A measure keeps its generated name
- **WHEN** a stored stage extends `orders` with `double_amount` (`sql` `amount * 2`, type `DOUBLE`) and selects `sum(double_amount)`
- **THEN** it loads as `sum(CAST(amount * 2 AS DOUBLE))` named `double_amount_sum` and returns the recorded value

#### Scenario: A reference through a named edge is qualified by the edge
- **WHEN** a stored stage rooted at `orders` extends `addresses` with `city_label` and selects `billing_address.city_label`
- **THEN** the expression is qualified as `billing_address.<…>` and returns the recorded rows

#### Scenario: Filtered, chained and sql-less columns
- **WHEN** a stored stage's extension has `q_amount` (`sql` `amount`, `filter` `status = 'ok'`), `q_double` (`sql` `q_amount * 2`) and a sql-less `status`, each used in a filter, an order item and a measure
- **THEN** each use takes `iif(status = 'ok', amount, NULL)`, `iif(status = 'ok', amount, NULL) * 2` and `status` respectively, and returns the recorded rows

#### Scenario: Unconvertible documents fail to load and stay untouched
- **WHEN** stored documents hold, in turn, an inline `source_model`, a column `sql` `strftime('%Y', created_at)`, a sql-less column naming a column the model does not declare, a join keyed on an extension column, and two columns referencing each other
- **THEN** each fails to load with the typed error naming the document and the construct, every other document of the store loads, and the stored bytes are unchanged

#### Scenario: A memory converts on every load
- **WHEN** a stored memory's query carries an extension column and the memory is loaded twice
- **THEN** both loads return the converted query with the warning, and the stored memory is unchanged

#### Scenario: Released stores still open with their results
- **WHEN** the committed 0.10.2 and 1.8.2 stores are opened
- **THEN** every model and memory loads and every recorded query returns the rows recorded from its release

### Requirement: Stored field sql converts to its expression

A stored dataset written before this version SHALL load with each field's `sql` converted to its `expression` by the rules of `models/field-expressions` › A field's sql converts to its expression, with no deprecation warning, and every query SHALL return the values it returned before. The conversion SHALL run in two phases: a context-free version step that turns a bare or double-quoted identifier into its shorthand (into its path when it reads a sibling field) and leaves any other `sql` in place, then a load step with the store's datasource and its datasets that writes each sibling reference as its path and tags the text with the datasource's own dialect. A dataset whose store holds no config for its datasource SHALL load with that `sql` unconverted (`models/field-expressions` › A field's sql waits for its dialect) and SHALL be converted on a later load that has it. A converted dataset SHALL be persisted as every other upgraded document is, by a save or `slayer storage upgrade`; `slayer storage upgrade` SHALL leave a dataset with unconverted `sql` untouched and report it.

#### Scenario: A stored dataset of every shape loads
- **WHEN** a store holding a DuckDB datasource config and a dataset `orders` written with fields `amount` (`sql: amount`), `label` (`sql: '"Label"'`), `doubled` (`sql: "amount * 2"`) and `ratio` (`sql: "doubled / qty"`) is opened
- **THEN** the fields load as `expression: amount`, `expression: '"Label"'`, `{dialects: [{dialect: DUCKDB, expression: "amount * 2"}]}` and `{dialects: [{dialect: DUCKDB, expression: "orders.doubled / qty"}]}`, no `FutureWarning` is emitted, the stored document is unchanged until `slayer storage upgrade` persists it at the current version, and every field returns its former values

#### Scenario: A stored dataset without its datasource config
- **WHEN** the same dataset is opened from a store that holds no datasource config
- **THEN** `amount` and `label` load converted, `doubled` and `ratio` keep their `sql`, `slayer storage upgrade` reports the dataset and leaves it untouched, and after the datasource config is added the upgrade converts and persists it

#### Scenario: Direct validation of a stored document
- **WHEN** a stored dataset document of the previous version is validated directly, with no store
- **THEN** its bare-identifier fields carry the shorthand and its other fields keep `sql`

### Requirement: Stored queries are upgraded to qualified references
A stored query written before this change — a saved query, a query-backed model's `source_queries` stage, or a memory's query, nested or not, with or without a version of its own — SHALL load in the qualified syntax of `queries/references`, with no deprecation warning, by a conversion that reads the stored datasets:
- each reference SHALL be rewritten to the qualified spelling that has the meaning it had before this change, reading the stage's own population;
- a bare name SHALL read what it read before this change, in this order: inside a metric, dimension or time-dimension entry, or the main time dimension, the population's field, then the population's metric, then an alias of the stage other than the entry's own; in a filter or order, the population's field, then an alias of the stage, then the population's metric; a population that is a stage or a query-backed dataset SHALL offer its output columns as fields;
- a reference the new routing would send along a different path SHALL be rewritten to the explicit route from the population that keeps its old path;
- a reference whose anchor was reached by a route longer than its written path SHALL be rewritten to its explicit route from the population, so its column name is kept;
- a rewritten path SHALL be kept only if it resolves back to the same oriented sequence of relationships;
- an unaliased entry whose references share no anchor SHALL get an alias equal to its column name before this change, so every later stage, query-backed model consumer and `DatasetField.sql` reading that column keeps working;
- `count(*)` SHALL become `count(<population>.*)`.
- a double-quoted segment SHALL match only its exact spelling, and a rewritten reference SHALL keep each double-quoted segment of its leaf double-quoted.

A stored query that cannot be converted this way (a dataset it reads is missing, or an old path has no unique explicit spelling) SHALL fail to load through the existing unloadable-document boundary, untouched on disk, with a warning naming the document and the reference. Every stored query SHALL convert on each load, which writes nothing, and SHALL be persisted only by a save or `slayer storage upgrade`, as other migrated documents are.

#### Scenario: A saved query loads qualified with its results
- **WHEN** a store holds a saved query with `population: orders`, the dimension `status`, the metric `sum(amount)` and the filter `customers.region = 'EAST'`
- **THEN** it loads as `orders.status`, `sum(orders.amount)` and `customers.region = 'EAST'`, with no `FutureWarning`, and returns the rows it returned before

#### Scenario: A stored cycle reference keeps its path
- **WHEN** a stored query with `population: x` over the cycle `x → y → z → x` measures `sum(z.amount)`
- **THEN** it loads as `sum(x.z.amount)` and returns the value it returned before

#### Scenario: A stored stage chain keeps its column names
- **WHEN** a stored query-backed model's first stage measures `sum(amount) / count(customers.*)` unaliased and its second stage reads that column by its old name
- **THEN** the first stage's entry gets that name as its alias and the model returns the rows it returned before

#### Scenario: An unspellable old path makes the document unloadable
- **WHEN** a stored query's reference reads a dataset that its population reaches only across two unnamed parallel relationships, so its old path has no explicit spelling
- **THEN** the document fails to load, stays untouched on disk, and the warning names the document and the reference

#### Scenario: A memory's query converts on every load
- **WHEN** a stored memory holds a query with `population: orders` measuring `count(*)`
- **THEN** each load reads it as `count(orders.*)`, with no `FutureWarning`, and the stored memory is not rewritten

#### Scenario: A quoted leaf keeps its exact spelling
- **WHEN** a stored query on `orders` filters `"Regions"."Opened_At" >= '2024-01-01'`, `Regions` reached by the route `orders → customers → Regions`
- **THEN** it loads with the leaf still spelt `"Opened_At"` on its explicit route

#### Scenario: A metric alias equal to a population column reads the column
- **WHEN** a stored query-backed model's stage over the query-backed model `rev_f`, which outputs the metric column `rev`, measures `sum(rev)` aliased `rev` and filters `rev >= 0`
- **THEN** it loads as `sum(rev_f.rev)` aliased `rev` with the filter `rev_f.rev >= 0`, and returns the sum of `rev_f.rev` over the `rev_f` rows with `rev >= 0`

#### Scenario: A metric reads the population metric a sibling alias shares
- **WHEN** a stored query on `orders`, whose metric `revenue` is `sum(amount)`, has the dimension `status`, measures `count(*)` aliased `revenue` and `revenue * 10` aliased `r10`, and filters `revenue > 1`
- **THEN** it loads as `count(orders.*)` aliased `revenue`, `orders.revenue * 10` aliased `r10` and the filter `revenue > 1`, and returns, for each `status` with more than one order, `r10` equal to ten times its sum of `amount`

#### Scenario: A stage over an earlier stage reads the earlier stage's column
- **WHEN** a stored query-backed model's first stage `s1` outputs `rev`, and its second stage over `s1` measures `sum(rev)` aliased `rev` and filters `rev > 25`
- **THEN** the second stage loads as `sum(s1.rev)` aliased `rev` with the filter `s1.rev > 25`, and the model returns the sum of the `s1` rows with `rev > 25`

#### Scenario: A partition key naming a computed dimension stays bare
- **WHEN** a stored query on `orders` has the dimension `status` and the computed dimension `upper(status)` named `su`, and measures `sum(amount, partition_by=su)`
- **THEN** it loads with `partition_by=su` unchanged and returns the rows it returned before

#### Scenario: A quoted alias matches only its exact spelling
- **WHEN** a stored query on `orders`, which has the field `amount`, has the dimension `status`, measures `count(*)` aliased `"Amount"` and filters `"Amount" > 1`
- **THEN** the filter stays `"Amount" > 1`, reading the metric, and returns each `status` with more than one order
