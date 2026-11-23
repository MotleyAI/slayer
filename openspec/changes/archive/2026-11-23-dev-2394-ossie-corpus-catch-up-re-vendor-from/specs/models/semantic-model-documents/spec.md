## MODIFIED Requirements

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

### Requirement: Entities SLayer cannot evaluate yet still load
A field or metric of a valid document whose expression SLayer cannot evaluate yet SHALL load with a structured load warning naming the entity and the reason, visible on `inspect`, `datasets_summary` and search results. These are: a window function; a model-scoped metric reading a field unqualified or outside an aggregate; a dataset-scoped metric that evaluates a measure of its dataset's source with `AGG(source.measure)` (SLayer's sources expose no measure columns, and Ossie requires a consumer to reject `AGG` on a column that is not a measure and never to replace it with an ordinary aggregate); and any model-scoped metric that reaches such an entity, directly through `AGG(d.m)` or transitively through other model-scoped metrics, whose warning SHALL name the dependency chain. Every query reading the entity SHALL fail with a typed error naming it and the reason; no other entity SHALL be affected. A save SLayer authors that adds or changes a definition to one SLayer cannot evaluate SHALL be refused with that typed error; an unchanged entity that loaded with a warning SHALL survive saves of other parts of its document.

#### Scenario: A windowed field loads and fails only when read
- **WHEN** a document declares a field `running_total` with expression `SUM(amount) OVER (ORDER BY order_date)` beside a field `amount`
- **THEN** the document loads, `inspect` shows the warning on `running_total`, a query of `sum(sales.amount)` succeeds, and a query reading `sales.running_total` fails with a typed error naming `running_total`

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

### Requirement: The Ossie corpus loads, compiles and round-trips
A corpus vendored with provenance pinning the upstream apache/ossie commit SHALL consist of every semantic-model document of upstream `examples/`, `compliance/portability/` (m0–m8), the `compliance/wide_table/` models and `compliance/query/scott`, plus every document upstream's `validation/tests` assert as valid, harvested into fixture files; it SHALL also hold upstream's validator, schema, loader, the core-spec text (`spec.md`, `spec.yaml`, `expression_language.md`, `relational_semantics.md`, `wide_table_semantics.md`, `spec_layers.md`) and the expression fixtures `compliance/expressions/valid.txt` and `invalid.txt`. Re-vendoring SHALL be one command over an apache/ossie checkout that copies every file byte for byte, records each file's upstream blob sha and the commit, removes only files the previous provenance listed and upstream no longer has, and leaves the previous corpus intact when any step fails. For every corpus document: upstream `validation/validate.py` SHALL pass; the native load SHALL succeed; every dataset-scoped and model-scoped metric SHALL bind and compile to SQL, except entities SLayer cannot evaluate yet, which SHALL raise their typed error; storing, loading, saving unchanged and exporting a single-document datasource SHALL each reproduce the document's text byte for byte.

#### Scenario: Every corpus document round-trips
- **WHEN** each corpus document is imported into its own empty datasource and exported
- **THEN** the export equals the original text byte for byte

#### Scenario: The source-measure document loads and round-trips
- **WHEN** upstream's harvested document with `orders.revenue_total = AGG(source.revenue)`, `orders.revenue_per_order = AGG(source.revenue) / COUNT(DISTINCT order_id)` and `revenue_per_customer = AGG(orders.revenue_total) / COUNT(DISTINCT customers.id)` is stored, loaded, saved unchanged and exported
- **THEN** the native check accepts it, all three metrics raise the typed error for entities SLayer cannot evaluate yet, and each step reproduces its text byte for byte

#### Scenario: A failed re-vendor keeps the corpus
- **WHEN** the re-vendor command runs over a checkout whose validator tests fail during the harvest
- **THEN** the command exits non-zero and every vendored file and the provenance file are unchanged

## ADDED Requirements

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
