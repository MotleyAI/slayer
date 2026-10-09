## Purpose

Lets a host application show each caller only the semantic models their access tags allow. A store wrapped with the caller's tags reads as the store with every model the caller cannot access deleted, so every surface that reads models is filtered without receiving caller context.

## ADDED Requirements

### Requirement: Models carry access tags

A model SHALL carry `access_tags`, a list of strings that defaults to empty. An empty list means the model is public. The tags SHALL round-trip unchanged through model JSON, storage, git sync, import and export, REST, the Python client, and MCP `create_model` / `edit_model`. SLayer SHALL accept any string as a tag. A stored model without the field SHALL load as untagged, and models SHALL be written at schema version 15. Access tags are independent of the `hidden` flag: `hidden` curates the catalog for everyone and keeps the model queryable.

#### Scenario: Tags round-trip through MCP and storage

- **WHEN** `create_model` is called with `access_tags=["hr"]` and the model is read back through storage, REST and inspect
- **THEN** each returns `access_tags == ["hr"]`

#### Scenario: A pre-v15 model loads untagged

- **WHEN** a stored v14 model is loaded
- **THEN** it has `access_tags == []` and is written back at version 15

#### Scenario: A non-conforming tag is kept

- **WHEN** a model with the tag `"HR Team"` is saved and loaded
- **THEN** it keeps the tag `"HR Team"`

### Requirement: A filtered store shows only the models the caller can see

A store wrapped with a caller's tags and a `bypass` flag (both required, with no defaults) SHALL treat a model as accessible iff the model is untagged, or its tags intersect the caller's tags, or the caller has `bypass`. For a non-bypass caller, a model SHALL be visible iff it is accessible, it loads, and every model it reads is visible, transitively. A model that cannot be loaded SHALL be invisible to a non-bypass caller, since its tags cannot be confirmed.

#### Scenario: Tag intersection grants access

- **WHEN** a caller with tags `{fin}` lists models in a store holding an untagged `pub`, `hr` tagged `{hr}` and `fin` tagged `{fin}`
- **THEN** the listing contains `pub` and `fin` and not `hr`

#### Scenario: A caller with no tags sees only untagged models

- **WHEN** a caller with no tags and no bypass lists the same store
- **THEN** the listing contains only `pub`

#### Scenario: An unloadable model is invisible

- **WHEN** a non-bypass caller lists a datasource holding an untagged model that cannot be loaded
- **THEN** the model is absent, and the listing emits no warning naming it

#### Scenario: Constructing a filtered store without access fails

- **WHEN** a filtered store is constructed without `tags` or without `bypass`
- **THEN** construction fails

### Requirement: A model that reads an invisible model is invisible

A model SHALL read every model that one of the following references, and every model on the path of such a reference:

- the source of each stage of a query-backed model (a model name, an extension's base, or an inline model);
- the targets of extension and inline joins;
- every dotted reference path in its stages' fields: dimensions, time dimensions, measures, filters and order;
- every dotted reference path in its own definitions: column SQL, column filters, model filters, saved measure formulas (including saved measures they reference), and aggregation parameters.

Paths SHALL be resolved against the full store. A path that does not resolve even in the full store SHALL add no read. Declaring a join SHALL NOT by itself count as reading its target.

#### Scenario: A query-backed model on a hidden model is hidden

- **WHEN** a `{fin}` caller lists a store holding a query-backed model whose stage reads `hr`
- **THEN** that model is absent

#### Scenario: Dependents are hidden transitively

- **WHEN** a query-backed model reads a query-backed model that reads `hr`
- **THEN** both are absent for a `{fin}` caller

#### Scenario: A stage that only hops through a hidden model hides its model

- **WHEN** a query-backed model's stage source is `orders` and its only reference to `hr` is a dimension `orders.hr.dept`
- **THEN** the model is absent for a `{fin}` caller

#### Scenario: A column reading through a hidden join hides its model

- **WHEN** an untagged model has a column, a saved measure, or a model filter that reads `hr` through a join
- **THEN** the model is absent for a `{fin}` caller

#### Scenario: A stage order key alone hides its model

- **WHEN** a query-backed model's only reference to `hr` is in a stage's `order`
- **THEN** the model is absent for a `{fin}` caller

#### Scenario: A declared but unused join does not hide

- **WHEN** an untagged model declares a join to `hr` and nothing in it reads through that join
- **THEN** the model is visible to a `{fin}` caller

### Requirement: An invisible model is absent from every read surface

For a non-bypass caller, an invisible model SHALL be absent from model listings, model lookups by qualified or bare name, bare-name ambiguity candidates, inspect (including join targets and join hops of other models), models summaries, keyword and embedding search (results and indexed text), "did you mean" suggestions, population and root inference, root-model recommendation, the SQL facade catalog, and model validation. A query that uses an invisible model as its root, a join target, or a join hop SHALL fail with the same error as a query on a model that does not exist.

#### Scenario: Querying through a hidden model fails like a missing one

- **WHEN** a `{fin}` caller queries `hr` as the root, as a join target, or as a hop of a dotted path
- **THEN** the error equals the error for the same query in a store where `hr` was deleted

#### Scenario: Bare-name ambiguity does not reveal a hidden model

- **WHEN** a bare model name exists in two datasources and the copy in one of them is invisible to the caller
- **THEN** the name resolves to the visible copy with no ambiguity error

#### Scenario: Search does not reveal a hidden model

- **WHEN** a `{fin}` caller searches with keyword and embedding retrieval for `hr`'s name and description
- **THEN** no result is `hr`, and no result text names it

### Requirement: Joins into invisible models are pruned

For a non-bypass caller, each model the filtered store returns SHALL omit every join whose target exists in the full store but is invisible to the caller. A join whose target does not exist in the full store SHALL be returned unchanged.

#### Scenario: A join to a hidden model is omitted

- **WHEN** a `{fin}` caller inspects `fin`, which declares a join to `hr`
- **THEN** neither its join targets, nor its join hops, nor its models-summary entry, nor its search text names `hr`
- **AND** model validation reports no drift for that join

#### Scenario: A dangling join is kept

- **WHEN** a `{fin}` caller inspects a model that declares a join to a model that does not exist anywhere
- **THEN** the join is shown, as it is for a bypass caller

### Requirement: Memories follow the models they link to

A memory's linked models SHALL be the models named by its `<datasource>.<model>[.<leaf>]` entities plus the models its example query reads. Datasource-only entities and `memory:` references SHALL link no model. For a non-bypass caller, a memory SHALL be hidden iff it links at least one model and every linked model exists in the full store but is invisible. A visible memory SHALL be returned without entities naming invisible models (or their columns and measures) or hidden memories, and without its example query when that query reads an invisible model. Query-resolution warnings SHALL NOT fire for a query withheld this way.

#### Scenario: A memory linked only to a hidden model is hidden

- **WHEN** a `{fin}` caller lists memories and one memory's entities are all `hr` entities
- **THEN** that memory is absent, and fetching it by id fails as not found

#### Scenario: A mixed memory is visible without the hidden parts

- **WHEN** a memory links `hr` and `fin`, and its example query reads `hr`
- **THEN** a `{fin}` caller sees the memory with only its `fin` entities and with no example query

#### Scenario: Unlinked memories stay visible

- **WHEN** a caller with no tags lists memories
- **THEN** help memories and memories whose entities are only datasources or `memory:` references are listed

### Requirement: Embeddings of invisible models and memories are hidden

For a non-bypass caller, every embedding read SHALL omit rows whose id is an invisible model, a column or measure of one, or a hidden memory.

#### Scenario: Embedding search skips a hidden model's columns

- **WHEN** a `{fin}` caller reads the embedding corpus
- **THEN** no row belongs to `hr` or to any of its columns

### Requirement: Non-bypass writes merge onto the full documents

A non-bypass caller's write SHALL be applied to the full stored document, keeping every part the caller cannot see:

- Saving a model SHALL keep its pruned joins.
- Saving a memory SHALL keep its withheld entities and example query. This includes every internal rewrite of a memory, such as ingestion cleanup and deletion cascades.
- A save that is valid in the caller's view but invalid once merged SHALL fail with an error stating that the change conflicts with parts of the model (or memory) the caller cannot access. The error SHALL name nothing the caller cannot see. Such conflicts include a new model or memory whose name or id belongs to a hidden one, an edge name already used by a pruned join, removing or retyping a column a pruned join uses as a key, and moving a model that has pruned joins to another datasource.
- A join whose target is absent from the caller's view SHALL fail as a join to an unknown model.
- Deleting or updating a model or memory invisible to the caller SHALL behave as if it did not exist. Deleting a visible model or datasource SHALL cascade over the full store.
- Embedding writes for hidden ids SHALL be refused.

#### Scenario: Editing a model keeps its hidden join

- **WHEN** a `{fin}` caller adds a column to `fin`, which has a join to `hr`
- **THEN** the stored `fin` has the new column and still has the join to `hr`

#### Scenario: A conflicting edit is refused without naming hidden parts

- **WHEN** a `{fin}` caller removes the column that `fin`'s join to `hr` uses as its key
- **THEN** the save fails with the conflict error, whose text does not contain `hr`

#### Scenario: Deleting a hidden memory looks like a missing one

- **WHEN** a `{fin}` caller deletes a memory that is hidden from them
- **THEN** the call fails as not found, and the memory is still stored

### Requirement: Only bypass callers change access tags

A non-bypass caller's save SHALL fail with an access-tags error when it changes a stored model's `access_tags` or creates a model with non-empty `access_tags`.

#### Scenario: A non-bypass caller cannot retag a model

- **WHEN** a `{fin}` caller saves `fin` with `access_tags=["fin", "hr"]`
- **THEN** the save fails with the access-tags error, and the stored tags are unchanged

### Requirement: Bypass callers see and write the full store

A store wrapped with `bypass=True` SHALL read and write exactly as the store it wraps.

#### Scenario: Bypass sees everything

- **WHEN** a bypass caller lists models, memories and embeddings, and inspects `fin`
- **THEN** every result equals the unwrapped store's, including `fin`'s join to `hr`

### Requirement: Narrowed access is reported

A model's access is narrowed when it reads, transitively, a tagged model whose tags do not include all of its own (an untagged model reading a tagged one is narrowed). Saving a narrowed model SHALL emit a structured `narrowed_access` warning naming the models that narrow it. Saving a tagged model SHALL emit the warning for each model that becomes narrowed because it reads it. Model validation SHALL report every narrowed model. A narrowed model SHALL still save.

#### Scenario: Saving a public model that reads a tagged one warns

- **WHEN** an untagged query-backed model reading `hr` is saved
- **THEN** the save succeeds and emits a `narrowed_access` warning naming `hr`

#### Scenario: Retagging a model warns about its readers

- **WHEN** `orders` is read by an untagged model and `orders` is saved with `access_tags=["ops"]`
- **THEN** the save emits a `narrowed_access` warning naming the reader

#### Scenario: Model validation lists narrowed models

- **WHEN** model validation runs over a datasource holding a narrowed model
- **THEN** the report lists it as narrowed, naming the models that narrow it

### Requirement: Cached views are never shared across callers

Any cache derived from stored models, memories or embeddings SHALL be keyed by the store's view, so that stores wrapped with different tag sets over one backend never share a cached entry. A bypass store SHALL share the wrapped store's cache entries. A store that cannot report a content fingerprint SHALL never serve a cached entry.

#### Scenario: Two tag sets never share the search graph

- **WHEN** a `{fin}` caller and an `{hr}` caller search through stores wrapping one backend, alternately and repeatedly
- **THEN** each sees only their own models, and the number of cached graphs does not grow beyond one per tag set

#### Scenario: A store without a fingerprint rebuilds

- **WHEN** a store that reports no content fingerprint changes between two searches
- **THEN** the second search sees the change
