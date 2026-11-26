# Ontologies and Mappings

SLayer imports, checks, stores and exports [Ossie](https://github.com/apache/ossie) Layer 4 documents: **ontologies**, which declare concepts, their relationships, identifiers and rules, and **mappings**, which populate an ontology's concepts from one semantic model's datasets. SLayer does not use them yet: queries, `inspect` of datasets and `search` ignore them.

## What SLayer does with them

- **Import and export.** `slayer import-osi` recognises an ontology by its root `ontology` key and a mapping by its root `concept_mappings`, `ontology_ref` and `semantic_model_ref` keys; `slayer export-osi --ontology` / `--mapping` writes one back ([Importing](../osi/osi_import.md), [Exporting](../osi/osi_export.md)).
- **Storage.** Ontologies are stored once per store, by name; mappings per datasource, by name. A document's name is the `name` it declares, so saving it under another name is refused. Each is stored as the text it was saved with and exports byte-identical, comments, `prefixes`, `iri` values and `annotations` included. Deleting a datasource deletes its mappings.
- **Checks.** Every save is refused, with every problem listed, when the document fails the upstream schema, any check upstream Ossie's validator makes, or SLayer's own rules below.
- **Surfaces.** Ontologies and mappings are listed, read, created, replaced and deleted through the Python storage API and client, REST, the CLI (`slayer ontologies …`, `slayer ontology-mappings …`) and MCP, where `inspect` shows them as the entity types `ontology` and `ontology_mapping`.

## Ontology checks

Besides the upstream schema and validator, an ontology is refused when a concept is declared twice, a relationship name repeats within its concept, two roles of one relationship share an effective name (the first role is named after its concept, each further role by its `name`, else its concept), the `extends` graph has a cycle, a value type extends an entity type or reaches no built-in (`Any`, `Boolean`, `Date`, `DateTime`, `Decimal`, `Float`, `Integer`, `String`), an entity type extends a value type or a built-in, a QName `iri` uses an undeclared prefix, or an `identify_by` entry is not a binary relationship of the concept or a concept it extends. Names are compared as Ossie compares identifiers.

`requires` and `derived_by` rules are stored and exported as written; SLayer does not interpret them.

## Mapping resolution

A mapping belongs to one datasource and must resolve when saved:

- `ontology_ref.name` names a stored ontology; `semantic_model_ref.name` names a semantic model of the datasource, else the datasource itself (every dataset it holds). A reference's `iri` is kept and never fetched.
- Every concept and relationship it names is declared, through `extends`, at the position it is used; a link-mapping node at depth *k* names a relationship of arity *k*; an object mapping uses `expression` only for a value type or an entity with a simple identifier, and its `referent_mappings` name each identifying relationship exactly once.
- Every `expression` is a row-level SQL expression over `dataset.field` references of the referenced semantic model, with no aggregate or window. SLayer reads `<value> WHERE ( <predicate> )`, which the upstream examples use but the Ossie spec does not yet define, as the value kept only for the rows where the boolean predicate holds.
- An expression's type must fit the built-in its concept reaches: any numeric type for `Integer`, `Decimal` and `Float`; a date or timestamp for `DateTime`; the same type otherwise; anything for `Any`. An expression of unknown type (an untyped field) is accepted and listed in one note per save.

## Stored mappings stay valid

An explicit change that would break a stored mapping is refused, naming the mapping and each problem: saving, editing or deleting an ontology, a semantic model or a dataset, and removing a field. Deleting a mapping is always allowed. A change that follows the database (ingest, applying schema drift) applies and warns instead; the mapping is then listed and inspected as unresolved, with its problems, until it is saved again.

With [row-level security](row-level-security.md), a caller reads a mapping without the concept mappings that read a dataset hidden from them, and a save by that caller keeps those entries. Ontologies are visible to every caller.

SLayer makes no Layer 4 compliance claim: Ossie defines no Layer 4 compliance suite yet.
