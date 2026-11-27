# Ontologies and Mappings

SLayer imports, checks, stores and exports [Ossie](https://github.com/apache/ossie) Layer 4 documents: **ontologies**, which declare concepts, their relationships, identifiers and rules, and **mappings**, which populate an ontology's concepts from one semantic model's datasets. Agents see the concepts in `inspect` and `search`, and `validate_datasets` checks the semantic model and its data against the ontology. Queries do not read them.

## What SLayer does with them

- **Import and export.** `slayer import-osi` recognises an ontology by its root `ontology` key and a mapping by its root `concept_mappings`, `ontology_ref` and `semantic_model_ref` keys; `slayer export-osi --ontology` / `--mapping` writes one back ([Importing](../osi/osi_import.md), [Exporting](../osi/osi_export.md)).
- **Storage.** Ontologies are stored once per store, by name; mappings per datasource, by name. A document's name is the `name` it declares, so saving it under another name is refused. Each is stored as the text it was saved with and exports byte-identical, comments, `prefixes`, `iri` values and `annotations` included. Deleting a datasource deletes its mappings.
- **Checks.** Every save is refused, with every problem listed, when the document fails the upstream schema, any check upstream Ossie's validator makes, or SLayer's own rules below.
- **Surfaces.** Ontologies and mappings are listed, read, created, replaced and deleted through the Python storage API and client, REST, the CLI (`slayer ontologies …`, `slayer ontology-mappings …`) and MCP, where `inspect` shows them as the entity types `ontology` and `ontology_mapping`. `inspect` of an ontology returns its concept graph (compact), plus its stored text (full).

## Ontology checks

Besides the upstream schema and validator, an ontology is refused when a concept is declared twice, a relationship name repeats within its concept, two roles of one relationship share an effective name (the first role is named after its concept, each further role by its `name`, else its concept), the `extends` graph has a cycle, a value type extends an entity type or reaches no built-in (`Any`, `Boolean`, `Date`, `DateTime`, `Decimal`, `Float`, `Integer`, `String`), an entity type extends a value type or a built-in, a QName `iri` uses an undeclared prefix, or an `identify_by` entry is not a binary relationship of the concept or a concept it extends. Names are compared as Ossie compares identifiers.

`requires` and `derived_by` rules are stored and exported as written. SLayer checks the forms described in [Checks against the data](#checks-against-the-data) and lists every other `requires` as not checked; it does not interpret `derived_by`.

## Mapping resolution

A mapping belongs to one datasource and must resolve when saved:

- `ontology_ref.name` names a stored ontology; `semantic_model_ref.name` names a semantic model of the datasource, else the datasource itself (every dataset it holds). A reference's `iri` is kept and never fetched.
- Every concept and relationship it names is declared, through `extends`, at the position it is used; a link-mapping node at depth *k* names a relationship of arity *k*; an object mapping uses `expression` only for a value type or an entity with a simple identifier, and its `referent_mappings` name each identifying relationship exactly once.
- Every `expression` is a row-level SQL expression over `dataset.field` references of the referenced semantic model, with no aggregate or window. SLayer reads `<value> WHERE ( <predicate> )`, which the upstream examples use but the Ossie spec does not yet define, as the value kept only for the rows where the boolean predicate holds.
- An expression's type must fit the built-in its concept reaches: any numeric type for `Integer`, `Decimal` and `Float`; a date or timestamp for `DateTime`; the same type otherwise; anything for `Any`. An expression of unknown type (an untyped field) is accepted and listed in one note per save.

## Stored mappings stay valid

An explicit change that would break a stored mapping is refused, naming the mapping and each problem: saving, editing or deleting an ontology, a semantic model or a dataset, and removing a field. Deleting a mapping is always allowed. A change that follows the database (ingest, applying schema drift) applies and warns instead; the mapping is then listed and inspected as unresolved, with its problems, until it is saved again.

With [row-level security](row-level-security.md), a caller reads a mapping without the concept mappings that read a dataset hidden from them, and a save by that caller keeps those entries. Ontologies are visible to every caller.

## Concepts as agent context

Every mapping that resolves annotates the datasets of the semantic model it resolves against, as the caller sees the mapping:

- **Fields.** One annotation per mapped position whose expression reads the field: the ontology and mapping, the concept expected there and its `extends` chain (`Delay → NrMinutes → Decimal`), its role — `value` (a value-type position), `identifies` (an identifying position of an object mapping of the dataset's own concept) or `references` (any other entity position) — the relationship it takes part in, and the expression when it is not a plain field. The annotation carries the concept description, the relationship's verbalization filled with the mapped expressions (`Flight `FLIGHT.id` has departure Delay `FLIGHT.dep_delay``), and the value type's `requires`, supertypes' included, as domains: a closed range as `1–10`, a one-sided bound as `<= 90`, an enumeration as `one of A, B, C, D`, any other rule as written and marked not checked.
- **Datasets.** A dataset's own concepts are the entities it produces that no `ManyToOne` or `OneToOne` link of another entity produced in the same dataset reaches; every other entity it produces is listed as also populated (`AIRCRAFT` is an `Aircraft` and also populates `Model`, `Manufacturer` and `Carrier`).
- **Relationships.** A dataset relationship lines up with a binary ontology relationship `C.r` to an entity `E` when, paired in order by `E`'s identifiers, its fields on one side are the plain fields the `E` referents under `C` read and those on the other side are the plain fields `E`'s own object mapping reads, in either orientation.

`inspect` shows these annotations on datasets, fields and relationships, in markdown and json. `search` indexes concept names, `extends` chains, descriptions and verbalizations in every channel, and `entities` accepts `concept:<Concept>` and `concept:<Ontology>.<Concept>`, which select every visible field and dataset annotated with that concept or a subtype:

```json
{"datasource": "flights", "entities": ["concept:Delay"]}
```

Saving, editing or deleting a mapping or an ontology refreshes the search entries of the datasets whose annotations change. A caller never sees an annotation or a search text derived from a dataset hidden from them.

## Checks against the semantic model

These run on every save and edit of a mapping, an ontology, a semantic model or a dataset, returned as `ontology_mismatch` warnings (a Python `UserWarning` without a warnings list), and from `validate_datasets`. They report and never refuse a save or change a model:

- **Identifiers.** A dataset's own concept, identified by plain fields of it, should be unique by the dataset's keys: its primary key, its `unique` fields and its enforced key sets, any of which is a subset of the identifying fields. A mismatch suggests the key to declare.
- **Multiplicity.** A lined-up relationship should be to-one from `C` to `E` for `ManyToOne`, and one-to-one for `OneToOne`. A contradicting cardinality is reported; a missing one is reported with the cardinality to declare. The ontology's multiplicity never changes how a query is evaluated, and never marks a relationship `trusted`.

## Checks against the data

`validate_datasets` runs these as SLayer queries under the caller's policies and reports the violating rows' count and up to five samples:

- **Value constraints.** Each value-type `requires` in the plain Layer 1 subset — the value type as the only variable, literals, comparisons, `AND`, `OR`, `NOT`, `IN` — supertypes' included, at every position mapping that value type or a subtype from one dataset: rows where the mapped value is not NULL, the mapping's predicate holds and the rule does not.
- **Required links.** Each entity-level `requires` that is a single path `E.r` (`requires: [ Airport.latitude ]`), per position producing `E` objects: the objects (identifiers with no NULL part) that no mapped position links through `E.r` with a non-NULL value, across datasets.

Everything SLayer cannot check is listed as `not_checked` with its reason: every other `requires` (document, concept or relationship level), every position whose expression reads several datasets, every check the database rejects, and every mapping that does not resolve (its `ontology` is null when the stored mapping names none readably). Every ontology finding is report-only: `slayer validate-datasets --force-clean` never acts on one.

SLayer makes no Layer 4 compliance claim: Ossie defines no Layer 4 compliance suite yet.
