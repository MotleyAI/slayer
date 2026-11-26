# ontology — Ontologies and mappings

## 1. Purpose & context

The ontology node reads, checks and resolves Ossie Layer 4 documents: ontologies
(concepts, relationships, identifiers, rules) and mappings that populate an
ontology from one semantic model's datasets. Storage calls it to refuse
invalid saves; importers and surfaces call it to import, list, inspect and edit.
Search and inspect call it to annotate datasets, fields and relationships with the concepts they map; storage and the engine call it to check the semantic model and its data against the ontology.

## 2. Building blocks

No view of its own; the node appears in the `landscape` view
([system.arc42.md](system.arc42.md) §2).

## 3. Principles

1. **Ossie's validator is the floor**: every check upstream Ossie's validator
   makes on an ontology or mapping document, this node makes too.
   [enforced: test:tests/test_dev2474_upstream_oracle.py]
2. **Documents are kept as written**: a stored ontology or mapping exports
   exactly as it was saved. [enforced: test:tests/test_dev2474_round_trip.py]
3. **Checks report, never rewrite**: no ontology check changes a stored model
   or refuses a save. [enforced: test:tests/test_ontology_checks_report_only.py]
4. **Unchecked is listed**: every `requires` SLayer does not check is reported
   as not checked, never skipped silently.
   [enforced: test:tests/test_ontology_not_checked_listed.py]

## 4. Rationale

Ontologies come from other tools (OWL importers, modelling tools) and carry
data SLayer does not interpret; keeping them verbatim and refusing only what
Ossie itself refuses keeps them portable (OpenSpec change
`dev-2474-ossie-layer-4-import-ontologies-and-mappings`).
