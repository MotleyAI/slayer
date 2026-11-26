## Context

See proposal.md for motivation. Current state:

- `slayer/ontology/` (archived change `dev-2474-ossie-layer-4-import-ontologies-and-mappings`) holds the concept graph (`Ontology`: kinds, supertypes, relationships, nearest-declared `identify_by`, `find` through `extends`), mapping resolution with the expected concept per position, and `MappingScope.model_of` (a mapping resolves against a semantic-model document, else the datasource).
- `AccessFilteredStorage` already redacts every mapping entry reading a dataset hidden from the caller, and its problems; access tags exist only on datasets.
- Only `engine.save_dataset(warnings_out=…)` returns save warnings to surfaces; semantic-model and ontology saves return nothing, mapping saves return notes.
- `validate_datasets` returns `DriftReportEntry` (a closed union in `live_schema.schema_drift`); `REPORT_ONLY_FINDINGS` keeps cleanup off report-only entries.
- Search builds its BM25 corpus per request from the caller's storage; embeddings are keyed by a hash of the rendered text and refreshed by `refresh_dataset_subtree` / `refresh_documents` / `refresh_datasource`, skipping unchanged text.
- `core.syntax` holds span-based respelling (`respell_bare_names`) and the canonical writer; it is the only Mode B reader (system §3.8).
- The flights semantic model declares no keys and no relationships.

Ossie semantics (Ontology Working Group, "Semantics of the Ossie ontology language" and "Formula language for ontologies", linked from apache/ossie#361): a `requires` list is conjunctive; an entity-type `requires` holds for every object of the concept, a subtype's objects included (§5.2); a value-type `requires` constrains every role played by the type or a subtype (§5.3); unification is inner-join, so a NULL makes no link; `ManyToOne` is functional, `OneToOne` an injection.

Applicable principles: system §3.4 (SQL as AST: checks run as SLayer queries through the engine), §3.6 (Pydantic), §3.7 (RLS fails closed: checks run under the caller's policies), §3.8 (one Mode B reader: the check predicate is built in `core.syntax`), §3.11 (fail at save time: ontology mismatches are observations about intent and current data, not definitions guaranteed to fail every query, so they warn); core §3.5 (typed errors), §3.6 (warnings are structured payloads, surfaced twice); storage §2 (access is a view: annotations come from the redacted mappings); engine §8 (models persist verbatim: no check rewrites a model); ontology §1–§2 and the new §3–§4.

## Goals / Non-Goals

**Goals:** tiers B and C of DEV-2474, as specified in `specs/ontologies/ontology-use/spec.md` and the two deltas.

**Non-Goals:**
- Querying by concept, interpreting `derived_by`, or any `requires` form beyond the plain value-type subset and single-path required links (DEV-2476).
- Using the ontology's multiplicity in query planning.
- Declaring composite unique keys beside a primary key (DEV-2463 adds dataset-level unique keys).
- A Layer 4 support claim in the docs (DEV-2476).
- Comparing multiplicities of unary, n-ary or value-typed relationships.

## Decisions

Each entry: the decision, the options weighed, the one taken and why, and who took it.

1. **Mismatches warn, never refuse.** Options: warn / refuse at save time. Refusing is not what §3.11 asks for: no query fails because of a mismatch; the ontology describes intent and the data may lag. **(default)**
2. **Ossie's `requires` semantics as stated in Context** (conjunctive lists; every object of a concept, referenced ones and subtypes' included; every role of a value type and its subtypes; NULL makes no link). **(default, following Ossie)**
3. **Multiplicity is report-only.** An undeclared cardinality gets a suggestion to declare it, never `trusted`; the ontology multiplicity never feeds planning. The issue: "counts as a declared cardinality, never a trusted one" and "none rewrites the model". **(default)**
4. **Identifier agreement is uniqueness** by `is_key_set_unique` over the primary key, `unique` fields and `enforced_keys` (a unique subset makes the identifier unique). The composite-beside-primary-key case is reported without a declaration, since none can be stored yet. **(default)**
5. **Data checks are SLayer queries** executed by the engine, never hand-built SQL (§3.4, §3.7). **(default)**
6. **Concept text joins each field's and dataset's rendered search text** for the BM25 and embedding channels; mapping and ontology saves, edits and deletes refresh the affected datasets' embeddings (the content hash re-embeds only changed text); `concept:` entity references give deterministic concept search. Options: BM25 only / both channels. Both: the embedding channel is the one that reaches a field by meaning. **(user)**
7. **Required links run one anti-join query per producing position.** Stages have no union, so each position producing `E` objects is a stage of distinct non-NULL identifier tuples, left-joined through query-local relationships to one stage per position linking `E.r`, keeping the tuples no link stage matches; violations are reported per producing position. Rejected: a Python set difference over all identifiers (unbounded). **(codex)**
8. **The check predicate is built in `core.syntax`**: the constraint is read once, checked against the plain subset, its concept variable respelled to the mapping's value by `core.syntax`'s own respelling, and the mapping's predicate conjoined; no other module tokenises Mode B (§3.8). **(codex)**
9. **Save warnings use `warnings_out`**, the channel `engine.save_dataset` already has, extended to semantic-model, ontology and mapping saves and edits on every surface; Python warnings remain the fallback (core §3.6). The refusals of changes that break a stored mapping are unchanged. **(codex)**
10. **A dataset's own concepts** are the entities it produces that no mapped `ManyToOne` / `OneToOne` link (identifying links included) of another entity produced in the same dataset reaches; the identifier check runs only for them. Every top-level object mapping would report `Model`, `Manufacturer`, `City`, `State` and `Market` as key mismatches on flights, whose datasets are denormalised. **(codex; user)**
11. **Inheritance:** value and entity `requires` accumulate over every ancestor; `identify_by` keeps the nearest declaration; relationships resolve through `Ontology.find`, qualified `Base.r` included. **(codex, following Ossie)**
12. **Required links** check each `requires` entry separately; a compound identifier with a NULL part produces no object. **(codex, following Ossie)**
13. **Capability per position:** value constraints run any row-level expression reading one dataset; identifier checks and relationship line-up need plain fields, paired in order by identifying relationship; an expression reading several datasets is listed as not checked. **(codex)**
14. **Annotations follow `semantic_model_ref`:** a mapping annotates only the datasets of the semantic model it resolves against. **(codex)**
15. **`concept:` unqualified** matches that concept in every ontology the caller's visible mappings use, each hit naming its ontology; a field reached through several mappings is listed once with every annotation. **(codex)**
16. **Query-backed and SQL datasets:** data checks go through normal binding and expansion, a binding failure becoming a not-checked entry; key checks read the stored declaration. **(codex)**
17. **Unresolved mappings** annotate nothing and each yields one not-checked entry with its (redacted) problems. **(codex)**
18. **One finding model**, `OntologyFinding` with a `check` field (`identifier`, `multiplicity`, `constraint`, `required_link`, `not_checked`), added to `DriftReportEntry` and `REPORT_ONLY_FINDINGS`. **(codex)**
19. **Search refresh on deletes too**; the BM25 corpus is per request and caller-scoped already; no search result carries concept text derived from a dataset hidden from the caller. **(codex)**
20. **Sample size:** up to five distinct violating values or identifiers. **(default)**
21. **Unresolved mappings, multi-dataset positions and failed check queries are listed as not checked**, never skipped. **(default)**
22. Rejected **(codex)**: "access redaction is missing" — `AccessFilteredStorage` redacts entries reading hidden datasets, and access tags are dataset-level only.
23. Rejected **(codex)**: "report an identifier unique only through a subset key separately" — the issue names `is_key_set_unique`; a unique subset makes the identifier unique, which agrees with the ontology.
24. Rejected **(codex)**: "list unary and n-ary multiplicities as not checked" — the comparison is defined only for lined-up binary entity relationships; listing every value-typed multiplicity would flood the report. The not-checked principle covers `requires`.
25. Rejected **(codex)**: "ontology inspect leaks mappings" — its mapped-field section reads the caller's redacted mappings.
26. **Architecture: arrows** (user, approved wording), in `architecture/model.c4`, after `surfaces -> ontology`:
    ```
        engine -> ontology
        live_schema -> ontology
        search -> ontology
    ```
    Any difference in measured edges comes back for approval.
27. **Spec attachment** (user, approved wording), in `architecture/index.yaml`:
    ```
      ontologies: {touches: [python.ontology, python.core, python.storage, python.importers, python.surfaces, python.engine, python.live_schema, python.search]}
    ```
28. **arc42 `architecture/python.ontology.arc42.md`** (user, approved wording). §1 gains, after "…to import, list, inspect and edit.":
    > Search and inspect call it to annotate datasets, fields and relationships with the concepts they map; storage and the engine call it to check the semantic model and its data against the ontology.

    §3 gains:
    ```markdown
    3. **Checks report, never rewrite**: no ontology check changes a stored model
       or refuses a save. [enforced: test:tests/test_ontology_checks_report_only.py]
    4. **Unchecked is listed**: every `requires` SLayer does not check is reported
       as not checked, never skipped silently.
       [enforced: test:tests/test_ontology_not_checked_listed.py]
    ```
29. **Regenerated `landscape` diagram** in `architecture/system.arc42.md`: whatever `la-arch-diagrams` generates from decision 26, confined to its block. **(user)**
30. **New capability `ontologies/ontology-use`** in the existing cross-cutting `ontologies` group. **(user)**
31. **A relationship line-up's target side** is the object mapping of `E` or of a subtype of `E`, since a subtype's objects are objects of `E` (as decision 2 reads `requires`). **(user, following Ossie)**

## Risks / Trade-offs

- [The required-link anti-join across stages and query-local relationships hits an engine limit] → The feasibility test lands in pr-tests first; a limit found there comes back as a plan question, never a silent narrowing.
- [Data checks are slow on large datasets] → One count query and one sample query per check, with the database's own limits; a check the database rejects is listed, not fatal.
- [Concept text changes embeddings on every mapping edit] → Only datasets whose annotations change are refreshed, and the content hash skips unchanged text.
- [The dataset's-own-concept rule misreads an unusual mapping] → It only gates the identifier check and the dataset annotation; every produced concept is still listed as populated.
