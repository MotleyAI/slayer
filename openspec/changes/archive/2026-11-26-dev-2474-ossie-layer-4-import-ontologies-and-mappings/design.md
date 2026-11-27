## Context

See proposal.md for motivation. Current state:

- `slayer/osi/importer.py` refuses any file that is not a semantic-model document.
- Semantic-model documents are stored verbatim per datasource (`save_semantic_model_text`), and a datasource can hold several of them plus standalone datasets. `export_osi` emits a whole datasource as one document named after the datasource, unless it holds exactly one document.
- The vendored upstream `validation/validate.py` already implements Ossie's reference checks for ontologies (`validate_ontology`) and mappings (`validate_mapping`, `mapping_relationship_errors`, `OntologyRelationships`). Tests already use it as an oracle for semantic models.
- Upstream `ontology/*` is unchanged between the pinned commit `d4dca4e9` and upstream `main` (`7b8ed657`).
- Both upstream mapping examples use `<value> WHERE ( <predicate> )`, which `ontology.md` does not define and Layer 1 lists as unsupported. `core.syntax` rejects it today; it accepts `==`.
- The schemas reference each other and the core schema by absolute `raw.githubusercontent.com/.../main/...` URIs.

Applicable principles: system §3.1 (the package is claimed by a node), §3.5 (async storage), §3.6 (Pydantic, never dataclasses), §3.8 (one reader of Mode B: the filtered-expression split is a `core.syntax` production, never re-tokenised elsewhere), §3.11 (fail at save time: every check runs when a document is saved, and explicit changes that would break a stored mapping are refused); importers §1 (preserve meaning: rule expressions are kept verbatim, the embedded-form upgrade is reported). The new node's own principles: python.ontology.arc42.md §3.

## Goals / Non-Goals

**Goals:** tier A of DEV-2474, as specified in the deltas under `specs/`.

**Non-Goals:**
- Using ontologies as agent context or model checks (DEV-2475), or as a query surface (DEV-2476).
- Interpreting `requires` / `derived_by` (no upstream grammar).
- Fetching documents by `iri`; OWL/RDF tooling.
- Any Layer 4 support claim in the docs.
- Changing the model-scoped-metric precedent (DEV-2477).

## Decisions

Each entry: the decision, the options weighed, the one taken and why, and who took it.

1. **Scope.** Tier A here; tiers B + C in DEV-2475 (one PR); tier D with the upstream grammar, compliance-suite and filtered-expression proposals in DEV-2476. Chain DEV-2474 → DEV-2475 → DEV-2476. Options: A only / A+B+C / A+B; the user asked for a chain in as few reasonable PRs as possible. **(user)**
2. **Storage location.** Ontologies global; mappings per datasource. Options: everything per datasource / ontologies global. Ossie treats ontology, mapping and semantic model as peer documents linked by name, one ontology mapped from several semantic models, each mapping its own document; a mapping has exactly one semantic model, which in SLayer lives in a datasource. **(user, following Ossie)**
3. **`semantic_model_ref` resolution.** A semantic-model document of the mapping's datasource, else the datasource itself; a document wins a name clash. Options: document only / document else datasource. Every semantic model SLayer presents to the outside (imported documents and `export_osi`'s datasource-named document) can be mapped, and SLayer's own exports round-trip. **(user)**
4. **Filtered mapping expressions.** `<value> WHERE ( <predicate> )` is read as the value with a row filter, parsed by a new `core.syntax` production (§3.8). Its meaning is clear from both upstream examples; the definition is proposed upstream in DEV-2476. **(default)**
5. **Rule expressions** are stored and exported as written, never interpreted. No upstream grammar exists. **(default)**
6. **References resolve by name only**; `iri` is kept, never fetched. Ossie allows a catalog to resolve by name. **(default)**
7. **Type compatibility by family**, numeric types interchangeable, `DateTime` accepting dates, `Any` accepting everything; an unknown type is accepted and reported once per mapping. Ossie: a mapping computes a value of the concept's type; §3.11 refuses only what is guaranteed to fail. **(default)**
8. **Integrity: explicit changes that break a stored mapping are refused; database-driven changes warn and leave the mapping unresolved.** Options: (a) warn everywhere, per the model-scoped-metric precedent; (b) refuse ontology changes only; (c) refuse every explicit change. Taken: (c). Ossie is silent on stores; (c) is the only option keeping every stored mapping Ossie-valid wherever SLayer controls the change. The access-tag leak is closed by the existing hidden-content conflict. The weaker model-scoped-metric precedent is DEV-2477. **(user; Codex raised it)**
9. **Access redaction** per `concept_mappings` entry, from the dataset references in each parsed expression (value and predicate, every depth); any hidden reference hides the entry. Follows the semantic-model redaction precedent; fails closed. **(default; codex)**
10. **Surfaces mirror semantic models**: Python storage API, client, REST, CLI, MCP, with MCP structured edits at concept, relationship and concept-mapping granularity. **(default; precedent)**
11. **CLI create and replace** for ontologies and mappings, as the issue lists CLI create and edit. **(codex)**
12. **Embedded `ontology_mappings` upgrade**: the inline semantic model is imported, each entry becomes a standalone mapping, one upgrade entry is reported. Ossie's deprecation path. **(default)**
13. **DEV-2452 does not block this change.** Stored ontology and mapping text is upstream Ossie text kept verbatim, already in its final format. **(default)**
14. **No Layer 4 support claim** in this PR; DEV-2476 owns the wording. **(default; issue rule)**
15. **Ossie's validator is the floor.** SLayer makes every check upstream `validate_ontology` / `validate_mapping` make, plus the issue's own, and the vendored validator is the test oracle over every example and the harvested upstream cases (`validation/tests/test_ontology.py`, `python/tests/test_ontology_models.py`). **(default, following Ossie)**
16. **Relationships resolve through `extends`**, and `Concept.name` qualified references are accepted, as upstream's `OntologyRelationships` does; SAP's `Customer` inherits `identify_by: [nr]`. **(codex)**
17. **Expected concept per position**: depth-1 link and object mappings map the concept mapping's concept; a depth-k link node maps its relationship's last role; nested referents resolve on the identifying relationship's last role. Upstream's `mapping_relationship_errors`. **(codex)**
18. **Referent coverage**: `expression` only for a simple identifier; compound identifiers need each `identify_by` relationship exactly once. **(codex)**
19. **Effective role names are unique**: the first role takes its concept's name, others their `name` else their concept's. `ontology.md`, Roles. **(codex)**
20. **QName vs full IRI** classified as upstream's `undeclared_qname_prefix`. **(codex)**
21. **Offline schema validation** through a local registry mapping the absolute `$ref` URIs to the vendored files. **(codex)**
22. **Binding errors always refuse** (unknown dataset or field, aggregate, window, non-boolean predicate); only an unknown datatype skips the type check. **(codex)**
23. **Corpus test** over every pinned ontology and mapping example, `flights.yaml` and FOAF: zero refusals, upstream validator passes, byte-identical export. **(codex)**
24. Rejected **(codex)**: "`semantic_model_ref.name` must name a datasource". The user ruled document-first (decision 3), and the examples name a document.
25. **Architecture: new precise node `ontology`.** (user, approved wording), in `architecture/model.c4`, after `storage`:
    ```
        ontology = node 'Ontologies and mappings' {
          metadata {
            package 'slayer.ontology'
          }
        }
    ```
    and, after `otel -> core.probes`:
    ```
        ontology -> core
        storage -> ontology
        importers -> ontology
        surfaces -> ontology
    ```
    Any difference in measured edges comes back for approval.
26. **Spec attachment.** (user, approved wording), in `architecture/index.yaml` `cross_cutting_specs`, after `otel`:
    ```
      ontologies: {touches: [python.ontology, python.core, python.storage, python.importers, python.surfaces]}
    ```
27. **New arc42 file `architecture/python.ontology.arc42.md`.** (user, approved wording):
    ```markdown
    # ontology — Ontologies and mappings

    ## 1. Purpose & context

    The ontology node reads, checks and resolves Ossie Layer 4 documents: ontologies
    (concepts, relationships, identifiers, rules) and mappings that populate an
    ontology from one semantic model's datasets. Storage calls it to refuse
    invalid saves; importers and surfaces call it to import, list, inspect and edit.

    ## 2. Building blocks

    No view of its own; the node appears in the `landscape` view
    ([system.arc42.md](system.arc42.md) §2).

    ## 3. Principles

    1. **Ossie's validator is the floor**: every check upstream Ossie's validator
       makes on an ontology or mapping document, this node makes too.
       [enforced: test:tests/test_dev2474_upstream_oracle.py]
    2. **Documents are kept as written**: a stored ontology or mapping exports
       exactly as it was saved. [enforced: test:tests/test_dev2474_round_trip.py]

    ## 4. Rationale

    Ontologies come from other tools (OWL importers, modelling tools) and carry
    data SLayer does not interpret; keeping them verbatim and refusing only what
    Ossie itself refuses keeps them portable (OpenSpec change
    `dev-2474-ossie-layer-4-import-ontologies-and-mappings`).
    ```
28. **Regenerated `landscape` diagram** in `architecture/system.arc42.md`: whatever `la-arch-diagrams` generates from decisions 25–26, confined to the `<!-- likec4:landscape -->` block. **(user)**

## Risks / Trade-offs

- [Upstream defines the filtered expression differently] → The reading is confined to one `core.syntax` production and documented as SLayer's; DEV-2476 proposes it upstream.
- [Upstream changes `ontology/*` after the pin] → The drift script now tracks the three files.
- [Refusing explicit changes makes mappings block dataset edits] → The refusal lists every broken mapping and path; deleting the mapping is always allowed.
- [Untyped semantic models make the type check weak] → An unknown type is reported per mapping, never silently passed; DEV-2475 can tighten this once types are refined from the database.
- [Six new MCP tools add to the agent's tool budget] → Each obeys the description-budget rule of `mcp/tool-surface`.
