## Why

DEV-2474 imports, checks and stores Ossie Layer 4 ontologies and mappings, but nothing uses them yet. Agents should see what each dataset, field and relationship means in the ontology, and the semantic model and its data should be checked against what the ontology declares (tiers B and C of DEV-2474).

## What Changes

- Concept annotations computed from every resolving, caller-visible mapping: each mapped field (concept, `extends` chain, role, verbalized relationship), each dataset (its own concepts and the concepts it also populates) and each SLayer relationship that lines up with an ontology relationship.
- `inspect` of a dataset, field or relationship shows its annotations, the concept descriptions, the filled verbalizations and value-type `requires` as a domain; `inspect` of an ontology returns its concept graph.
- `search` matches concept names, `extends` chains, descriptions and verbalizations in both the question and embedding channels, and accepts `concept:<Concept>` / `concept:<Ontology>.<Concept>` entity references that select every field and dataset of that concept or a subtype.
- Model checks (identifier vs dataset keys, multiplicity vs a lined-up relationship's cardinality) report as typed warnings on saves and as `validate_datasets` findings; they never refuse a save or rewrite the model.
- Data checks in `validate_datasets`: value-type `requires` in the plain Layer 1 subset and concept-level required links, with violation counts and samples; every `requires` not checked is listed with its reason.
- Save warnings (`warnings_out`) on semantic-model, ontology and mapping saves and edits, on every surface.
- Docs: ontologies concept page, `validate_datasets`, `search`, `inspect`.

## Capabilities

### New Capabilities
- `ontologies/ontology-use`: the ontology as agent context (annotations, inspect, search) and as checks on the semantic model and its data (save-time warnings, `validate_datasets` findings).

### Modified Capabilities
- `ontologies/ontology-documents`: `inspect` of an ontology returns its concept graph (compact) and adds the stored text (full).
- `mcp/tool-surface`: the `search` and `validate_datasets` descriptions name concept references and ontology findings within the budget.

## Impact

- Code: `slayer/ontology/` (annotations, checks, findings), `slayer/core/syntax.py` (constraint reading and the check predicate), `slayer/storage/base.py` and `access_filtered.py` (save warnings), `slayer/engine/query_engine.py` (`validate_datasets`, save warnings), `slayer/live_schema/schema_drift.py` (report entry union), `slayer/search/` (render, entity resolution, embedding refresh), `slayer/inspect/`, `slayer/mcp/server.py`, `slayer/api/server.py`, `slayer/cli.py`, `slayer/client/`.
- Architecture: arrows `engine -> ontology`, `live_schema -> ontology`, `search -> ontology`; `ontologies` touches `python.engine`, `python.live_schema`, `python.search`; two new principles in `python.ontology.arc42.md`.
- No stored-format change; no breaking change.
