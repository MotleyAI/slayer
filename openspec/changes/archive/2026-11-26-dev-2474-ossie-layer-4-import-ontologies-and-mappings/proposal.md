## Why

Ossie Layer 4 defines ontology documents (concepts, relationships, identifiers, rules) and mapping documents that populate an ontology from a semantic model's datasets. SLayer refuses both today, so a user with an Ossie ontology cannot bring it into SLayer at all. This change is the foundation (tier A of DEV-2474) for using ontologies as agent context and model checks (DEV-2475) and as a query surface (DEV-2476).

## What Changes

- Vendor upstream `ontology/ontology.json`, `ontology/mapping.json` and `ontology/ontology.md` at the pinned commit, tracked by the drift script.
- `import-osi` imports ontology and mapping documents instead of refusing them (**BREAKING** only for callers relying on the refusal). The deprecated embedded `ontology_mappings` form is upgraded: its inline semantic model is imported and each entry becomes a standalone mapping, reported as an upgrade.
- Ontologies are checked against the upstream schema, every check upstream Ossie's validator makes, and SLayer's own rules: names resolve (through `extends`), `extends` is acyclic and type-correct, QName prefixes are declared, effective role names are unique, `identify_by` names binary relationships. `requires` / `derived_by` are kept but not interpreted.
- Mappings are resolved at save time: the ontology by name; the semantic model as a stored semantic-model document of the mapping's datasource, else the datasource itself; every concept, relationship, referent and link-tree level per Ossie; every expression parsed as a row-level Layer 1 expression (with SLayer's reading of the filtered form `<value> WHERE ( <predicate> )`) whose type is compatible with the concept's built-in base type.
- Ontologies are stored globally; mappings per datasource. Both are stored verbatim and export byte-identical.
- An explicit change (to an ontology, semantic model, dataset or datasource) that would break a stored mapping is refused; database-driven changes warn and leave the mapping unresolved.
- Ontologies and mappings are available through the Python storage API, the client, REST, the CLI and MCP (list, inspect, create, edit, delete), with access-tag redaction of mapping entries.
- New architecture node `ontology` (`slayer.ontology`).
- No Layer 4 support claim: the docs state what is imported, checked and stored, and that the documents are not used yet.

## Capabilities

### New Capabilities
- `ontologies/ontology-documents`: ontology and mapping documents — recognition, checks, mapping resolution, storage, integrity, access, surfaces and export.

### Modified Capabilities
- `importers/osi-import`: ontology and mapping documents import instead of being refused; the upstream ontology examples import.
- `importers/osi-export`: ontologies and mappings export.
- `storage/custom-backends`: ontology and mapping hooks default to unsupported.
- `mcp/tool-surface`: the ontology and mapping tools fit the surface rules.

## Impact

- New package `slayer/ontology/`; `slayer/core/syntax.py` gains the mapping-expression production.
- `slayer/osi/importer.py` (recognition, ordering, embedded upgrade), `slayer/osi/exporter.py` / `slayer/cli.py` (export).
- `slayer/storage/base.py`, `yaml_storage.py`, `sqlite_storage.py`, `access_filtered.py` (new document kinds, hooks, integrity refusals).
- Surfaces: `slayer/mcp/server.py`, `slayer/api/server.py`, `slayer/client/slayer_client.py`, `slayer/cli.py`, `slayer/inspect/`.
- Corpus: `tests/fixtures/ossie_corpus/ontology/`, `provenance.yaml`, `scripts/ossie_schema_drift.py`.
- Architecture: `model.c4`, `index.yaml`, new `python.ontology.arc42.md`, regenerated `system.arc42.md` diagram.
- Docs: `docs/concepts/ontologies.md` (new), `docs/osi/`, `docs/reference/`.
