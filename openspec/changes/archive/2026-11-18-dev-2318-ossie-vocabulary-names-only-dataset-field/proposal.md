## Why

SLayer 2.0 is a strict superset of Ossie (umbrella DEV-2212), but SLayer's names (`model`, `column`, `measure`, `join`, `formula`, `source_model`) differ from Ossie's (`dataset`, `field`, `metric`, `relationship`, `expression`, `population`). This change is the names-only half of DEV-1607: every SLayer name moves to its final Ossie form, with no shape change, and every old spelling keeps working until 3.0. DEV-1607 then makes its shape changes on top. Renaming and restructuring are kept in separate changes.

## What Changes

- **Code:** every class, field, method, function, parameter, constant, `Literal` member, payload key and module named with the old words is renamed per DEV-2318's word table (`measure` → `metric` everywhere, internal names included). The carve-outs are physical database objects, result-set columns, SQL `JOIN` as an operation, foreign formats, Pydantic's `model_*`, and the storage layout. Renamed modules leave an `ImportError` stub naming the new path.
- **Stored documents:** version steps SlayerModel 25→26, SlayerQuery 15→16, Memory 11→12, Embedding 1→2 rename the stored keys silently. The embeddings sidecar rewrites its row kinds in place.
- **Fresh input in the old spelling** converts with one `deprecated_input` warning per distinct old form per document, until 3.0. An old and a new spelling of one concept together are refused.
- **Python:** a curated public list of old class, method and attribute names keeps working until 3.0 as typed `@deprecated` twins. `reportDeprecated` becomes an error in this repo.
- **Surfaces:**
  - MCP: the tools become `datasets_summary`, `create_dataset`, `edit_dataset`, `delete_dataset`, `validate_datasets`, `recommend_population` and `ingest_datasource`. Old tool arguments, `inspect` kinds and help references still convert inside the new tools.
  - REST: `/datasets…`, with the old `/models…` routes answering until 3.0 and sending a `Deprecation` header.
  - CLI: new commands and flags, with the old ones kept until 3.0.
  - Search: new kinds and graph labels; Cypher accepts both spellings.
- **BREAKING:**
  - old MCP tool names are gone;
  - responses carry `attributes.metrics` and no `attributes.measures`;
  - warning `kind` values and payload fields are renamed;
  - a 2.0 store is no longer readable by 1.x;
  - the usage report goes to schema 3;
  - the Ossie export `SLAYER` payload goes to version 2, and version 1 is still read.
- **Docs, notebooks and help topics** move to the new words. The literal identifiers in `openspec/specs/` and `architecture/` are respelled.

## Capabilities

### New Capabilities
- `compatibility/ossie-vocabulary`: SLayer's names in Ossie's vocabulary across stored documents, queries, Python names and entry points, and how each pre-2.0 spelling converts until 3.0.

### Modified Capabilities
- `models/stored-upgrade`: a store written by this release is no longer promised readable by v1.4.1; omitting unset fields and the backend layout are kept.
- `usage-reporting/collection`: the report schema version becomes 3, and a usage key renamed by this change counts as its new key when a stored batch is read.

## Impact

- **Code:** every package under `slayer/`; `tests/`; `docs/` and its notebooks; `slayer/memories/help_content/`; `examples/`.
- **Stored data:** four version steps and an in-place rewrite of the embeddings sidecar's row kinds. The YAML `models/` folder and the SQLite schema are unchanged, and nothing is re-embedded.
- **Downstream:**
  - Storyline keeps working through the curated twins until 3.0, except for its hard-coded MCP tool names and its imports of moved modules.
  - The usage-report receiver must accept schema 3.
- **Tooling:** `dr-refactor rename-map`, with its input committed as `rename-map.yaml` in this change. `typing-extensions` becomes a direct dependency. `[tool.basedpyright]` sets `reportDeprecated = "error"`. The `living-architecture` pin moves to 0.11.0.
- **Architecture:** three approved literal-identifier edits, in `python.engine` P5 and P8 and in `semantics`. The `compatibility` group's `touches:` list in `index.yaml` is extended. `model.c4` is unchanged.
- **Follow-up:** DEV-1607 stacks its shape changes on this change. Its Ossie payload becomes version 3, and it keeps the Ossie datatype values.
