## Why

An Ossie semantic-model document reaches SLayer today only through `slayer import-osi`, which converts it: it needs a live datasource for introspection, it is lossy and reported, and writing back rewrites user documents. After the 2.0 vocabulary and shape slices SLayer's dataset shapes match Ossie's, so a document can be stored as written and decomposed into native datasets, relationships and model-scoped metrics. This is SLayer 2.0 slice 4b (umbrella DEV-2372): it makes every valid Ossie document valid SLayer configuration with Ossie's meaning, absorbs DEV-2214 (model-scoped metrics referenced by bare name), and is the first step toward strictly Ossie-valid storage after 2.0.

## What Changes

- **A stored semantic-model document**: `semantic_models/<data_source>/<name>.yaml` (YAML storage) or a `semantic_models` table holding the document text verbatim (SQLite). The datasource binding comes from where the document is stored. Only Ossie's flat `0.3.0.dev0` shape is accepted natively; unknown keys are refused. SLayer's own migration counter lives in a document-level `SLAYER` non-portable extension entry, never in Ossie's `version`.
- **Decomposition, without conversion**: datasets become native datasets exposing exactly their declared fields; top-level relationships become named many-to-one relationships on their `from` dataset; top-level metrics become model-scoped metrics. A load never writes and never introspects.
- **One namespace per datasource**: dataset and model-scoped metric names are unique across every document and standalone dataset of a datasource (a SLayer datasource is the equivalent of an Ossie semantic model).
- **Model-scoped metrics by bare name**: a bare name in a query resolves as query alias, then model-scoped metric, then the legacy conversion, then an unknown-metric error. A model-scoped metric is the query-level expression it names, each aggregate one component homed on the dataset it reads.
- **`AGG(dataset.metric)`** evaluates a dataset metric as one component, in model-scoped metrics, queries and stored dataset metrics, with Ossie's restrictions.
- **Relationship shapes Ossie allows are accepted for every dataset**: self-relationships (inert in route search), relationships named like a dataset, keys on computed fields.
- **Edit through**: the dataset tools (`edit_dataset`, `create_dataset`, `delete_dataset`, REST, CLI) work on a document's datasets by patching the document; Ossie-expressible content is written as Ossie core, SLayer-only content as `SLAYER` entries. Saves keep comments and formatting (ruamel.yaml round-trip mode).
- **Unknown datatypes** load as unknown and are probed lazily by the engine, never persisted on load; `validate-datasets` reports them and `--force-clean` writes them in.
- **BREAKING** `slayer import-osi` stores the file verbatim as a native document after an Ossie-validity check (an invalid document is refused with every reason) instead of converting it; it no longer needs a reachable datasource except to upgrade legacy shapes. Re-importing a document imported under 1.x replaces its converted datasets.
- **BREAKING** `slayer export-osi` writes one merged document per datasource (the stored text verbatim when the datasource holds exactly one document); model-scoped references to dataset metrics are written as `AGG(…)`.
- New MCP tools `create_semantic_model`, `edit_semantic_model`, `delete_semantic_model`; REST `/semantic-models`; Python client methods; CLI `slayer semantic-models {list,show,delete}` and `slayer ossie-roundtrip`. Search indexes documents and model-scoped metrics.
- **Ossie corpus acceptance**: upstream examples, portability rungs m0–m8, compliance models and harvested validator documents are vendored with provenance; every document validates, loads, compiles and round-trips; upstream's `compliance/wide_table` cases are the query oracle. A dedicated weekly job runs the suite against a fresh upstream corpus.

## Capabilities

### New Capabilities
- `models/semantic-model-documents`: the stored document entity, its datasource binding, decomposition, the per-datasource namespace, edit-through and round-trip fidelity, unknown datatypes, access filtering, surfaces, search, and the Ossie corpus acceptance.
- `queries/model-scoped-metrics`: bare-name resolution of model-scoped metrics, their evaluation, their save-time rules, and `AGG(dataset.metric)` in every position.

### Modified Capabilities
- `importers/osi-import`: `import-osi` stores documents natively after an Ossie-validity check instead of converting them; legacy shapes are upgraded per Ossie's migration guidance; the conversion requirements are retired.
- `importers/osi-export`: one merged document per datasource, verbatim for a single-document datasource, document boundaries carried in `SLAYER`, `AGG(…)` for model-scoped references to dataset metrics.
- `models/join-keys`: relationship keys may name computed fields.
- `models/join-traversal`: edge names no longer share the dataset-name namespace; self-relationships are accepted and inert.
- `models/schema-drift`: untyped document fields are a finding that `--force-clean` applies.
- `models/save-validation`: save-time checks judge what a save adds or changes, plus the existing broken-dependents analysis.
- `mcp/tool-surface`: the semantic-model tools and bare model-scoped metrics.
- `cli/commands`: `semantic-models` and `ossie-roundtrip` commands; `export-osi` writes the merged document.
- `storage/custom-backends`: the semantic-model storage hooks and their default.

## Impact

- **Code**: `slayer/core` (document model; the `SLAYER` payload moves here from `slayer/osi/slayer_extension.py`), `slayer/storage` (hooks, decomposition, ruamel patcher, owner routing, YAML/SQLite backends, access filtering, fingerprints), `slayer/engine` (bare-name resolution, `AGG`, unknown-type probing, model-scoped metric expansion), `slayer/live_schema` (drift findings), `slayer/osi` (import/export; the semantic converter is retired), `slayer/mcp`, `slayer/api`, `slayer/client`, `slayer/cli.py`, `slayer/search`.
- **Dependencies**: `ruamel-yaml` moves from a dev to a runtime dependency.
- **Architecture**: `architecture/semantics.arc42.md` Principle 3 gains the model-scoped metric clause (approved wording); no `model.c4` or `index.yaml` change intended.
- **CI**: a new weekly-only job for the fresh-upstream corpus; `scripts/ossie_schema_drift.py` extended to the corpus.
- **Docs**: new `docs/concepts/semantic-models.md` (in `zensical.toml` nav), updates to datasets, storage, Ossie import/export, schema drift and reference pages, and a migration-guide section in `docs/upgrading/2.0.md`.
- **Related issues**: DEV-2212 and DEV-2217 take over the xfailed wide-table cases; DEV-2220 takes the upstream grader registration; DEV-1661 mirrors the layout; DEV-2282 and DEV-2244 own the constructs that load but cannot yet be evaluated.
