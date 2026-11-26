## Why

Deleting a dataset, or removing or renaming a field or dataset metric, that a stored model-scoped metric reads only warns, and the stored semantic-model document is left holding a metric that does not resolve. Stored documents should stay Ossie-valid wherever SLayer controls the change (`architecture/system.arc42.md` §3.11, fail at save time), the rule DEV-2474 adopted for ontology mappings.

## What Changes

- **BREAKING** An explicit change that would add an unresolved reference to a stored model-scoped metric is refused, listing every metric it breaks with its reason: saving, editing, deleting or moving a dataset, saving a dataset's SLayer extensions, and saving, editing or deleting a semantic-model document. Readers of a broken metric are broken too, and named with their chain.
- One refusal lists every stored dependent a change breaks, ontology mappings and model-scoped metrics together.
- A refusal caused by a metric hidden from the caller by access tags is a hidden-content conflict naming no hidden content.
- Database-driven changes (ingest, drift apply, live-schema refinement) still apply; they warn naming each metric they break, move every affected core metric into the document's `SLAYER` entry, and the metric loads as unloadable.
- **BREAKING** A model-scoped metric with an unresolved reference (a missing dataset, field, dataset metric or model-scoped metric) is unloadable on load, transitively, instead of loading with a warning.
- REST deletes of datasets and semantic models answer such refusals with 400; MCP and CLI deletes report them.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/semantic-model-documents`: deleting document entities no longer warns about model-scoped metrics; explicit changes that break a model-scoped metric are refused, database-driven ones warn and leave it unloadable.
- `models/save-validation`: a save that breaks a model-scoped metric is refused instead of reporting it as a broken dependent.

## Impact

- `slayer/storage/base.py` (guard on every dataset and document write), `slayer/storage/access_filtered.py` (hidden-content translation), `slayer/storage/ontology_documents.py` (generic guard parts move out), `slayer/storage/model_metric_rules.py` and `slayer/storage/semantic_models.py` (one resolver; load disposition), `slayer/storage/document_mapping.py` (moving unresolved core metrics), `slayer/ontology/scope.py` (scope generalised into `slayer.core`), `slayer/core/errors.py` (generalised refusal), `slayer/engine/query_engine.py` and `slayer/mcp/server.py` (datasource moves preflighted), `slayer/api/server.py`, `slayer/cli.py` (surface handling).
- Docs: `docs/concepts/semantic-models.md`, `docs/reference/rest-api.md`, `docs/reference/mcp.md`.
