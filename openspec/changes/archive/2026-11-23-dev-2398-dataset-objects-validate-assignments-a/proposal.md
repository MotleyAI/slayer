## Why

Dataset-side objects validate only when they are built: `field.label = 5`, a duplicate field appended to `dataset.fields`, or a half-switched source are accepted afterwards and reach queries and storage. Construction already refuses unknown keys (DEV-1607); mutation is the remaining door to silently wrong datasets. It is breaking, so it ships in 2.0.

## What Changes

- **BREAKING** Every dataset-side object (dataset, field, metric, relationship, aggregation, aggregation parameter, source-dataset origin, extension note, datasource, and their nested number format and expression objects) validates an assigned attribute with the checks construction applies; a refused assignment raises and leaves the object exactly as it was.
- New `assign(**changes)` on these objects: changes linked attributes together (e.g. `sql` and `sql_table`, a relationship's two key lists), validated once on the final state, all or nothing.
- **BREAKING** Saving a dataset (`save_dataset`, `save_dataset_extensions`) or a datasource re-validates it in full first, so an object broken by an in-place container edit (`fields.append(...)`) is refused before anything is written.
- Construction normalizations (deprecated `sql` shorthand, the number format's default currency symbol, an sql-mode list field's Opaque element type) keep their results but no longer write through assignment; a prebuilt field placed in a dataset is no longer modified.
- MCP `edit_dataset` source switches and the deprecated `join_pairs` setter change their linked attributes in one step.
- New `python.core` principle: every write is checked; enforced by a law test over every model reachable from a dataset or datasource.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `compatibility/ossie-vocabulary`: adds "Dataset objects validate assignments" beside "Datasets refuse unknown keys".
- `models/save-validation`: adds "Saves re-validate the whole dataset or datasource".

## Impact

- `slayer/core/models.py`, `slayer/core/format.py`, `slayer/core/field_expression.py`; new `slayer/core/assignment.py` (atomic-assignment mixin, shared with DEV-1695).
- `slayer/storage/base.py` (`save_dataset`, `save_dataset_extensions`, `save_datasource`), `slayer/mcp/server.py` (`edit_dataset`).
- `architecture/python.core.arc42.md` §3 (approved principle).
- `docs/upgrading/2.0.md`.
- Python callers that assign invalid intermediate states or mutate dataset lists in place before saving; in-memory in-place edits stay possible until DEV-2401.
