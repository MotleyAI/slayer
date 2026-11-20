## Why

A stage's output field can come out with no type, and `dataset_from_stage_schema` then reads it as FLOAT. Since DEV-2219 every stage is read as the dataset its schema defines, so a later stage treats a column of unknown type as a number: branch typing goes wrong (`iif(n > 1, u, 'x')` loses its TEXT type, `coalesce(u, n)` types DOUBLE instead of INT) and checks see a guessed type. A probe over the unit suite found about 16.8k untyped stage fields. They are not a few holes: slot types come from several partial derivations instead of the one derivation `architecture/system.arc42.md` §3.10 asks for.

## What Changes

- Every value is typed by one total derivation of its key: arithmetic, scalars, transforms, `IN`, literals, cross-dataset aggregates, regroup placeholders and custom aggregations all get their real type. A NULL-only value is INT.
- Every place that types a slot reads that derivation: bound measures and dimensions, stage-producer slots, hidden slots and the SQL facade's metric catalogue. Explicit types still win.
- A stage column always carries a type; the FLOAT fallback is removed.
- A custom aggregation's result type is its declared `datatype`, else the type of its SQL template inferred with the bound arguments' types, else its input's type. Aggregation definitions get an optional `datatype`.
- **BREAKING** (types only) Values whose type was guessed now report their real type: a custom `AVG({value})` over an INT column is DOUBLE, a custom aggregation over a boolean takes its template's type, and a later stage sees each column's real type. Mixed-type conditionals over stage columns are now refused at plan time like the same conditional over model columns. Newly typed values get the casts the existing cast policy derives.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities
- `queries/multi-stage`: every stage column carries a type, read the same by a later stage.
- `aggregations/formula-templates`: a custom aggregation's result type (declared, inferred from its template, else its input's type).
- `aggregations/boolean-inputs`: a custom aggregation over a boolean takes its declared or inferred type, not the source type.
- `queries/time-dimensions`: no untyped stage column can exist; the scenario "Untyped stage column fails closed" keeps its title (OpenSpec cannot drop or rename a published scenario) and now covers a NULL-only, INT stage column.

## Impact

- `slayer/core/keys.py` (the one derivation), `slayer/engine/key_metadata.py` and `slayer/engine/bind_inputs.py` (parallel derivations retired), `slayer/engine/compile/projection.py` and `stages.py` (slots typed at intern; producer and placeholder typing), `slayer/ir/planned.py` and `slayer/core/scope.py` (required types), `slayer/ir/source_bundle.py` (fallback removed), `slayer/facade/catalog.py`, `slayer/sql/` (template typer), `slayer/core/models.py` (`Aggregation.datatype`).
- Emitted SQL gains casts for newly typed values; golden SQL is re-blessed.
- Stored datasets: `Aggregation.datatype` is a new optional key; existing documents load unchanged.
- Docs: custom aggregations and multi-stage pages; MCP schema descriptions of aggregations.
