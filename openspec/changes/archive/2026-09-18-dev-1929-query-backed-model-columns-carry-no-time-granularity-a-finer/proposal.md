# Proposal: column time granularity — a finer time dimension over a bucketed model column is a typed error

## Why

A saved query-backed model caches its final stage's columns as plain `Column`s: the type survives, the time-bucket granularity does not, so a `day` time dimension over a `month`-bucketed cached column renders a day truncation over month starts and returns month rows labelled as days with no error (DEV-1929, reproduced 2026-09-18). DEV-1471 closed the same gap for runtime stages by putting the granularity on the stage schema and adding one checker rule; the persisted `Column` has no field to carry the fact, so the rule is total over stage datasets and vacuous over model datasets — one type fact living on one carrier but not the other.

## What Changes

- `Column` gains an optional `granularity` (one of the `TimeDimension` granularities): the bucket the column is already truncated to. It is honoured on every model kind — engine-stamped on query-backed models, hand-settable on table-backed columns — and its description tells authors to set it only when they are sure.
- The query-backed cache builder stamps it from the final stage's bucketed columns, so the persisted snapshot and the runtime virtual model both carry it; the sibling-stage stand-in models used by `ModelExtension`-over-sibling and stage joins carry it too.
- The DEV-1471 refinement rule reads it for model columns, so a finer or non-nesting time dimension over any bucketed column — stage, query-backed or hand-set — is the same typed checker error; same or nesting-coarser still binds and executes.
- One rejection message for all three cases, no longer phrased in terms of an upstream stage.
- A `granularity` on a non-temporal column is rejected at construction.
- Ingestion never sets it; the MCP model tools accept, preserve and report it.

## Capabilities

### New Capabilities

- `models/column-granularity`: a column's declared time-bucket granularity — honoured on hand-set and query-backed columns, validated against the column type, persisted, left unset by ingestion, round-tripped by the MCP model tools.

### Modified Capabilities

- `queries/time-dimensions`: MODIFIED — the re-bucketing error's remedy no longer refers to an upstream stage; ADDED — re-bucketing a bucketed model column is typed (query-backed cache and hand-set); ADDED — sibling-stage stand-ins carry the bucket so `ModelExtension`-over-sibling and stage joins are covered.

## Impact

- `slayer/core/models.py` (`Column.granularity` + validator), `slayer/engine/query_engine.py` (`_expand_query_backed_model` stamps it), `slayer/ir/source_bundle.py` (`synthetic_model_from_stage_schema` stamps it), `slayer/engine/binding.py` (model arm of `_time_dimension_column_facts` reads it), `slayer/engine/elaborate_env.py` (message), `slayer/ir/bound.py` (docstring), `slayer/mcp/server.py` (two docstring clauses), `slayer/inspect/model_render.py` (`render_model_inspection` emits `granularity` in its JSON column payload when set).
- Tests: `tests/_dev1871_raise_ledger.py` one row's message (approved); new `tests/test_dev1929_column_granularity.py`.
- Docs: `docs/concepts/models.md`, `docs/concepts/queries.md`, `docs/concepts/terminology.md` (one sentence each). No schema-version bump; no arc42 edit.
- Follow-ups filed: DEV-1938 (dbt/Cube granularity import), DEV-1939 (one `ColumnFacts` core shared by `Column`, `ValueSlot`, `StageColumn`).
