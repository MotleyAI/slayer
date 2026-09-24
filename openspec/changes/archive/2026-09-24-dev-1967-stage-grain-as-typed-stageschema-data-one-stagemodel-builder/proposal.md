## Why

A stage's output grain is well-defined (a grouped stage is unique on its dimensions; only a raw-rows stage has no key), but it is lost at the stage handoff: the sibling stand-in model carries no key, so every join to a runtime sibling is unproven and broadcasts, while the query-backed path re-derives the grain from the plan — and gets it wrong twice. It drops aggregate-valued computed dimensions from the grain (a join onto such a model is falsely proven to-one and silently multiplies), and it stamps `primary_key` on grain dimensions, which makes them identifiers (`tier:max` is refused; profiling and inspect skip them). DEV-1948's cross-stage chain tests need a proven to-one sibling join.

## What Changes

- `StageSchema` carries the stage's grain as typed data, recorded once where the stage schema is emitted: `None` for a raw-rows stage, else the columns in dimension / time-dimension position (computed dimensions — including aggregate- and transform-valued ones — included; measure positions never).
- One `model_from_stage_schema` builder turns a `StageSchema` into a `SlayerModel` for both sibling stand-ins and query-backed models; query-backed specifics (wrapped SQL, length-fitted column SQL, default time dimension) are inputs. The query-backed grain re-derivation is deleted.
- The builder stamps the grain's uniqueness: a single-column grain → `Column.unique`; a composite grain → `primary_key` on each member; an empty or absent grain → nothing.
- Joins onto a sibling stage (or a `ModelExtension` over one) that cover its grain become provably to-one and stop broadcasting.
- Only a model's **sole** primary-key column is treated as an identifier (count-family plus `min` / `max` aggregations only; skipped by inspect sampling, type probing and profiling). Composite primary-key members behave like ordinary columns. `min` and `max` join the primary-key aggregation allowlist.
- Sibling stand-ins additionally carry each column's label, format and description.

## Capabilities

### New Capabilities
- `models/identifier-columns`: which columns are identifiers (a sole primary key) and what that restricts — aggregation eligibility and exclusion from sampling / probing / profiling.

### Modified Capabilities
- `models/join-cardinality`: "Query-backed models carry their provable uniqueness" generalised to every stage output (sibling stand-ins included), with the grain defined by dimension positions and the single / composite uniqueness stamping.

## Impact

- `slayer/core/scope.py` (`StageSchema.grain`), `slayer/engine/compile/stages.py` (`_emit_stage_schema`), `slayer/ir/source_bundle.py` (builder), `slayer/engine/plan.py`, `slayer/sql/generator.py`, `slayer/engine/query_engine.py` (query-backed expansion, type probe).
- Identifier predicate in `slayer/core/models.py`, used by `slayer/engine/binding.py`, `slayer/facade/catalog.py`, `slayer/inspect/model_render.py`, `slayer/engine/profiling.py`, `slayer/engine/cardinality.py`; `PRIMARY_KEY_AGGREGATIONS` in `slayer/core/enums.py`.
- No persisted-schema change; no arc42 change.
- Docs: `docs/concepts/models.md`, `docs/concepts/queries.md`.
- Deferred: the issue's exact `x → c → b → root` repro is DEV-1948's test (needs its stage ordering); the composite-primary-key representation is DEV-1968.
