## Why

Agents still see the legacy colon aggregation spelling (`amount:sum`, `*:count`): importers write it
into saved models, `recommend_root_model` replies in it, error remedies suggest it, a tool description
teaches it, and the legacy formula validator even warns that functional input should be rewritten to
colon. Agent-facing docs and help already use only the functional spelling (`sum(amount)`); every other
surface, the architecture docs and the specs must match, while colon input keeps parsing.

## What Changes

- Every emitter of aggregation text (dbt, Cube incl. view facades, OSI importers; root-model
  recommendation paths) renders through one functional renderer — no hand-assembled colon text.
- Error/warning remedies and the `create_model` tool description name the functional spelling.
- The legacy formula validator stops warning on functional input and accepts `count(<path>.*)`.
- Cube import: a measure over a dropped column is reported (was silent), and calc measures depending
  on a dropped measure are dropped and reported too (were left dangling).
- Specs: every colon example becomes functional; `aggregations/functional-form` keeps one requirement
  accepting the legacy colon spelling as an exact equivalent, and gains one stating that SLayer emits
  only the functional spelling.
- arc42 (semantics Axiom 2.7, engine §3.2) and code comments/docstrings drop colon examples.
- Not changed: colon input stays accepted without a warning; result keys are unchanged.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `aggregations/functional-form`: emission is functional-only; colon equivalence collapses into one legacy-acceptance requirement; examples functional.
- `aggregations/expression-aggregation`: examples → functional spelling.
- `aggregations/formula-templates`: examples → functional spelling.
- `aggregations/native-type-preservation`: examples → functional spelling.
- `aggregations/trailing-window`: examples → functional spelling.
- `mcp/query-tool`: examples → functional spelling.
- `models/column-definitions`: examples and the fanning-error remedy → functional spelling.
- `models/column-filters`: examples → functional spelling.
- `models/column-granularity`: examples → functional spelling.
- `models/join-cardinality`: examples → functional spelling.
- `models/join-traversal`: examples → functional spelling.
- `models/save-validation`: examples → functional spelling.
- `queries/attribution-modes`: examples → functional spelling.
- `queries/computed-dimensions`: examples → functional spelling.
- `queries/cross-model-aggregates`: examples → functional spelling.
- `queries/dotted-dimension-routing`: examples → functional spelling.
- `queries/measure-naming`: examples → functional spelling.
- `queries/partitioned-aggregates`: examples → functional spelling.
- `queries/population`: examples → functional spelling.
- `queries/positions`: examples → functional spelling.
- `queries/saved-measures`: examples → functional spelling.
- `queries/semantics`: examples → functional spelling.
- `queries/time-dimensions`: examples → functional spelling.
- `queries/transforms`: examples → functional spelling.
- `sql/statement-assembly`: examples → functional spelling.

## Impact

- Code: `slayer/core/refs.py` (new renderer), `slayer/core/formula.py`, `slayer/core/errors.py`,
  `slayer/core/models.py`, `slayer/engine/{elaborate_env,param_binding,syntax,query_engine}.py`,
  `slayer/sql/generator.py`, `slayer/dbt/converter.py`, `slayer/cube/converter.py`,
  `slayer/osi/expression.py`, `slayer/mcp/server.py`, plus comment/docstring edits across `slayer/`.
- Saved models produced by a re-import carry functional formulas (already-saved models are untouched;
  both spellings execute identically).
- Docs: `docs/dbt/dbt_import.md`. Architecture: `semantics.arc42.md`, `engine.arc42.md`.
- Overlap: takes over DEV-1956's importer half and DEV-1920's emitter/error-message items; DEV-1831
  (retire `core/formula.py`) is untouched.
