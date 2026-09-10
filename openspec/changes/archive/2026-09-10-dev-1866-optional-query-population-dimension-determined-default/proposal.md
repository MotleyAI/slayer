# Optional query population: dimension-determined default

## Why

Post-DEV-1836/1840 the query root's only remaining jobs are quantification (which
dimension combinations exist as result rows — the population) and anchoring reference
paths, yet `source_model` is still mandatory on every query. Axiom 12 of the query
algebra (`architecture/semantics.arc42.md`, target DEV-1866) pins the end state: the
population may be omitted and defaults to the smallest dataset determining every
queried dimension, inferred from dimensions and row-level filters only — never
measures.

## What Changes

- `SlayerQuery.source_model` becomes optional (default `None`); explicit values —
  including a bridge model owning none of the queried items — keep byte-identical
  behavior and remain the population override.
- A query omitting `source_model` infers its population: the viable candidate
  (every determination item binds along a provably to-one routed chain) with the
  fewest total routed hops, where determination items are the dimensions plus the
  field refs of field-typed query filters. Ties, empty viable sets, empty
  determination sets, and datasource-scoping failures fail closed with a new typed
  `PopulationInferenceError`.
- `SlayerResponse` gains `population` and `population_inferred`, populated uniformly
  across execute / dry-run / explain / cache paths and the client response model.
- REST and MCP query inputs relax `source_model` to optional end to end.
- `recommend_root_model` aligns to the same selection core: resolved columns are
  determination items; saved measures and aggregation-suffixed items are reported as
  reachability-only attachments; `root_hint` and coverage semantics retained.
- Docs reposition `recommend_root_model` as the explain/coverage surface for the
  default; axiom 12's `[target: DEV-1866]` tag flips to `[enforced:]`.

## Capabilities

### New Capabilities

- `queries/population`: the query population — explicit override semantics, the
  dimension-determined inference default, its fail-closed clauses, response
  reporting, and the aligned root-recommendation surface.

### Modified Capabilities

(none — no existing spec mandates `source_model` or owns the recommendation
selection objective)

## Impact

- `slayer/core/query.py` (optional field), `slayer/core/errors.py` (new error),
  new `slayer/engine/population.py`, `slayer/engine/query_engine.py` (pre-bundle
  inference pass, `_Prepared`/response plumbing, `recommend_root_model`),
  `slayer/api/server.py`, `slayer/mcp/server.py`, `slayer/client/slayer_client.py`.
- Docs: `docs/concepts/queries.md`, `.claude/skills/slayer-query.md`, `CLAUDE.md`
  (one line), `architecture/semantics.arc42.md` tag flip.
- Purely additive: every existing query names `source_model` and keeps
  byte-identical behavior.
