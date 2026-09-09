# Design — optional query population, dimension-determined default

## Context

`_prepare_pipeline` (slayer/engine/query_engine.py) runs
`strip_source_model_prefix()` first, then builds the resolved source bundle (the
single eager storage consult, engine principle 3), then normalizes and substitutes
variables into filters. DEV-1853's oriented walker (`slayer/core/join_walker.py`)
plus `provably_to_one` (`slayer/engine/join_safety.py`) give the determination
primitive; `JoinGraph` stays cardinality-blind. DEV-1856 (short-form auto-routing)
is a parallel branch, not merged here.

## Goals / Non-Goals

**Goals:** the inference semantics in `specs/queries/population/spec.md`,
implemented so the chosen candidate then flows through the untouched pipeline —
identical SQL, data, columns, attributes, and warnings vs the explicit twin (the
population metadata fields are the one expected difference).

**Non-Goals:** aggregate/stage-as-population inference (explicit multi-stage covers
it); a `data_source` query field (the `data_source=` execute argument already
pins scoping); 1:1-tie-equivalence collapse (compatible later refinement);
short-form routing itself (DEV-1856 — whichever lands second adds an interaction
test).

## Decisions

1. **Pre-bundle inference pass** in `_prepare_pipeline`: when `source_model is
   None`, infer and `model_copy` the chosen name onto the query (main and each
   named stage independently) *before* prefix-strip, so everything downstream is
   byte-identical to the explicit twin. This adds one eager storage sweep for
   root-less queries only; the "only storage consult" comment moves accordingly.
   Alternative — inference inside the bundle builder — rejected: prefix-strip has
   already run by then, breaking the identical-twin guarantee.
2. **Viability probe = real binding per candidate.** `slayer/engine/population.py`
   loads the scoped datasource's models once, then for each candidate binds the
   determination items via the production binder (pure in (parsed, scope, bundle),
   principle 3) against a cheap synthetic bundle, and checks `provably_to_one` on
   every oriented hop of each bound path. Parity with execution by construction;
   an `AmbiguousJoinPathError` or bind failure marks the candidate non-viable with
   a recorded reason. Alternative — a parallel lightweight walker probe — rejected
   (Codex: alias/saved-measure/edge-token drift risk). The rule stays parametric
   over the binding relation, so DEV-1856 broadens it for free.
3. **Determination-item extraction.** Dimensions and time dimensions as parsed;
   computed dimensions contribute row-valued refs and `partition_by` refs, never
   refs inside an aggregation node. Filters: substitute the source-independent
   variable layers (query.variables + runtime kwarg — model defaults don't exist
   yet), mask still-unresolved `{var}` with a neutral literal, parse, drop filters
   containing aggregation nodes, and drop refs resolving to saved measures or
   custom aggregations on the scoped models. Model-level filters are candidate
   properties and never participate. Items dedup on resolved identity.
4. **Selection** minimizes total bound-path hops over distinct items; unique
   minimum or `PopulationInferenceError` — one class, structured payload
   (`reason` kind, `candidates`, `datasources`), stable-prefix message via
   `_format_error_message`.
5. **Reporting plumbing**: effective population name + inferred flag travel on
   `_Prepared` and land on `SlayerResponse` in every construction site (execute,
   dry-run, explain, cache store/hit, refresh); the client response model mirrors
   the two fields. Cache keys are unaffected (keyed on prepared SQL/datasource).
6. **`recommend_root_model` shares the selection core** from
   `slayer/engine/population.py`; items classify by resolved entity type (column →
   determination item; saved measure / suffixed → attachment, reachability via the
   existing `JoinGraph` paths). `RootModelRecommendation.item_paths` entries gain
   an attachment marker; `root_hint`, coverage, and warnings keep their shapes.
7. **Surfaces**: `SlayerQuery.source_model: object | None = None` (no version
   bump — pure relaxation, extra="forbid" unaffected); REST request model and MCP
   tool parameter become optional accordingly.

## Risks / Trade-offs

- [Per-candidate binding cost on wide datasources] → candidates × few items, models
  loaded once; root-less queries only. Acceptable; measured in tests via the
  executed suites.
- [DEV-1856 lands second and changes root-less outcomes for short forms] →
  documented interaction; whichever branch merges second adds the
  population-invariance-under-routing test (DEV-1873 continuous thread).
- [Variable-injected column refs invisible to inference] → documented: such refs
  don't participate; binding still resolves them post-selection or errors normally.

## Migration Plan

Purely additive — no storage migration, no version bump, explicit queries
byte-identical. Rollback = revert; no persisted state changes shape.

## Divergences (spec-implement)

- **`recommend_root_model` fully aligned.** Its objective changed from the retired
  cardinality-blind `JoinGraph` min-hops (all items) to the to-one determination
  core (columns only; saved measures / suffixed items are reachability-only
  attachments). ~10 pre-existing recommend tests encoded the old objective and were
  rewritten to the new semantics (approved). Recommend auto-routes to each item's
  owning model (BFS over unambiguous to-one hops, `to_one_reachable`) while
  inference walks the spelled dimension path — both share `provably_to_one`, so the
  two surfaces agree on the winner.
- The MCP surface test was corrected to nest query fields under `query` (the tool
  contract is unchanged; only `source_model` became optional).
- Two Codex findings were scoped deliberately, not "fixed": (a) `recommend_root_model`
  breaks a genuine determination-hop tie advisorily (deterministic name order) rather
  than raising `TIE` like inference — recommend's job is to suggest, not fail — while
  still using the same to-one determination for viability; (b) the MCP *text* surface
  reports the population only when inferred (to avoid changing explicit-query output),
  while the structured `SlayerResponse` / REST / Python client always carry it.
