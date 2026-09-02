# Proposal — DEV-1838 stage 4: node discipline

## Why

After stage 3 every isolation family CAN ride the regroup primitive, but three legacy attach mechanisms still live beside it (host-rooted `CrossModelAggregatePlan`, `RankedAggregatePlan`, `WindowedAggregatePlan`), the renderer keeps five dispatch arms, two duplicated Kahn transform chains, and per-arm base construction, a producer needed by two scopes materializes twice, and three CTE-body coexistence guards remain. The closure axiom — any grain-legal dimension composes with any legal measure — holds by test coverage today, not by construction; each new feature pair stays an O(N×M) wiring task until every feature is an attach onto a node or a step over a node.

## What Changes

- **Producer interning.** A per-query registry keyed by the complete structural producer spec (body, root, grain, kernel, inherited filters/rewrites) makes a producer needed by several scopes one shared node rendering as one CTE; the DEV-1835 band×wm duplicate-CTE case collapses by construction.
- **Full family unification.** Plain windowed, host-route `first`/`last`, and host-rooted cross-model shapes (crossing inputs, `grain="host"` wraps, filtered-local) migrate onto regroup producers carrying an explicit typed producer kernel (plain | ranked | trailing-window). **BREAKING (internal only):** `CrossModelAggregatePlan`, `WindowedAggregatePlan`, `RankedAggregatePlan`, `classify_isolation`, `IsolationKind`, and `cross_model_planner.py` are deleted; `may_inline_crossing_inputs` relocates to `join_safety.py`.
- **Uniform per-role crossing-input safety.** The stage-3 safety predicate applies to every producer: a crossed *predicate* over an unproven hop on a host-rooted producer becomes a hard error naming hop + remedy; a host-grain aggregate's crossing *source path* (aggregation defined over the join result) stays legal.
- **Node discipline.** One render pipeline — base → aggregate → combined → steps → post, each node consuming only the previous node's schema — with deterministic emission fusion preserving today's fast-path SQL; the two Kahn step chains merge into one driver (subsumes DEV-1799); the collapsed-CTE pre-dispatch arms are deleted.
- **CTE-body deferrals lift.** Row attaches, combined attaches, and re-rooted ranked sub-plans nested where the plan renders as a CTE body compile via the CTE-hoist and execute correctly; the guard list is empty.
- **No executed-value changes** (divergence class (c) empty); SQL-shape changes re-blessed in enumerated batches; new errors enumerated per input role.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/computed-dimensions`: the CTE-body coexistence deferral requirement flips to supported behavior (nested attaches render via the hoist).
- `queries/partitioned-aggregates`: a shared-producer materialization requirement is added (one CTE per distinct producer, no merge across differing producer context); the row/combined coexistence requirement records the keyless-grain cross-model dual-role exclusion.
- `queries/cross-model-aggregates`: the unsafe-input rule extends to host-rooted producers with a per-input-role decision table; the expression/dimension composition requirement records the same keyless-grain dual-role exclusion. NOTE: this capability enters `openspec/specs/` when DEV-1836 archives, which precedes this change's archive by PR stacking; the delta here is written against that post-1836 corpus.

## Impact

- `slayer/engine`: `stage_planner.py` (classifier loop removed, interning registry, kernel synthesis), `planned.py` (plan classes/fields deleted, kernel + identity on `RegroupAttachPlan`), `isolation.py` deleted, `cross_model_planner.py` deleted, `ranked_planner.py` reduced to producer-kernel helpers, `join_safety.py` (seam relocation), `response_meta.py` / `warnings.py` / `query_engine.py` (plan-walk updates).
- `slayer/sql`: `generator.py` (single node pipeline, one Kahn driver, legacy arms deleted), `render/ranked.py` (kernel emission), `stage_wrapper.py` untouched.
- Tests: suite dispositions enumerated in `tasks.md`; golden corpus re-blessed in enumerated class-(b) batches; new fixtures `tests/_dev1838_fixtures.py` + `tests/golden/dev1838_sql_baseline.json`.
- Docs: `docs/architecture/composable-attach.md` stage-4 section + roadmap. No REST/MCP surface change (error-surface additions only).
