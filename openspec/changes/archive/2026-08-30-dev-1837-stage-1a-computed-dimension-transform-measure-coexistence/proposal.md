# Stage 1a — computed dimension + transform-measure coexistence

## Why

Stage 1 (DEV-1824) established the measure ⇔ dimension symmetry axiom but left a fail-closed guard: a computed dimension over a partitioned aggregate (ROW attach) may not appear in a query that also carries a transform measure (`time_shift` / `change` / `change_pct` / `cumsum` / `lag` / `lead` / `consecutive_periods` / rank-of-measure). That pair is the motivating scenario of the whole migration — an aggregation-derived dimension combined with a time-shift measure — and no later family migration dissolves it: transform measures are steps over the query-grain result, not attaches. Probing also surfaced a live defect: a partitioned measure (combined attach) combined with `time_shift` emits a reserved `__regroup__` placeholder into the shifted CTE — invalid SQL on every database.

## What Changes

- Lift the ROW-attach coexistence guard (`generator.py`) for the `transform_layers`-only case in both render paths: the plain path (row attach + transform chain) and `_render_with_cross_model_plans` (row + combined + transform).
- One step-layer transform-grain rule (DEV-1824 D4 extended, user-approved "Option A"): window-family auto-partition, the `time_shift` shifted-CTE grain, and the `consecutive_periods` grain all include every projected dimension slot — computed, derived-column, and bare row-attach placeholder dims — and exclude time buckets and combined-attach placeholder slots. This changes emitted SQL for existing computed/derived-dim + window-transform queries (divergences batch-approved before re-blessing goldens).
- Fix the placeholder leak: the shifted-CTE re-aggregation never includes an attached partitioned-measure value in its grain, and its WHERE parts render regroup-aware so row-lowered predicates over the computed dimension resolve to the producer column.
- Narrow the remaining guards to windowed/ranked (stage 2), cross-model (stage 3), and CTE-body nesting (stage 4), each with its own exact message; re-point demonstrably stale issue refs only.
- Split top-level AND conjuncts of a single filter string before `classify_regroup_filter` (user-approved D9), so `band == 1 and change(...) > 0` — and equally `band == 1 and status == 'ok'` — route per conjunct instead of raising the DEV-1825 separate-filters directive; mixed OR keeps failing closed.
- Introduce the dimension-family × measure-family compatibility-matrix test (SQLite + DuckDB executed values; still-guarded pairs strict-xfail pointing at their stage issue). The matrix shrinking to empty is the migration's definition of done.

## Capabilities

### New Capabilities

(Both paths are introduced by the in-flight `dev-1824-partition-by-on-aggregations-deferred-shapes-window` change and do not yet exist in the corpus; this change only ADDs requirements, so the two archive cleanly in either order.)

- `queries/computed-dimensions`: coexistence of grain-self-contained computed dimensions with transform measures; the transform-grain rule; filter placement across attach and transform phases; remaining coexistence deferrals fail closed.
- `queries/partitioned-aggregates`: temporal-transform re-aggregation excludes attached partitioned-measure values (the placeholder-leak fix).

### Modified Capabilities

None (corpus is empty; see above).

## Impact

- `slayer/sql/generator.py`: guard rework; transform-chain CTE assembly gains regroup producer entries (plain path) and dependency-preserving prelude entries (cross-model path); shared transform-grain helper; regroup-aware shifted-CTE scopes and WHERE parts.
- `slayer/engine/stage_planner.py`: stale deferral-message refs only.
- Tests: new matrix, oracle fixtures, golden suite; existing golden baselines for computed/derived-dim + window-transform shapes diverge (batch-approved); guard-message tests updated.
- Docs: `docs/architecture/composable-attach.md`, `docs/concepts/formulas.md`.
