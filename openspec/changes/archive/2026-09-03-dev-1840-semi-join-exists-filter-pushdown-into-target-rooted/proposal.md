# Proposal — DEV-1840 semi-join (EXISTS) filter pushdown into target-rooted producers

## Why

Since DEV-1836, a ROW-phase query-filter conjunct reachable from a target-rooted producer's root only across an unproven/unsafe hop is dropped from that producer — the metric broadcasts its unfiltered value with a warning, and `strict=True` errors. That is honest but not what users mean (Looker/Cube semantics): the metric should be computed over the root rows related to at least one row passing the filter. A correlated EXISTS semi-join delivers exactly that, cardinality-safe by construction, and degenerates to today's inline WHERE on provably many-to-one hops — one uniform semantics with inline as a pure optimization.

## What Changes

- Per-conjunct producer filter disposition becomes three-way: safe-reachable → inline (unchanged); unsafe-but-reachable → **NEW: pushed into the producer as a correlated EXISTS semi-join** along the reverse join path; unreachable → dropped + warned (unchanged; strict still errors).
- Pushed conjuncts group by their first reverse hop (connected join tree): one EXISTS per group, all conjuncts of the group AND-ed inside the same subquery — the same related row (combination) must satisfy all of them.
- Pushability is conservatively scoped: a conjunct whose cross-path refs span multiple join branches, or that mixes root-local refs with cross-path refs under an OR/NOT, stays dropped + warned (never silently wrong; liftable later).
- Reverse-path resolution may invert a stored forward edge (flipping `join_pairs` and the cardinality label) **for EXISTS correlation only** — never for inline/safe classification; ambiguous inversions (several candidate forward edges, no stored reverse edge) stay dropped + warned. Seed of the general bidirectional traversal planned separately (DEV-1853).
- Conjunct classification (safe and pushable alike) resolves the full expanded dependency set of Mode-A `Column.sql` refs, not just declared key paths.
- **BREAKING (deliberate semantics fix):** lenient-mode values change where a filter was being dropped — the metric is now filtered. Pushed filters emit no warning/metadata; `strict=True` stops erroring on them. On ClickHouse < 25.4 these queries now fail closed (correlated EXISTS unsupported) instead of returning broadcast values.
- ClickHouse ≥ 25.4 runs get `allow_experimental_correlated_subqueries=1` attached to planner-emitted EXISTS SQL via a plan-driven finalization step (today only the RLS rewrite attaches it).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/cross-model-aggregates`: "Producer filter inheritance" gains the semi-join disposition (the core change); "Strict mode" narrows the dropped-filter error to genuinely unreachable filters; the value-flip enumeration sentence in "Existing cross-model behavior is preserved where already safe" is updated.

## Impact

- `slayer/engine/stage_planner.py` (`_conjunct_disposition`, `_cross_model_inherited_filters`, producer synthesis), `slayer/engine/planned.py` (new `SemiJoinFilter` IR), `slayer/engine/join_safety.py` (scoped edge inversion for correlation paths), `slayer/engine/query_engine.py` (warning collectors, strict raise, ClickHouse preflight), `slayer/sql/generator.py` (+ render helpers: EXISTS emission in producer bodies), ClickHouse settings finalization (shared with `slayer/sql/session_policy.py`).
- Tests: three DEV-1836 pins updated with consent; new executed-value, planner, golden (Tier-1 dialects), and ClickHouse-gating suites.
- Docs: `docs/architecture/composable-attach.md`, `docs/concepts/queries.md`.
