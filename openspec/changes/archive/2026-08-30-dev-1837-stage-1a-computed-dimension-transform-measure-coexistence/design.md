## Context

See proposal.md — Why. The guard sits at `generator.py:1645`; the plain path already prepares regroup CTEs / env / join specs before branching on `transform_layers`, and the cross-model transform-chain prelude already receives `row_regroup_ctes` (dead code behind the guard). The step layer has two divergent grain rules today: `_render_window_transform_sql` auto-partitions by projected `ColumnKey` dims only (excluding computed/derived dims and, correctly, combined placeholders), while `_emit_time_shift_ctes_for_planned` includes computed dims but leaks combined placeholders into the shifted CTE (probed: `orders.__regroup__0__…` in emitted SQL for a partitioned measure + `time_shift`). Constraints: golden byte-identity except individually approved divergences; fail-closed for everything still deferred; SQLite + DuckDB executed ground truth.

## Goals / Non-Goals

**Goals:** one transform-grain rule shared by the window, time_shift, and consecutive_periods emitters; regroup-aware shifted-CTE rendering (scope AND pre-rendered WHERE parts); dependency-preserving CTE assembly in both chains; the compatibility matrix as the migration's tracked frontier.

**Non-Goals:** windowed/ranked/cross-model coexistence with row attaches (stages 2–3); CTE-body nesting (stage 4); cross-phase producer dedup; transform-family-internal gaps unrelated to attaches (guard + defer + a comment on the target issue).

## Decisions

- **D1 — Option A grain rule (user-approved).** The transform auto-grain is: every projected ROW-phase slot with `slot.is_dimension`, excluding `TimeTruncKey` slots and combined-attach placeholder slots. This includes plain computed dims (`upper(city)`), derived-column dims, and bare row-attach placeholder dims — deliberately changing behavior for existing computed-dim + window-transform queries (today the cumsum sums *across* the computed dim's groups while time_shift partitions by it — incoherent within one query). Divergent goldens are enumerated with before/after SQL and batch-approved before re-blessing. *Rejected:* regroup-only inclusion — zero golden risk but enshrines the incoherence for stages 2–3 to inherit.
- **D2 — Placeholder-role discrimination is structural.** Row vs combined placeholders are told apart via `regroup_attach_plans[*].substitutions[*].placeholder` keyed by `attach_phase` — never by leaf-text prefix matching (Codex F4: the helper keys on `slot.is_dimension` plus these sets, so a projected row-valued non-dimension slot can never become a partition key).
- **D3 — Shifted-CTE regroup-awareness covers both render seams (Codex F2).** `_build_shifted_cte_where_parts` renders predicates to text once, before any emitter scope exists — so it, not just the emitter's `ScopeFrame`, takes `regroup_env` + join specs; a row-lowered predicate over the computed dimension (e.g. `band == 1`) renders against the producer column and the shifted FROM carries the producer LEFT JOIN. Values are identical with or without the predicate in the shifted CTE (the band is per-row deterministic and in the join grain); parity with `base` is kept because it is the cheaper invariant to reason about. A SQL assertion covers the shifted CTE's WHERE explicitly.
- **D4 — CTE assembly preserves `depends_on` (Codex F3).** The plain chain gains the producer CTEs (plus their hoisted internals) as real `CteEntry` objects ahead of `base`, which declares them as dependencies; the cross-model prelude stops flattening to `(name, query)` tuples so a producer's hoisted internals keep their edges. Declaration order masks this today; a transform-root producer with an internal WITH makes it load-bearing.
- **D5 — Guard split, refs narrowed (Codex F1).** The single arm becomes three (windowed/ranked → stage 2; cross-model → stage 3; CTE-body → stage 4), each with its own exact message. Only demonstrably stale DEV-1824 refs are re-pointed (`stage_planner.py:1509`, `:1591`, the two generator arms); guards describing shapes still owned elsewhere keep their refs.
- **D6 — Matrix xfail hybrid (Codex F6).** Guarded cells run a solo-equality body under `xfail(strict=True, raises=NotImplementedError, reason="DEV-18xx: …")` so a lift XPASSes and forces the cell flip; each distinct guard arm's exact message is asserted once in dedicated guard tests, not per cell.
- **D7 — Dialect coverage is generation-smoke, not matrix-wide (Codex F5, reduced).** The flagship lifted shapes get generation-only assertions on `tsql` + `bigquery` + one case-folding dialect: parses, one flat WITH, scope-closed. Full per-dialect goldens for every cell are out of proportion to the acceptance (SQLite + DuckDB executed).
- **D8 — Deferral protocol.** Any cell guarded-and-deferred during implementation gets a strict-xfail ref AND a comment on the issue it defers to describing the exact shape.
- **D9 — General AND split for row-attach filters (user-approved).** Top-level AND conjuncts of one filter string split (via the existing `split_top_level_and`) BEFORE `classify_regroup_filter`, so each conjunct routes to its own phase — for transform operands AND plain row operands alike, aligning the row-attach family with the DEV-1824 D7 combined-attach router. This flips one pinned test (`test_dev1825_regroup_planner.py::test_mixed_and_in_one_filter_raises_directive`, explicitly approved): `band == 1 and status == 'ok'` becomes a working query. Mixed OR keeps the split-the-filter directive. *Rejected:* transform-only narrow split (a special case on a special case) and keeping the directive (leaves the two attach families' filter rules divergent).

## Risks / Trade-offs

- [Option A flips goldens beyond the anticipated computed-dim + window-transform set] → full-suite run enumerates them; nothing re-blessed without batch approval.
- [`is_dimension` disagrees with today's ColumnKey-type test on some projected slot] → golden byte-identity over the existing suite is the tripwire; any flip is investigated, not blessed.
- [The cross-model row+combined+transform path hides more than the prelude wiring] → the matrix's D-band × M-part × transform cells are executed ground truth; breakage is fixed here if transform-related, else guarded + deferred per D8.
- [Shifted-CTE producer join changes shifted values] → producer CTEs aggregate raw rows unfiltered by time bucket, so the attached dimension value is bucket-invariant; oracle tests pin it.

## Migration Plan

Single PR on this branch; guard-message changes and grain-rule change land with their tests. Rollback = revert the PR (no storage or schema migrations). Post-merge: archive this change; comment deferred cells onto DEV-1835 / DEV-1836 / DEV-1838 as they arise (D8).

## Open Questions

None.
