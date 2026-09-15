# Design — planner-owned materialisation stage

## Context

See proposal.md — Why. Today `Phase` (ROW / AGGREGATE / POST, key-intrinsic) is the only
placement fact a slot carries, while the emitted pipeline has five relations (host base,
producer CTEs, combined SELECT, transform-chain steps, post wrap). The generator therefore
re-derives slot placement at 31 sites (`walk_value_keys` walks, `isinstance(TransformKey)`
checks, three isolated-set builders, alias-availability Kahn readiness) and assembles the
`_base` column set from four independent sources; the live failures are exactly where two
of those sources disagree. Constraints: byte-identical goldens (DEV-1836 D10 protocol),
engine P9 (user-facing type errors raise in the checker; this change adds only internal
invariants), sql P10/P11, the guard ratchet and raise-parity ledger, the approved arc42
edits (engine P6, sql P11). Plan reviewed by Codex (15 findings; 14 folded, 1 rejected —
the resolutions are the decisions below).

## Goals / Non-Goals

**Goals:** one planner-owned stage + needs-column fact per slot; a plan-time invariant
that makes "rendered before its inputs are materialised" unrepresentable; the generator
partitions by stage with no key-walking routing left; the failing composite class executes.

**Non-Goals:** producer-root discovery walks, producer flags, transform-input checker and
first/last dispatch (DEV-1903); unifying the window / time_shift / consecutive_periods
emitters (different SQL, not different routing); any change to the semantics of an
already-legal shape; DEV-1859 / DEV-1832 content. Frame-bound stripping, producer `_src`
filter placement and correlated semi-joins are orthogonal planner facilities, untouched.

## Decisions

- **D1 — Stage kinds `BASE < PRODUCER < COMBINED < CHAIN(level) < POST`** (frozen
  `Stage(kind, level)` in `slayer/ir/planned.py`, `level` 1-based for CHAIN only).
  PRODUCER = a value supplied by a producer CTE joined at the combined SELECT (combined
  placeholders, producer-answered windowed / ranked aggregates); a row-attach placeholder
  is BASE (joined inside `_base`). Every combined-SELECT expression is thus strictly later
  than its inputs (Codex F5). *Rejected:* a single COMBINED kind — cannot distinguish a
  joined producer column from an expression computed in the same SELECT.
- **D2 — `Phase` stays.** It is the key-intrinsic semantic phase the checker types
  against; stage is plan-assigned (producer-boundness is a planning fact). Two facts, two
  fields; no field is derived from the other.
- **D3 — One staging pass** (`slayer/engine/compile/staging.py`, wired at the end of
  `compile_prebound`, applied to every producer body through the same recursion). Rules:
  row leaf / local aggregate / computed-dimension composite → BASE; placeholder or
  producer-answered aggregate → its attach phase (row → BASE, combined → PRODUCER);
  transform → CHAIN(1 + max level of CHAIN deps) over input + partition keys + time key;
  non-dimension composite and every mask → max(dep stages), any CHAIN dep lifting to POST.
  Dependencies come from the one `_iter_slot_deps` contract (fail-closed on unclassified
  kinds); a materialised computed-dimension slot is dependency-terminal (Codex F7). A cycle
  raises; the planner's toposort straggler fallback is deleted (F9). *Rejected:* staging
  in `slayer/ir` — it consults attach plans and producer kernels (planning facts), so it
  is compilation, not representation.
- **D4 — `needs_column` as consumer necessity** (F3, F6, F10, F11): projected; dep of a
  mask (HAVING renders aggregates by base alias; a hidden local first/last then builds its
  ranked subquery); dep of any strictly-later-stage slot; every host-side join-back key of
  a combined attach; order targets by stage — BASE → hidden column, PRODUCER / COMBINED →
  inline in the ORDER BY, CHAIN / POST → column. Mask-only expressions render as
  predicates, never columns.
- **D5 — Filter placement = f(stage, mask typing)** (F1, F2): BASE + field-typed → base
  WHERE; BASE + measure-typed → HAVING; PRODUCER / COMBINED → combined outer WHERE; CHAIN /
  POST → post wrapper. Windowed-value filters follow the rule (COMBINED) with no POST
  special case (James, 2026-09-15: accept rule + approved divergence); executed values are
  pinned with and without transforms.
- **D6 — time_shift regime is a planner fact** (F4): `series: bool` on the transform slot,
  computed at staging from placeholder→original provenance (cross-model leaf inside a
  composite), nested transform, or boolean-shaped input — exactly today's rule, so local
  partitioned composites keep re-aggregation. The generator's key-walk classifier retires.
- **D7 — Strict validator, one IR dialect** (F8): the `PlannedQuery` model validator
  requires every slot staged and dep-ordered, recursing into every `RegroupAttachPlan`
  producer plan; `MaterialisationStageError(SlayerError, ValueError)` (core/errors.py,
  ledger category `internal`). The generator entry gate stays only as a belt against
  `model_copy` bypass. Hand-built test plans gain `stage=` (approved).
- **D8 — Derived surfaces keep their names.** `_lower_positions` / `_LoweredPositions`,
  `OrderScope` + `_classify_order_scope`, `_unmaterialised_post_slots`, `_emit_step_cte`,
  `_emit_time_shift_ctes_for_planned`, `_run_transform_chain` keep signatures and are
  re-implemented as reads of stage / needs_column (23 test files pin the names). Deleted:
  `_collect_base_aux_slot_ids`, `_composite_has_remote_operand`,
  `_composite_reads_an_isolated_cte`, `_combined_placeholder_slot_ids`,
  `_windowed_agg_slot_ids`, `_transform_layer_deps_ready`, `_classify_ready_transform_layers`,
  the `outer_composite_slot_ids` walk and its DEV-1838 `continue`, `order_only_local_ids`,
  `_add_local_aux_slots` and the outer-composite leaf loop, the chain deadlock RuntimeError,
  `_classify_walk` / `_classify_time_shift_composite` / `_time_shift_series_mode`. `TransformLayer`
  keeps its shape; the level lives on the slot only.
- **D9 — Goldens byte-identical; divergences approved one by one.** The rule is not bent
  to reproduce an accident; each divergence the rule surfaces (windowed-value filter
  placement, order-only composite materialisation, hidden placeholder projection under
  transforms, `host_combined_ids`) is reported with executed-value parity and approved
  individually. A transition parity test (stage-derived base set == legacy collector
  output over the law-harness shapes) lives until the legacy collector is deleted (F14).

## Risks / Trade-offs

- [The rule diverges from today's SQL on a shape nobody listed] → goldens across 19
  baselines + the law harness trip; each divergence is executed-value-checked and approved,
  never re-blessed silently.
- [A producer body or nested producer escapes staging] → the validator recurses into every
  producer plan; the generator belt refuses unstaged plans.
- [`model_copy(update=…)` builds an inconsistent plan after validation] → the generator
  belt plus the alias-exclusive render context (the existing fail-closed backstop).
- [DEV-1859 lands with further review changes] → merge `origin/main` forward at each
  commit boundary; its diff sits in other regions of `stages.py` / `generator.py`.
- [Stacked corpus: DEV-1859's transforms delta archives first] → re-run
  `openspec validate --strict` after that archive; the MODIFIED block here targets the
  cross-model-aggregates requirement DEV-1859 does not touch.

## Migration Plan

Ordered commits, each green with goldens byte-identical (see tasks.md): (1) stage type,
staging pass, validator, belt, error class, fixture updates — no consumer change; (2) base
column set from stage in both render paths (fixes the class); (3) chain levels, post step,
regime from stage; (4) filter / order classifiers and isolated sets from stage; (5) docs,
arc42, ledger, lint, arch_check. Rollback = revert the PR (no storage migrations).
