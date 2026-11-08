# Design — DEV-1846 composite-input transforms

## Context

See proposal.md — Why. Load-bearing current state: the binder puts any Mode-B
key on `TransformKey.input` (`binding.py:1133`); the planner materialises each
aggregate leaf of a composite as a hidden base slot but never the composite
(`planning.py:646`); window transforms already render composite inputs inline
over chain aliases (`generator.py:5975`); the shifted CTE reads raw source rows,
so it alone needs re-aggregation. The base SELECT already renders
AGGREGATE-phase composite measures via `render_value_key` +
`CompositeFacilities(agg_builder=…)` (`generator.py:3278`) — that seam is the
reuse door. Kahn readiness (`_transform_layer_deps_ready`) already walks
composite inputs recursively.

## Goals / Non-Goals

**Goals:** one render path per emitter (single-leaf = N=1 of the composite
path); one validation gate shared by all render paths; guard inventory left
accurate (no unreachable arms, no stale stage markers).

**Non-Goals:** Mode-B type inference; cross-model aggregate leaves inside
time_shift composites (fail-closed); nested transforms inside time_shift
(would need a chain-reading shifted design); the `7b.10+` row-phase/filter-phase
markers (different family); windowed-measure × transform combinations.

## Decisions

**D1 — time_shift composites render by emitter recomposition, not planner
pushdown.** Rewriting `time_shift(f(leaves))` to `f(time_shift(leaf)…)` was
rejected: it changes semantics for NULL-absorbing wrappers (`coalesce` would
turn a missing bucket into 0 instead of NULL), is invalid for row-level leaves,
and emits one shifted/sjoin CTE pair per leaf instead of one total. The emitter
renders the composite via `render_value_key` with a shifted leaf-agg builder
(`CompositeFacilities.agg_builder`): per leaf, the existing DEV-1750
registration block (joins, fragment kwargs, column filters through
`shifted_scope`) + synth spec + `_build_agg` + leaf-slot cast. A bare
`AggregateKey` input goes through the same door as N=1 — existing single-leaf
SQL must stay byte-identical (any movement is an enumerated class-(b) golden
re-bless). Composite outputs project under an allocated internal alias (no base
slot exists); per-leaf casts mirror how the outer recomposition consumes
per-leaf-cast base aliases, so no whole-composite cast.

**D2 — composite leaves must be local aggregates.** A path-bearing leaf inside
a time_shift composite fails closed: the base renders such leaves through
kernel routing, the shifted CTE would re-aggregate host-rooted, and value
parity between the two cannot be guaranteed. Crossing fragment kwargs / params
/ derived sources on local leaves keep working (registration per leaf).

**D3 — consecutive_periods unifies on one alias-context render.**
`render_value_key(key=input, ctx=_alias_render_ctx(…))` for every shape; the
current leaf / comparison special cases collapse. Wrap by the recursive
boolean-shaped contract (spec: predicate typing contract) — chosen over
per-function argument contracts for all of Mode-B (out of scope) and over
silent emission (Postgres rejects bool-vs-int, and text truthiness is
undefined, hence the string-family rejection via the existing
`SCALAR_PASSTHROUGH` families). Codex review R1/R3.

**D4 — one hoisted validation gate.** The input-shape validator moves above
the kernel-body / combined-attaches early returns in `generate_from_planned`
and is renamed `_validate_transform_input_shapes`; the dead `deferred`-set walk
and its two unreachable raises are deleted. Emitter guards become
`RuntimeError` invariants. Alternative (planner-side validation) rejected: the
generator entry is the one choke point shared by prebound-key callers too.

**D5 — partition-kind arm deleted as proven unreachable.** `partition_by` is
binder-rejected on non-rank transforms (DEV-1739) and rank never routes through
the time_shift emitter, so `TransformKey.partition_keys` is always empty there;
auto-partitions are query-dimension slots — exactly the five kinds already
handled. The arm becomes a descriptive `RuntimeError` (naming the offending
kind), and the dead explicit-`partition_keys` loop plus stale C6 comments
(including `binding.py:1099`) are removed. Kept as RuntimeError rather than a
user error because no user input can reach it (Codex R5: message stays
descriptive).

**D6 — window-dispatch fallthrough is a total-dispatch backstop.** The 13-op
vocabulary minus desugared `change`/`change_pct` minus the two dedicated
emitters equals the 9 dispatch arms. Kept as `NotImplementedError`, reworded
without stage markers; the dev1838 sweep allowlist narrows to match.

**D7 — planner dep-walk completeness.** `_iter_slot_deps` gains
`BetweenKey`/`InKey` recursion wherever they nest (Codex R2), mirroring the
generator's readiness walk, so nested predicate columns materialise.

## Risks / Trade-offs

- [N=1 unification moves single-leaf SQL] → golden suite across 5 dialects;
  any movement enumerated in divergences.md as class (b), values unchanged.
- [Boolean contract too strict somewhere] → contract is fail-closed; loosening
  later is compatible. `iif` condition position is the one predicate-consuming
  seat kept open.
- [Truthiness on non-numeric aggregates (`cp(name:max)`)] → pre-existing leaf
  behavior, unchanged; only composites gained the string-family gate.
- [Stacked on DEV-1838 (PR #352)] → integrate forward by merging only.

## Migration Plan

Pure generator/planner change behind existing query surface; no storage or API
migration. Rollback = revert the PR. Goldens re-blessed per the divergence
ledger (divergences.md in this change folder).
