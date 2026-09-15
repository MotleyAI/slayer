# DEV-1859 design — the row-grain branch of the grain-union rule

## Context

See proposal.md for motivation. Substrate facts (verified against HEAD
`cc7bebdb`) that shape the design:

- The parse gate `syntax.py:_validated_agg_source` (~:1103) rejects the mixed
  source (raise ~:1116-1122) and, separately, transforms nested in sources
  (~:1112-1115). It is outside the raise-parity scan; deleting the mixed branch
  needs no ledger edit, and `guards.baseline` (1 = DEV-1878's `source_queries`
  `NotImplementedError`) is untouched.
- Re-aggregation discovery: `is_reaggregation_key` (`core/keys.py:1210`, via
  `operand_aggregates`) classifies ANY source containing an inner aggregate —
  row leaves are not consulted. `_discover_reaggregation_roots`
  (`compile/stages.py:~1590`) replaces the whole root with a placeholder and
  `_synthesize_reaggregation_producer` (~:1700) builds the carrier from attached
  constituents only (~:1723-1727). A mixed root falling through would compile
  with cell-over-cell semantics — silently wrong values.
- Pure-re-aggregation checkers `check_reaggregation_no_window` /
  `check_reaggregation_no_column_param` fire on everything the classifier calls
  a re-aggregation.
- Row attach: `RegroupAttachPlan(attach_phase="row")` +
  `AssociationProducerKernel(null_safe=True)`;
  `_assert_attach_covers_producer_grain` (`stages.py:344`) enforces the
  complete-grain join; `_regroup_inherited_filters` (~:310) threads stratum-0
  filters into every producer.
- The shift-family checker `check_time_shift_input` / `_check_shift_family_key`
  (`elaborate_env.py:~638-670`, called from `bind_inputs.py:~521`) inspects only
  `_SHIFT_FAMILY_OPS`; non-shift transforms over row leaves reach the generator
  and inflate the base GROUP BY (execution-confirmed) — unless the leaf is a
  projected dimension, in which case today's compile is already correct.
- Leg C's substrate (kwarg binding widened to `AggregateKey`,
  `grain_determines` / `check_parameter_determined`, the association level-1
  parameter pick) arrives with DEV-1892 — not yet merged, names planned.

## Goals / Non-Goals

**Goals:** axiom 6's last leg true by construction on the existing row-attach
machinery; the mixed shape structurally unable to reach the DEV-1847 carrier;
the non-shift transform miscompile closed by a consumer-grain-aware type rule.

**Non-Goals:** any change to pure re-aggregation (DEV-1847 semantics, checkers,
goldens); `first`/`last` over expressions; the shift family's regimes;
transform-in-dimension rules (DEV-1868); `source_queries` (DEV-1878);
implementing leg C before DEV-1892 merges.

## Decisions

1. **Classify mixed sources before re-aggregation discovery** (Codex high
   finding). A sibling classifier next to `operand_aggregates` in
   `core/keys.py` detects row-level leaves in an aggregation source; in
   `_plan_regroups`, a root with both attached constituents and row leaves is a
   row-grain source: its inner aggregates register as row-phase attaches
   (computed-dim mechanism) and the root itself stays an ordinary inline
   expression aggregate over the placeholder-rewritten source. Pure roots keep
   the 1847 path bit-for-bit. Alternative — extending
   `_synthesize_reaggregation_producer` with a row-grain carrier mode —
   rejected: the "carrier" would just re-derive the base relation, duplicating
   the regroup row-attach path.
2. **Pure-re-aggregation checkers scope to pure roots** (Codex high finding).
   `check_reaggregation_no_window` / `check_reaggregation_no_column_param`
   consult the same classifier, so mixed roots reach the expression-source
   surface (outer `partition_by=`/`window=`, row-valued parameters) instead of
   the pure-root rejections. Modifier legality is decided by the operand's
   grain type, never by construction — the closure-honest reading of axiom 9.
3. **Leg B is consumer-grain-aware** (Codex medium finding, execution-verified):
   the checker generalizes the `check_time_shift_input` walk to every non-shift
   transform op (derived set: all transform ops − `_SHIFT_FAMILY_OPS`;
   `first`/`last` never reach it, being aggregation-dispatched) and rejects only
   row leaves that are not projected grain keys. `bind_query_inputs` already
   sees the query's projected dimensions and passes them in. Alternative — the
   blanket any-row-leaf rule — rejected: it would break `rank(weight)` with
   `weight` projected, which compiles correctly today.
4. **Leg C rides leg A's attach, parameter-shaped.** After DEV-1892 lands, the
   attached parameter's producer row-attaches into the aggregation's input
   relation (association kernel: level 1, currently sealed by
   `enable_producer_regroups=False` at `stages.py:~1518`; ordinary kernel: the
   base relation via leg A's path), and 1892's `_p<i>` pick carries it to level
   2. The lift removes 1892's typed rejection wholesale — a per-kernel residue
   would be construction-inspecting. NULL grain keys keep the pinned
   null-safe-cell rule.
5. **No new warnings.** The broadcast onto rows is the standard coarser→finer
   coercion; per-row weighting is the shape's meaning (issue doctrine); the
   degenerate-re-aggregation warning does not apply (the mixed value genuinely
   differs from the inner).
6. **Architecture bundle in this PR.** `semantics.arc42.md` axiom-6 tag flips to
   `[enforced: test:…]` (exact one-line diff presented for approval before
   editing — normative harness); no `index.yaml` change; docs per proposal.
7. **`first`/`last` over a mixed source dispatch to the aggregation path**
   (found at test-writing: today a composite-with-agg first arg dispatches to
   the TransformCall, so `first(quantity * avg(...))` dies on the
   time-dimension error, not the expression rule). The dispatch narrows: a
   PURE attached first arg keeps the transform routing (`first(INNER_CR)`),
   a mixed composite routes to the aggregation, where the existing
   binding-level "not supported over an expression" rule fires — no test pins
   the current transform routing of the mixed shape.
8. **Computed-dimension leaves under non-shift transforms: expression-equality
   only** (decided with James at test-writing, 2026-09-14). A transform input
   whose ValueKey EQUALS a projected computed dimension's key is a projected
   grain key: the leg-B checker exempts it, and the generator resolves it
   against the dimension's slot. The plan is already correct (the dimension is
   a projected row slot and the transform layer sits over the same key); only
   `_transform_layer_deps_ready` (sql/generator.py) descends into the input's
   children and dies on the unprojected base column — fix readiness to check
   the whole input key against `slot_id_by_key` first, and the step render to
   read that slot's alias, converting today's RuntimeError leak
   ("transform layer dependencies could not be resolved") into working
   behavior. By-NAME references to computed dimensions as expression leaves
   stay unresolvable — a separate binding feature, out of scope.

## Risks / Trade-offs

- [Mixed root silently falling into the 1847 carrier through some discovery
  path not covered by the classifier] → structural plan tests: mixed root ∉
  re-aggregation roots, constituents present as row-phase attaches, outer slot
  stays an aggregate, no producer GROUP BY contains the placeholder or row leaf
  (Codex finding 5).
- [Row-attach inside a windowed/partitioned outer producer's sub-plan exposes a
  nesting gap in the shape-B machinery] → executed + golden tests for outer
  `window=` and outer `partition_by=` specifically; no new deferral is
  acceptable (ratchet only lowers), so any gap is fixed in this PR.
- [Leg-B derived-set drift as transform ops are added] → the rejection matrix
  parametrizes over the derived set itself and asserts classifier membership
  (Codex finding 4).
- [DEV-1892 lands with different names/shapes than planned] → leg C is gated:
  re-verify against the landed code and the follow-up Linear comment at
  test-writing time; `openspec validate --strict` after each merge-forward
  catches spec drift in the shared expression-aggregation requirement.
- [Custom/multi-input aggregation rendering paths diverge from the probed
  built-ins] → dedicated executed cases (custom aggregation, `corr`) rather
  than inference from `percentile` (Codex finding 6).

## Migration Plan

Pure feature lift plus one new type rule. Previously erroring mixed sources now
execute; previously *miscompiling* non-shift row-leaf transforms now raise the
typed error — a behavioural break only for queries that were returning wrong
numbers. Golden divergences follow the divergence-ledger protocol. Rollback =
revert the PR. Two existing rejection pins re-point with maintainer consent
(recorded in tasks.md).

## Open Questions

None — the c4 NULL-region oracle, the leg-B grain rule, and the modifier
surface were resolved in the plan interview.
