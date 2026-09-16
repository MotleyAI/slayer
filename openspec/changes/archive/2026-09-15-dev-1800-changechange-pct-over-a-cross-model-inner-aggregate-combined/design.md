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
the resolutions are the decisions below). Amended 2026-09-15 to the leveled derived stage
(D10) after the `origin/main` merge surfaced `last(change(x))`; Codex round 2 on the
amendment (13 findings; 12 folded, 1 rejected) and the DAG framing are recorded in
D10/D11.

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

- **D1 — Stage kinds `BASE < PRODUCER < COMBINED < DERIVED(level)`** (frozen
  `Stage(kind, level)` in `slayer/ir/planned.py`, `level` 1-based for DERIVED only).
  PRODUCER = a value supplied by a producer CTE joined at the combined SELECT (combined
  placeholders, producer-answered windowed / ranked aggregates); a row-attach placeholder
  is BASE (joined inside `_base`) — also when the same value is combined-attached (dual
  role: `_base` is the earliest relation it materialises in). Every combined-SELECT
  expression is thus strictly later than its inputs (Codex F5). DERIVED(level) = a value
  that reads a transform; `level` counts the transform layers beneath it. A transform is
  one more relation layer on top of the transforms it reads, and its output is a dataset
  like any other (semantics axioms 6, 9, 11). A non-dimension composite or mask reading a
  transform is *not* a relation layer: it renders inline wherever it is read — as a
  transform's input, a predicate, a sibling composite — and materialises as a column of
  the trailing derived step only for a consumer that needs one (D4). There is no terminal
  "post" stage: the outer trim / order / limit wrapper is a rendering step, not a value
  stage. *Rejected:* a single COMBINED kind —
  cannot distinguish a joined producer column from an expression computed in the same
  SELECT. *Superseded (D10):* the original `CHAIN(level) < POST` split — a terminal POST
  cannot order a transform over a composite (`last(change(x))`).
- **D2 — `Phase` stays.** It is the key-intrinsic semantic phase the checker types
  against; stage is plan-assigned (producer-boundness is a planning fact). Two facts, two
  fields; no field is derived from the other.
- **D3 — One staging pass** (`slayer/engine/compile/staging.py`, wired at the end of
  `compile_prebound`, applied to every producer body through the same recursion). Rules:
  row leaf / local aggregate / computed-dimension composite → BASE; placeholder or
  producer-answered aggregate → its attach phase (row → BASE, combined → PRODUCER);
  a transform, and any non-dimension composite or mask reading a transform → DERIVED(1 +
  max level of the transforms it reads) over input + partition keys + time key (transform)
  or operands (composite / mask); a non-dimension composite or mask reading no transform →
  max(dep stages): BASE, or COMBINED when any operand is PRODUCER / COMBINED. A windowed
  aggregate, and a host-grain wrap answered by a combined attach, are PRODUCER; the same
  host-grain aggregate as a producer body's own output is that body's BASE.
  One traversal serves stage and `needs_column` (round 2, F1): an interned slot read by
  alias — aggregate, transform, column, computed dimension — is terminal, and an
  aggregate's internals are its own; a non-dimension composite is transparent — traversal
  descends through it whether or not it is interned, so a value's stage is a function of
  its term alone, never of what else the query projects (`last(change(x))` is level 2
  with or without `change(x)` projected). Built on the `_iter_slot_deps` contract
  (fail-closed on unclassified kinds) plus the computed-dimension terminal rule (Codex
  F7). A cycle raises; the planner's toposort straggler fallback is deleted (F9).
  *Rejected:* staging in `slayer/ir` — it consults attach plans and producer kernels
  (planning facts), so it is compilation, not representation.
- **D4 — `needs_column` as consumer necessity** (F3, F6, F10, F11): projected (in
  projection order, keeping a multi-name slot's multiplicity so every alias is emitted);
  dep of a measure-typed mask (HAVING renders aggregates by base alias; a hidden local
  first/last then builds its ranked subquery — a field-typed mask renders inline in WHERE
  and projects nothing); dep of any strictly-later-stage slot under the D3 traversal (a
  computed dimension resolves by its grouped alias, F7; an interned composite read inline
  by a same-level consumer needs no column for it); every host-side join-back key of an attach
  (`join_pairs` names the host key in this plan's coordinates — never the producer's slot
  id); order targets by stage — BASE → hidden column, PRODUCER / COMBINED → inline in the
  ORDER BY, DERIVED → column — except in a raw-rows query, whose order targets resolve
  inline via split emission. Mask-only expressions render as predicates, never columns.
- **D5 — Filter placement = f(stage, mask typing)** (F1, F2): BASE + field-typed → base
  WHERE; BASE + measure-typed → HAVING; PRODUCER / COMBINED → combined outer WHERE; DERIVED
  → the outer wrapper after every derived step. A measure mask applies after all values are
  computed (semantics axiom 14), so a mask's level is a validation and column-necessity
  fact only, never a placement (round 2, F4: `last(change(x))` beside filter `change(x) >
  0` computes `last` over every period, then masks rows). Windowed-value filters follow
  the rule (COMBINED) with no post-wrapper special case (James, 2026-09-15: accept rule +
  approved divergence); executed values are pinned with and without transforms.
- **D6 — time_shift regime is a planner fact** (F4): `series: bool` on the transform slot,
  computed at staging from placeholder→original provenance (cross-model leaf inside a
  composite), nested transform, or boolean-shaped input — exactly today's rule, so local
  partitioned composites keep re-aggregation. The generator's key-walk classifier retires.
- **D7 — Strict validator, one IR dialect** (F8): the `PlannedQuery` model validator
  requires every slot staged and dep-ordered — no value references a later stage, and a
  derived value reading a transform (alias-read) is strictly later than it, checked
  explicitly so a hand-built equal-level plan is rejected (round 2, F6); a composite
  operand renders inline, so its consumer may share its level; a computed-dimension slot is
  dependency-terminal — recursing into every `RegroupAttachPlan`
  producer plan; `MaterialisationStageError(SlayerError, ValueError)` (core/errors.py,
  ledger category `internal`). The generator entry gate stays only as a belt against
  `model_copy` bypass. Hand-built test plans gain `stage=` (approved).
- **D8 — Derived surfaces keep their names.** `_lower_positions` / `_LoweredPositions`,
  `OrderScope` + `_classify_order_scope`, `_unmaterialised_post_slots`, `_emit_step_cte`,
  `_emit_time_shift_ctes_for_planned`, `_run_transform_chain` keep signatures and are
  re-implemented as reads of stage / needs_column (23 test files pin the names):
  `_run_transform_chain` emits transform batches by level ascending (within a level:
  window batch, then time_shift, then cp, in `transform_layers` order — exactly the order
  today's Kahn rounds produce, so transform goldens stay byte-identical), then one trailing
  derived-composite step (`_unmaterialised_post_slots` = DERIVED composites ∧
  `needs_column`, every level fused — sql P11; no transform reads a composite by alias, so
  nothing ever forces an earlier flush), then the wrapper. Deleted:
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
  *Outcome:* the legacy collector was deleted with byte-identical goldens across every
  baseline as the proof, so no separate parity test was written.
- **D10 — One leveled derived stage (amendment, 2026-09-15).** The original `CHAIN(level)
  < POST` split encoded the current emitter's shape (all transform layers, then one
  trailing composite step) as a semantic lattice over values. `last(change(x))` — a
  transform whose operand is a composite over a transform — cannot be staged under it: the
  composite is POST, the transform must be later, and nothing is later than POST. Refusing
  a well-typed term because of *how its operand was constructed* is exactly semantics
  axiom 9's closure violation; axioms 6 and 11 already say a transform's output is an
  aggregate — a dataset like any other. So CHAIN and POST collapse into one kind:
  `DERIVED(level)`, `level = 1 + max(level of the transforms it reads)` for a transform
  and for any non-dimension composite or mask reading a transform (`revenue:sum` BASE →
  `time_shift` 1 → `change` 2 → `last(change)` 2, the transform rendering the composite
  inline from its leaves → `last(change) < 0` 3). Codex round 2 and the D11 framing
  corrected the amendment's first draft, which put `last(change)` one level above `change`
  and emitted composites per level: the window, series time_shift and consecutive_periods emitters all render a
  composite input inline over the chain's aliases and none reads a composite by alias, so
  a composite is transparent to staging (D3), the emitter keeps one trailing composite
  step (D8) and every already-legal shape's SQL is byte-identical by construction; the
  validator checks transform-dep strictness explicitly (D7); mask level is validation-only
  (D5); the P6 target no longer claims `needs_column` derives from stages alone (F10).
  Emission (sql P11) runs base → producers → combined → transform steps by level →
  derived-composite step → outer wrap. The pinned "composite over a transform is POST"
  assertion is re-pointed to "one level above its time_shift"; `last(change(x))` is added
  as a structure case at the composite's level, pinned identical with and without
  `change(x)` projected. *Rejected:* keeping POST terminal and special-casing a transform
  over a composite — reintroduces the shape-inspecting special case this change deletes
  and fails again on the next `f(change(g(x)))`. *Rejected (round 2):* Codex's
  barrier-flush mechanism for per-level composite steps — no transform reads a composite
  by alias, so no flush point exists; and F9's re-ordering of §3.2 after §4 — moot once
  emission order equals today's.

  Target arc42 text (supersedes the 2026-09-15 approved wording; both are normative
  harnesses, so each is re-presented as the exact diff for an explicit OK in §6.2, never
  applied on the strength of this plan):

  `engine.arc42.md` P6 → **Phase and stage are properties of the value**: a value's
  semantic phase (ROW / AGGREGATE / POST) is key-intrinsic, and the planner assigns every
  value in a plan — producer bodies included — exactly one materialisation stage
  (`BASE < PRODUCER < COMBINED < DERIVED(level)`), derived only from the stages of the
  values its term reads — a transform is one relation layer above the transforms it reads
  (`1 + max`); a composite or predicate reading a transform shares that layer and renders
  inline — and a plan referencing a later stage is rejected at plan time. Whether a value
  is projected as a column is a separate consumer fact (projection, a later stage, a
  measure mask, an attach join-back, an order key). WHERE / HAVING / post placement
  derives from stage and mask typing, never from text analysis or key shape.
  `[enforced: test:tests/test_dev1800_materialisation_stage.py]`

  `sql.arc42.md` P11 → **One flat WITH, partitioned by stage**: every statement renders
  through one pipeline (base → producers → combined → transform steps in ascending level →
  derived-composite step → outer wrap) with one allocator; the generator places each
  materialised value in the relation its planner-assigned stage names, and a relation
  reads earlier relations by alias only, never re-deriving placement from a value's shape
  — a composite is not a relation boundary: it renders inline wherever it is read and is a
  column only where a consumer needs one; a producer's internal WITH hoists to the top
  level — a WITH never nests inside a CTE definition; fusion of adjacent phases is an
  emission decision, never semantic. `[review]`

- **D11 — The DAG is the end-state; stage is its emitter-quotient (James, 2026-09-15).**
  The algebra's rational model is a DAG of grain-typed nodes (semantics axioms 6/9/10/11/
  12): every term a node typed by its grain, owning its rows and joins, the population the
  spine the result joins onto, emission a fused bottom-up walk — sql P10 generalised to
  every node, fusion a pure emission decision (law 6). A stage is that DAG quotiented by
  what one SELECT can fuse: crossing to another dataset's rows (PRODUCER) and a window /
  self-join shift (each DERIVED level) are the only barriers; composites and local
  aggregates fuse, so they are not strata — which is why D10's transparent-composite
  levelling is the right one. The lattice is chosen over building the DAG now because
  goldens must stay byte-identical and fusion soundness needs the law harness to grow
  first; centralising placement as one planner fact makes the future DAG walk a
  planner-local swap.

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
column set from stage in both render paths (fixes the class); (3) derived levels — transform
batches by level, one fused composite step — and regime from stage; (4) filter / order
classifiers and isolated sets from stage; (5) docs, arc42, ledger, lint, arch_check.
Rollback = revert the PR (no storage migrations).
