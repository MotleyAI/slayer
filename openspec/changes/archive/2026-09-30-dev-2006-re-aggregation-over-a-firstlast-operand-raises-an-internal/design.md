## Context

See proposal.md › Why. Probed on `5581d752`:

- A re-aggregation compiles as producer-over-producer: a carrier at the operand's union grain (`_build_carrier_attach`, `slayer/engine/compile/stages.py`) and an outer association-kernel producer over it. The carrier is compiled by `compile_synthesized`, whose `_producer_nesting_rule` keeps a combined root at exactly the producer grain INLINE, bar two carve-outs (windowed transform inputs; ranked/windowed constituents of a composite answer). The carrier's `last(...)` constituent therefore becomes a plain aggregate slot under a plain kernel → the generator guard in `_build_agg_render_spec_from_planned`; a windowed constituent renders no column → rendered-schema `ValueError`.
- "Needs its own kernel producer" is decided by hand at several sites: discovery `_is_bare_windowed_or_ranked`; `_local_regroup_groups` identity + `_local_regroup_kernel` (local regroup); the target-rooted synthesiser (association → trailing-window → ranked); `_shifted_kernel_kwargs` (own-grain, host-local, kernel root); plus the two nesting carve-outs. The carrier never decides.
- Cross-model ranked operands already work (the target-rooted producer receives a ranked kernel). Associate-mode cross-model ranked/windowed aggregates are refused by `check_association_windowed_ranked` before kernel selection (DEV-1914).
- A throwaway prototype (every ranked/windowed root nests inside the carrier's compile) made every scenario in the spec delta pass on SQLite and DuckDB; all oracle values there are probe-confirmed.

Architecture: sql P10 (own ordering / own frame → own producer), P9 (fail closed); engine P6 (placement from typed plan data); semantics Axioms 6, 9, 13, Law 4.

Sequencing: DEV-1976 (dimension values; `combined_admits`) and DEV-1994 (required `RegroupSubstitution.empty_value`; one attached-value record) merge first. This change adds no `RegroupSubstitution` construction site — carrier constituents nest through `_synthesize_local_regroup`, already stamped by DEV-1994 — and does not touch `combined_admits`.

## Goals / Non-Goals

**Goals:** make "a kernel-requiring aggregate renders inline in a foreign producer" unrepresentable for every synthesiser, present and future — one decision, one nesting rule, no carve-outs.

**Non-Goals:** the association kernel's handling of ranked/windowed picks (DEV-1914); a cross-model re-aggregation grouped by a joined to-many time bucket (DEV-2007); changing any SQL emitted for queries that work today.

## Decisions

### D1 — One predicate
Kernel-requiring ≡ `_windowed_or_ranked_identity(k) is not None` (an `AggregateKey` that is `first`/`last` or carries `window=`). No second spelling of the predicate anywhere.

### D2 — One typed kernel decision
A single function returns a typed decision (`None`, or the kernel kind + the answer key it renders) from explicit inputs: the answer, whether the producer is associate-mode, whether it is windowed (active time key present), and — for the shifted site — own-grain / host-locality / kernel-root availability. Precedence: association → trailing-window → ranked. Each of the three kernel sites (local regroup, cross-model target-rooted, shifted own-grain) calls it BEFORE compiling the producer and builds its kernel from the same decision afterwards, so the answer passed into compilation and the kernel on the attach can never disagree. Rejected: keeping per-site branches and adding a fourth for the carrier (option A/B in the interview) — the bug class recurs with the next synthesiser.

### D3 — The nesting rule is the invariant
`ProducerContext` gains `kernel_answer: Optional[ValueKey]`. `_producer_nesting_rule`: at the producer grain a root nests iff it is kernel-requiring and is not `context.kernel_answer` (or per the existing off-grain rules). The two carve-outs (windowed transform inputs; ranked/windowed composite constituents) are deleted — both are instances. The carrier passes no kernel answer, so every ranked/windowed constituent becomes its own local kernel producer inside it; the existing per-identity grouping keeps distinct ranking keys / windows in distinct producers and structural twins in one.

### D4 — Plan-time invariant, renderer backstop kept
`_Routed` carries the producer's `kernel_answer` (from `_route_producer`; `None` at top level). `_emit_planned` asserts no aggregate key — walked through aggregate slots AND combined-expression slots — is kernel-requiring unless it equals that `kernel_answer`: an internal assertion (a planner bug, not a user error). The generator guard stays unchanged as the SQL node's fail-closed backstop at the `PlannedQuery` boundary (sql P9). Rejected: deleting the renderer guard (a plan built outside `_emit_planned` would render silently wrong).

### D5 — No SQL drift for working queries
The rule subsumes both carve-outs, so every currently-passing query emits byte-identical SQL. Any golden diff is a STOP-and-ask, never a re-bless.

### D6 — Architecture
No principle text changes. Approved: `sql.arc42.md` P10 gains `[enforced: test:tests/test_dev2006_kernel_operands.py]`.

## Risks / Trade-offs

- [A kernel producer nests its own answer → infinite recursion] → `kernel_answer` is set before compile at every kernel site (D2); the plan-structure tests assert exactly one kernel producer per identity.
- [Carve-out deletion changes an existing plan] → D5; the ranked/windowed composite, transform-input, shifted and cross-model suites plus goldens must pass unchanged.
- [Shifted-site eligibility lost in the shared decision] → explicit inputs in D2; regression pins for shifted own-grain vs non-own-grain and cross-model shifted.
- [DEV-1994 merge] → the ranked/windowed nested producers go through the already-stamped local-regroup site; the cross-model `count` scenario asserts DEV-1994's 0.
