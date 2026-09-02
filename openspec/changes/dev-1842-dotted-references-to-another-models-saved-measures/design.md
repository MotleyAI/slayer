# Design — DEV-1842 dotted saved-measure references

## Context

See proposal.md — Why. Today: bare saved measures resolve via the pre-bind `expand_model_measures` pass (`slayer/engine/measure_expansion.py`), invoked from exactly two `stage_planner.py` call sites (measure formulas ~4370, computed-dimension expressions ~4262); the binder (`slayer/engine/binding.py`) detects measures only to emit errors. `_resolve_dotted` already walks the join chain to the terminal model and looks up the leaf as a column only. `_resolve_ref` consults `alias_map` before columns (selected-measure names win in filters). `ResolvedSourceBundle` holds the transitive join closure, so target-reachable models are always loaded. `reroot_value_key` (`slayer/core/keys.py:880`) is the total, fail-closed strip-direction re-anchoring visitor; `column_filter_key` is owner-anchored and invariant under it. `ModelExtension` overlays merge extension measures into the model pre-bind and are re-applied after query-backed expansion; `_expand_query_backed_model` drops directly-declared measures. The user approved: full unification (option C), test migration for the pass's unit suites, round-trip rejection, and fail-closed query-backed validation. Codex plan review produced 9 findings, all folded (alias precedence, dotted alias/order coverage, extension overlay re-apply, query-backed fail-closed, metadata correction, per-edge eligibility, column-filter executed coverage, reroot structural tests, raw-row detector parity).

## Goals / Non-Goals

**Goals:** one measure-resolution authority (the binder); one re-anchoring authority (`keys.py`); dotted references bound-tree-identical to hand-expanded formulas so DEV-1836 semantics are inherited, never re-implemented; `measure_expansion.py` deleted.

**Non-Goals:** importer-side dotted measure support (Cube cross-cube members); lifting `ModelMeasure.description` into result metadata (not done for bare today — parity kept); preserving directly-declared measures through query-backed expansion (fail-closed instead, user decision); lifting the round-trip restriction; broadcast-warning naming under the composite public name.

## Decisions

- **D1 — Binder as the single resolution authority.** `_resolve_ref`: alias_map → column → saved measure (inline: `parse_expr(formula)` → `_bind` at the same scope; recursion resolves nesting). `_resolve_dotted`: alias_map on the full dotted text → join walk (existing) → leaf column → saved measure (bind the formula against `ModelScope(source_model=terminal)`, same bundle, then prepend-reroot into host coordinates). Measure lookup is last-wins over `model.measures` purely as internal defense; it never observes same-name duplicates in practice, because `ResolvedSourceBundle` re-validates the overlaid model and the DEV-1443 duplicate-name guard rejects a `ModelExtension` measure reusing a model measure's name (a loud error, not a silent shadow — user decision, correcting the earlier "model_copy bypasses the validator" premise). *Rejected:* extending the pre-bind pass with dotted support — keeps two resolution layers and needs a second ParsedExpr-level re-anchoring walker; *rejected:* dotted-only in the binder with bare names still pre-bind — same two-layer drift risk (the shape DEV-1836 D1 refused).
- **D2 — Eligibility as an explicit bind context, consumed per edge.** A context (threaded like `in_filter`) carries measure-eligibility plus the (model, measure) expansion chain for cycle/depth detection. Only the two eligible call sites enable it; `_bind_agg` (source/args/kwargs), `partition_by` binding, and transform scalar-arg paths explicitly drop it, each edge pinned by a test. *Rejected:* a bare boolean silently inherited by every recursive `_bind` — a missed edge would legalize measures inside aggregation-level positions.
- **D3 — One generic path-map visitor in `keys.py`.** `reroot_value_key` refactors into a total visitor parameterized by the per-path transform; strip (existing semantics, byte-identical — `SqlExprKey` reconstruction, `None`-vs-empty partition sets, `column_filter_key` skip all preserved) and prepend become thin wrappers. Direct structural/hash equality tests cover every `ValueKey` variant in both directions plus fail-closed `TypeError`. *Rejected:* a hand-written second visitor for prepend — the drift §5.4 exists to prevent.
- **D4 — Round trips rejected post-prepend.** Every re-anchored join path is validated against the host join graph with the no-revisit rule (`_walk_join_chain` semantics); violation errors naming the saved measure and the revisited model. Parity with the hand-written spelling; lifting later is backwards-compatible.
- **D5 — Query-backed fail-closed at validation.** `SlayerModel` with non-empty `source_queries` and non-empty `measures` is rejected at model validation (message: declare in the backing query's final stage, or via `ModelExtension`). Extension-supplied measures survive (overlay re-applied after expansion) and are tested bare + dotted. *Rejected:* preserving measures on the virtual model — needs projection-survival design, and the silent drop was never a working feature.
- **D6 — Naming/metadata mirrors bare rules.** Implicit name = the dotted text (DEV-1713 extended) → key `orders.customers.aov`; `canonical_alias` retained (DEV-1443); `_saved_model_measure_type` / `_bare_saved_measure_name` gain dotted awareness via one shared resolver; format/description keep deriving from the bound tree (identical to hand-expanded — `ModelMeasure` has no `format` field, and its `description` is not lifted for bare today). Raw-row measure detector (`stage_planner.py:~607`) extends its `DottedRef` case through the join graph for message parity.

## Risks / Trade-offs

- [Binder-side inlining subtly diverges from the pre-bind pass] → the migrated unit suites preserve every eligibility/cycle/depth scenario; equality suites pin SQL bytes; existing suites (`test_named_measures.py` etc.) run unchanged; goldens are the tripwire for non-dotted queries.
- [D3 refactor perturbs reroot identity/interning] → structural + hash equality tests per `ValueKey` variant in both directions before any consumer changes; goldens byte-identical.
- [Query-backed validation breaks stored models carrying dead measures] → loud beats silent (user-approved BREAKING); the error names the remedy; such measures never took effect.
- [Deep expansion chains slow binding] → depth cap (default 32) unchanged; formulas are short; no new parse layers versus today.

## Migration Plan

One PR on this branch, tests-first; ordered commits: (0) D3 keys.py refactor + structural suite (no behavior change) → (1) binder resolution context + bare-name cutover + pass deletion + test migration (behavior-identical) → (2) dotted resolution + prepend + round-trip guard + errors → (3) D5 validation + naming/metadata + raw-row detector → (4) docs. Rollback = revert the PR (no storage migrations).

## Open Questions

None.
