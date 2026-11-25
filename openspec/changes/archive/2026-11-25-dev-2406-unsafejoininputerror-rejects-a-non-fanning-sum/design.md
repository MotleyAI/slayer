## Context

See proposal.md › Why. On 1.x the elaborator already resolves each aggregation's home
(`home_path_for`, semantics Axiom 2.4), but the compiler's routing — host-rooted local
producer vs target-rooted producer — reads the source anchor at about a dozen sites
(`is_cross_model_agg`, `is_local_partitioned_agg`, `is_local_combined_regroup_ref`,
`combined_kind`, `is_partitioned_consumer`, `crossing_local_root_predicate`, discovery's
bare windowed/ranked check, `_has_inline_population_aggregate`, the cross-model slot
backstop, `home_of`'s anchor fallback). Two code paths must agree by hand, and they
disagree exactly when the home lies below the anchor: the aggregation is routed host-rooted
and input safety refuses the to-many crossing. Main fixed this in `f40d3554e` (route by
resolved home) and `d95e3d49b` (anchor-first homing, D15); this change backports both.

Separately, 1.x parses Mode-B expressions with Python `ast` in two entry points
(`slayer/engine/syntax.py` for filters and Mode-B text, `slayer/core/formula.py` for
measure formulas, which round-trips through `ast.unparse`). `ast` erases parentheses
around a single literal, so `x in ('a')` binds as the bare time-point form `x in 'a'` and
`x in (5)` is refused.

## Goals / Non-Goals

**Goals:**
- One routing rule: every aggregation is routed by its resolved home.
- No regression for any query that executes today.
- Code shapes and names aligned with main, so the forward merge resolves by taking
  main's side.

**Non-Goals:**
- Main's list-element scenarios of the same requirement (list elements read across a
  join are not on 1.x).
- Changing the input-safety error message for still-refused sources (main kept it).
- Main's grammar reading of `timestamp_col in ('2025')` as a value list.

## Decisions

1. **Target 1.x first, then forward-merge into main.** (user)
2. **Route by resolved home — the structural fix**, a backport of main `f40d3554e`: an
   `AggregateHome` (`kind`: `host` / `physical_target` / `operand_dataset` /
   `unresolved`, plus `path`) resolved once in elaboration, carried as `AggregateHomes`
   on the elaborated stage and on each `Aggregate` term (`home_kind`); every
   anchor-reading routing check reads it instead; homes carried through every compiler
   substitution that rebuilds an aggregate (redundant-partition strip, re-aggregation
   substitution, routed prebound rewrite); `home_of` reads the homes with no anchor
   fallback. Alternative rejected: patching the source-crossing check to re-root the one
   reported shape — it leaves the two code paths to disagree for every other producer
   kind. (default; structural bias, aligns with main)
3. **`target_rooted` is main's predicate exactly**: false for a host-locus wrap or a
   re-aggregation; otherwise true when the home kind is `physical_target` **or** the
   source anchor is non-empty — so an `unresolved` aggregation with a cross-model anchor
   keeps target-rooted routing and input safety judges it from its anchor. (codex)
4. **Anchor-first homing (D15)**, a backport of main `d95e3d49b`: candidates try the
   source anchor first, then deepest-first. Without it, routing by home would move an
   aggregation with a one-to-one input off its anchor and could drop unmatched anchor
   rows. (default)
5. **arc42 `semantics.arc42.md`** (user, approved wording) — Axiom 2.4: "An aggregation
   over a row-level source is homed on its source anchor (2.6) when the anchor determines
   the source's home (2.2) and every column-valued parameter and non-overridden
   definition default, else on the deepest dataset that determines them all — each a
   row-level expression"; Axiom 2.6: "the anchor is tried first, then the other
   candidates deepest-first."
6. **Parenthesised single literal after `in` / `not in`: option B'.** The parser keeps
   the parentheses; a parenthesised single literal reads as `(v,)`, except a string
   against a DATE/TIMESTAMP operand, which keeps period membership; bare `x in 'v'`
   unchanged. Alternatives: A — match main exactly (breaks the working
   `ts in ('2025')` period reading on a patch line); B — no parser change, any
   single-string `in` on a non-temporal operand becomes a value list (accepts
   `state in 'v'`, which main refuses). (user)
7. **The parentheses are captured from the source text before `ast.parse` in
   `engine/syntax.py`** — the parser every query filter and measure goes through — and
   carried on the parsed comparison node; `core/formula.py` only checks that converter
   formulas parse and its output is never compiled, so it is unchanged. (codex; amended in
   pr-implement)
8. **The string case is decided at elaboration, where the operand's type is known**: a
   parenthesised single string binds as a time-point comparison flagged
   `parenthesised`; elaboration keeps period membership for a temporal operand and
   lowers it to a one-element value-list membership for a non-temporal one instead of
   raising. A parenthesised single number or boolean becomes `(v,)` at parse time. (codex)
9. **A parenthesised single number or boolean is covered too** — same lost-parentheses
   cause. (default; structural)
10. **Main's list-element scenarios omitted** — they belong to list elements read across
    a join, absent on 1.x. (default)
11. **The input-safety message for still-refused sources is unchanged** — main kept it;
    the issue's own case is now accepted. (default)
12. **New tests are portable to main** — dict models saved through storage, `count(*)`,
    plain type names, executed-value assertions (main deduplicates the parent with
    `ROW_NUMBER`, so SQL text differs); the 1.x-only timestamp-parentheses check sits in
    its own test, flipped or dropped at the forward merge. (default)
13. **Existing tests pinning the old refusals are aligned to main's versions**, with the
    owner's approval asked in pr-tests: `test_dev1750_execution::TestSiblingProtectionUnderFanOut::test_one_to_many_fragment_fails_closed`,
    `test_dev1900_home_path::TestReverseHopDefaultFailsClosed`,
    `test_dev1908_default_cancellation::TestFailClosed::test_reverse_hop_not_on_the_path_stays_refused`.
    (default)

## Risks / Trade-offs

- [A producer kind that main fixed in a later commit misbehaves when homed below the
  root on 1.x] → the "every producer kind" scenario is tested per kind from both roots;
  a failure is ported from main's corresponding fix.
- [An aggregation whose home changes under D15 (one-to-one inputs) reads its grain or
  attribution from a different dataset] → anchor and deeper dataset determine each other
  over the one-to-one hop; the to-one scenario and the existing home suites pin it.
- [Forward-merge conflicts] → every code conflict resolves to main's side; the arc42
  hunks are identical to main's and merge cleanly; the portable test file lands as main's
  regression test.

## Migration Plan

Patch release on 1.x; no stored-data or API change. Rollback is a revert.
