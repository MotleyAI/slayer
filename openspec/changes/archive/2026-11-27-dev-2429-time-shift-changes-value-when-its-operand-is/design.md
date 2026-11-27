## Context

`_series_mode` (`slayer/engine/compile/shift.py`) selects one of two shifted evaluations from the operand's construction:

- **series** (nested transform, cross-model leaf in a composite, top-level predicate): `_series_shift_cte` (`slayer/sql/generator.py`) reads the operand's materialised series off the chain tail, so a source bucket outside the frame is NULL;
- **re-aggregation** (everything else): `_plan_shifted_attaches` (`slayer/engine/compile/stages.py`) synthesizes a frame-free shifted producer (`_frame_free_filters` strips frame bounds) and carries axis-free placeholders at their in-frame value (DEV-1958).

Spine queries already force every `time_shift` onto the producer path (`series=False` override in `_emit_planned`) and move their spine bounds by the shift (`shifted_spine_bounds`, `slayer/engine/compile/spine.py`), so spine and non-spine disagree on `time_shift(cumsum(…))` today (`tests/test_dev2218_spine_rows.py` vs `tests/test_dev1868_series_shift_exec.py`).

Symptom → cause → grounding: an equal rewrite of the operand changes the shifted value (the issue's repro) because the evaluation is chosen by construction (semantics Axiom 9, Axiom 2.7, Law 3) and two code paths must agree by hand (sql P10: one composition primitive for any aggregate needing its own frame).

Principles that apply: semantics Axioms 2.7, 9, 10, 11.1, 11.3 (frame clause added, user-approved), 11.6; Laws 3, 6, 7; sql P9 (fail closed), P10, P12 (attach cardinality-neutral); engine P4 (interning).

## Goals / Non-Goals

**Goals:**
- One shifted evaluation for every `time_shift` operand: the operand evaluated in the query with its frame moved by the shift.
- Delete the series regime from planner, IR and generator; spine and non-spine share the moved-frame bounds.
- Spec, docs and help state the one rule.

**Non-Goals:**
- Translating every other time-valued column, secondary time-dimension coordinate, temporal literal or cohort filter by the shift — DEV-1959.
- Re-evaluating axis-free constituents in the moved frame (DEV-1958 ruling: they stay carried).
- Changing unshifted `cumsum`, `lag`, `lead`, rank family, `consecutive_periods`, `first` / `last`.

## Decisions

1. **Moved frame for every constituent whose grain holds the axis (user).** `time_shift(E, k)` reads `E` evaluated with the frame moved by `k`. A fixed-frame reading (frame-relative constituents NULL outside the frame) was dropped: it leaves the issue's repro unfixed, contradicts the owner ruling ("composite and nested operands change from NULL to the value") and regresses spine results.
2. **Axis-free constituents stay carried at their in-frame value (user, DEV-1958 ruling).** Decided by grain (a type), not construction.
3. **Only the axis frame moves (default).** Frame bounds on the axis's column move; every other filter — including frame bounds on another time dimension — applies unchanged in the shifted evaluation (today non-axis frame bounds are stripped; keeping them matches the host's evaluation). DEV-1959 widens the translation.
4. **The moved frame is whole source buckets (default).** The producer interval runs from the bucket containing (first visible bucket start + offset) to the bucket containing (last visible bucket start + offset), extended to bucket boundaries at the query's bucket granularity (custom granularities included), computed after frame normalization and `whole_periods_only` snapping, with open-ended sides left open and the query's time zone applied as for any frame bound. This follows the ruling's "the source bucket's value" and fixes the spine's partial source bucket for unaligned shifts (today `shifted_spine_bounds` floors by the shift unit).
5. **Moved bounds stay frame bounds in the producer (codex).** They reach the synthesized producer as frame bounds on its time dimension (today `_regroup_producer_prebound` is called with `n_date_range=0`), so trailing windows and nested shifts inside it still read before them.
6. **Producer identity includes the moved frame (default).** Equal moved frames intern to one producer (`change(x)` and `time_shift(x, -1)`; distinct offsets without a frame, as "Two offsets share one shifted evaluation" pins); distinct offsets under a frame read distinct producers.
7. **Every `time_shift` root routes to the shifted producer (codex).** Discovery's non-series exemption (`routes()`), the `series=` emission flag and `_shift_candidates`' `series is False` filter go; hidden, nested, filter-only and order-only shifts all reach `_plan_shifted_attaches`.
8. **arc42 Axiom 11.3 clause (user, approved wording):** "A frame bound selects which buckets are shown; `time_shift(E, k)` reads `E` evaluated with the frame moved by `k`, an axis-free constituent keeping its in-frame value. [target: DEV-2429]" — the tag flips to the new test file in pr-implement.
9. **No other harness edit (default).** No model, view or index change; the spec stays under the cross-cutting `queries` spec.
10. **Cross-model scenario compares against the moved query (codex).** Not against the sum of leaf shifts, which differs under NULL propagation and differing domains.
11. **Nested-shift scenario uses a December row (codex).** `time_shift(time_shift(sum,-1),-1)` at April is NULL (the inner shift has no March cell, Axiom 11.6); the seed adds 2023-12 so February reads December's 20.
12. **"Two offsets share one shifted evaluation" stays as is — rejected (codex).** Codex asked to modify it; its `monthly` query has no frame, so both offsets' moved frames are equal and one relation is still required.

## Risks / Trade-offs

- `time_shift(T, k)` of a frame-relative `T` is no longer "T as shown k buckets earlier": `change(cumsum(x))` at the first visible buckets compares against the moved range's running total. This is the moved-frame rule's intended reading (YTD vs prior YTD).
- Under a frame, each distinct offset compiles its own producer — more CTEs for queries mixing offsets, each bounded (today's producer reads all history).
- Golden SQL for non-spine series shapes changes (the series CTE becomes a producer); spine goldens change only for unaligned shifts.
- A predicate answer projected by a producer must render as a value on strict dialects (Postgres, T-SQL, BigQuery) — covered by golden SQL across the dialect set.
