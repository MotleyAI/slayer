## Why

Under a date range, `time_shift(sum(amount), -1)` reads the bucket before the range, while the equal operand `sum(amount) + 0 * cumsum(sum(amount))` reads NULL there: the evaluation regime is chosen from how the operand is built (semantics Axiom 9, Axiom 2.7, Law 3). Spine and non-spine queries also disagree on `time_shift(cumsum(…))` today.

## What Changes

- One evaluation for every `time_shift` operand (and so `change` / `change_pct`): `time_shift(E, k)` reads `E` evaluated in the same query with its frame moved by `k`; an axis-free constituent keeps its in-frame value (unchanged DEV-1958 rule).
- Composite operands with a cross-dataset leaf, top-level predicates and nested `time_shift` / `change` / `change_pct` read their source bucket outside the range (NULL today).
- A nested frame-relative transform (`cumsum`, `lag`, `lead`, the rank family, `consecutive_periods`) inside a shifted operand evaluates over the moved frame — e.g. `time_shift(cumsum(x), -1)` at the first visible bucket reads the running total of the moved range (NULL today in non-spine queries; spine queries already do this).
- The moved frame covers exactly the source buckets of the visible buckets, in whole buckets — an unaligned shift over a spine range no longer reads a partial source bucket.
- Unshifted `lag`, `lead`, `cumsum`, `first` / `last` are unchanged.
- The series regime is removed from the planner, IR and SQL generator.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/transforms`: `time_shift` states one rule (the moved frame) instead of two construction-selected regimes; composites over transforms no longer mention a regime.

## Impact

- `slayer/engine/compile/shift.py`, `discovery.py`, `stages.py`, `spine.py` (regime selection, routing, shifted producer, moved-frame bounds).
- `slayer/ir/planned.py` (`ValueSlot.series` removed), `slayer/sql/generator.py` (`_series_shift_cte` removed).
- `architecture/semantics.arc42.md` Axiom 11.3 (frame clause, user-approved).
- Docs: `docs/concepts/expressions.md`, `slayer/memories/help_content/05_time.md`.
- Tests: new executed suite; `tests/test_dev1868_series_shift_exec.py` aligned; golden SQL re-blessed where shifted shapes change.
