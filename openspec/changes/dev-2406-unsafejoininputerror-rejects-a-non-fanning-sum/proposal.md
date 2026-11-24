## Why

On 1.x, an aggregation whose home is a dataset below the query root — e.g. a sum over a to-many child whose host column is read only per row, as in `sum(iif(<host column> in ('a', 'b'), <child>.amount, 0))` — is refused with the input-safety error, although its home determines every input and nothing fans out; the same measure rooted at the child works. Separately, a parenthesised single literal after `in` / `not in` (`status in ('a')`, `id in (5)`) is refused because the formula parser drops the parentheses. Both are fixed on main; this backports the fixes to the 1.x line, to be forward-merged into main afterwards.

## What Changes

- The compiler decides where an aggregation is computed from its **resolved home** (Axiom 2), never from the longest common prefix of its source leaves: one home-reading routing predicate replaces every anchor-reading routing check, and homes survive every compiler substitution that rebuilds an aggregate.
- Home resolution tries the source anchor first, then the other candidates deepest-first (main's D15), so an aggregation whose anchor determines every input stays where it is computed today.
- Aggregations previously refused although a dataset below the root determines all their inputs now compute at that dataset, with the same value as when the query is rooted there.
- A parenthesised single literal after `in` / `not in` reads as a one-element value list, except a string against a DATE/TIMESTAMP operand, which keeps period membership; a bare `x in '<string>'` is unchanged.
- `architecture/semantics.arc42.md` Axioms 2.4 and 2.6 adopt main's anchor-first wording; docs updated.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/semantics`: the home-candidate order becomes anchor-first; a reverse hop to a dataset not on the path homes below the root instead of being refused; new requirement that the home, not the query root, decides where an aggregation is computed.
- `queries/time-points`: a parenthesised single literal after `in` / `not in` is a one-element value list unless it is a string against a temporal operand.

## Impact

- Code: `slayer/engine/home.py`, `slayer/ir/terms.py`, `slayer/ir/elaborated.py`, `slayer/engine/elaborate.py`, `slayer/engine/elaborate_env.py`, `slayer/engine/join_safety.py`, `slayer/engine/compile/discovery.py`, `slayer/engine/compile/stages.py`, `slayer/core/keys.py`; `slayer/engine/syntax.py`, `slayer/core/formula.py`, `slayer/engine/binding.py`.
- Behaviour: only queries refused today change (they now compute); every query that executes today keeps its result, except that an aggregation with a one-to-one input now homes on its anchor rather than the deeper dataset (same rows, unmatched anchor rows kept).
- Docs: `docs/concepts/queries.md`, `docs/concepts/formulas.md`, `docs/concepts/time.md`.
- Forward merge into main: every code conflict resolves to main's side (main already carries both fixes in its own form).
