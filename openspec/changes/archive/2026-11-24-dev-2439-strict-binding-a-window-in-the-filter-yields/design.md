## Context

See proposal.md (Why). `_reject_over` / `_OVER` sit in `_PRIMARY` in `slayer/core/syntax.py`, and raise `IllegalWindowInFilterError` as soon as `OVER (` is seen. `read_filter` (`slayer/core/ossie_conformance.py`) catches that and returns `window=True` with no tree. `legacy_spans`, `nonossie_spellings` and `call_spans` swallow the same exception, so `_text_problem` sees nothing. `ossie_l3._bound` falls back to the unbound text, because `bind_variables` parses the template natively. `_read` is `lru_cache`d per text.

**Structural cause:** "SLayer cannot evaluate a window" is enforced as "the grammar cannot read a window", and precedence is judged from an exception instead of a tree. The fix gives the strict reader a tree. Row 7 is then a judgment on the tree like rows 1, 3 and 6, which fixes every instance of the class: an incomplete window, a name inside a window, and a construct beside a window.

**Principles that apply:**
- `system.arc42.md` §3.8, one reader of Mode B: windows are read only in `core.syntax`.
- `python.core.arc42.md` §3.3, traversal is total: the new node kinds are `_BaseNode`s, and `walk_parsed` and the generic rewriters cover them.
- `python.core.arc42.md` §3.4, one kind per meaning: a window call is a new kind, not a bent `AggCall`.
- `python.core.arc42.md` §3.5, typed errors: strict refusals stay `OssieQueryError`, native stays `IllegalWindowInFilterError`.

No arc42 or `.c4` edit. The spec attaches to `queries/ossie-l3-binding`, which is already mapped.

## Goals / Non-Goals

**Goals:**
- The strict reader reads Ossie Layer 1 windows into a tree, and §8 precedence holds for every windowed filter.
- One aggregate judgment, with Ossie's argument shapes, for plain and windowed aggregates.

**Non-Goals:**
- Evaluating windows. The native reading keeps refusing them; DEV-2282 adds native `OVER`.
- `SUM(DISTINCT …)`, `PERCENTILE_DISC`, and other Ossie aggregate forms SLayer does not read yet. Inside a window they stay row 1, as outside one (DEV-2282).
- Named windows (`OVER w`), `GROUPS` frames, `EXCLUDE`, and a "ranking needs ORDER BY" check.
- Updating DEV-2220's compliance statement. That issue lists this deviation, and it goes away when this change lands.

## Decisions

1. **Strict-only window reading; the native refusal is unchanged; the cache is split per mode.** (default, from the brief.) The mode is set on `_STATE` after `reset` and under `_PARSE_LOCK`, and `reset` turns it off. `OVER` followed by `(` keeps its parse action: with the mode off it raises `IllegalWindowInFilterError` at the same point as today, before the clause is read. `_read` is cached on `(text, windows)`.
2. **The window clause is read inside `core.syntax`'s grammar, not by a pre-pass.** (default, system §3.8.)
3. **The mode is a keyword-only `windows: bool = False`** on `parse_expr`, `parse_template`, `parse_bound_expr`, `bind_variables`, `legacy_spans`, `nonossie_spellings` and `call_spans`. `reference_spans` and `literal_comparisons` stay native. (default.) The alternative, a parallel strict API, would duplicate the entry points.
4. **New node kinds:**
   - `WindowCall`: `function`, `partition`, `order`, `frame`;
   - `OrderItem`: value, `ASC`/`DESC`, `NULLS FIRST`/`LAST`;
   - `WindowFrame`: `ROWS` or `RANGE`, plus start and end bounds.

   (default, core §3.4.) `OVER` comes last in a primary: after an optional `WITHIN GROUP` and `FILTER (WHERE …)`, and at most once. A `FILTER` after `OVER`, a second `OVER`, or `OVER` after a non-call is a syntax problem.
5. **A window function's callee.** (codex; user, resolving Codex findings 2 and 4.)
   - Ossie's ranking and offset names (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`) stay an undispatched named call. Otherwise they would become SLayer transforms.
   - Every other callee is dispatched exactly as outside a window, including `WITHIN GROUP`'s `_ordered_set`. The result is wrapped in `function`.
   - So windowed aggregates share the one aggregate reading (`DISTINCT`, `COUNT(*)`, `WITHIN GROUP`, `FILTER`), and follow DEV-2282 automatically.
   - An unknown name raises `UnknownFunctionError`, a `ValueError`, so row 1.
6. **A function outside Ossie's window functions is row 1, not admitted.** (default, Ossie decides: Layer 3 §3.4 says the filter is "one Layer 1 boolean expression", and Layer 1 lists its window functions in `expression_language.md` "Window Functions".) The issue's "resolves leniently" means "not refused as an unknown SLayer function". Arities: ranking functions 0, except `NTILE` 1; `LAG`/`LEAD` 1–3; `FIRST_VALUE`/`LAST_VALUE` 1; `NTH_VALUE` 2.
7. **Frames are ANSI `ROWS` / `RANGE`, with `BETWEEN b AND b` or a single start bound.** Bounds are `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING` and `UNBOUNDED FOLLOWING`, with `n` a whole-number literal, in ANSI order: start ≤ end, the start is not `UNBOUNDED FOLLOWING`, the end is not `UNBOUNDED PRECEDING`, and a single bound is not a `FOLLOWING` one. Out of order is row 1. (default.) **Rejected (codex) Finding 3**, which asked to restrict frames to Ossie's three listed forms: Ossie's own example `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` (`expression_language.md` "Window Aggregations") is not among them, so the list is illustrative. (user)
8. **A window nested in a window's function arguments, partition or order is row 1** (ANSI). (default.)
9. **Aggregates and windows across rows:**
   - a windowed aggregate does not mark `aggregate`, so it is row 7;
   - a plain aggregate or a metric reference inside a window marks row 6;
   - references inside a window are collected, so row 3 resolves them.

   (default, §8 order.)
10. **Ossie aggregate argument shapes, in the one `_aggregate` judgment, plain and windowed:** one value; `COUNT(*)`; `APPROX_PERCENTILE(x, p)`; the rewritten `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)`. Others are row 1. (user, resolving Codex finding 1 as in scope. This also moves plain `SUM(a, b) > 1` from row 6 to row 1.)
11. **Partly rejected (codex) Finding 2:** `SUM(DISTINCT x) OVER ()` stays row 1, consistent with plain `SUM(DISTINCT x)` under "an Ossie construct SLayer does not support yet" (§3.5). DEV-2282 adds it, and the windowed form then follows through decision 5. (user)
12. **Folded in (codex) Finding 5:** a test that a metric reference inside a window is row 6. (user)
13. **Folded in (codex) Finding 6:** `_bound` fallback tests through the strict entry point, covering a malformed window with variables and a bad variable slot. (user)
14. **Folded in (codex) Finding 7:** the mode is set after `_STATE.reset` under the lock, with strict and native reads alternating in both orders as a test. (user)
15. **Extension: a non-Ossie construct beside a window gives row 1.** It is the same bug class and is fixed by the same change, so it gets its own scenario. (user, approved with the plan.)

## Risks / Trade-offs

- [The reordered `_PRIMARY` changes where native fails on `OVER`] → The native parse action still fires on `OVER` followed by `(` before any clause token. Tests pin the native error type and message for `x OVER (`, `sum(x) FILTER (WHERE c) OVER (…)` and `ROW_NUMBER() OVER (…)`.
- [The strict tree reaches a consumer that dispatches on node kind and fails closed on `WindowCall`] → Only the strict reader asks for window mode. Generic walkers (`walk_parsed`, `_with_in_variables`, `_deferred_intervals`) traverse `model_fields`. `bind_variables`' slot ordinals are tested with a variable inside and beside a window.
- [Plain-aggregate arity moves row 6 to row 1] → It is an error code only, and the approved scope.
