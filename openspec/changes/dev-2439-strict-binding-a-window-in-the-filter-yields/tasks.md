## 1. Tests (pr-tests stage)

- [x] 1.1 Remove the strict xfail on `test_a_window_yields_to_earlier_rows` in `tests/test_dev2217_errors.py`, and add one strict-binding test per scenario in the delta spec (the malformed windows, names inside a window including the row-6 metric case, the rest of a windowed filter, complete windows including the bound variable, aggregate argument shapes, native still refusing). Verify that each fails for the right reason before implementation.
- [x] 1.2 Add `_bound` fallback tests through the strict entry point: a malformed window with a `{variable}`, and an invalid variable slot beside a window. Each must raise `E_INVALID_QUERY`. Verify by running them.
- [x] 1.3 Add `core.syntax` unit tests:
  - the window tree shape (callee, partition, order with direction and NULLS, frame bounds);
  - `walk_parsed` reaching references in the function arguments, partition and order;
  - a windowed aggregate wrapping the same `AggCall` the plain call reads to (`COUNT(*)`, `FILTER`, `PERCENTILE_CONT … WITHIN GROUP`);
  - Ossie ranking and offset names not dispatched as transforms.

  Verify by running them.
- [x] 1.4 Add native guard tests: native `parse_expr` and `bind_variables` on `x OVER (`, `sum(x) FILTER (WHERE c) OVER (…)` and `ROW_NUMBER() OVER (…)` raise `IllegalWindowInFilterError` with today's message. Run strict and native readings of the same text alternating in both orders. Verify that they pass before and after implementation.

## 2. `core.syntax` window-reading mode

- [ ] 2.1 Add the node kinds `WindowCall`, `OrderItem` and `WindowFrame` as `_BaseNode`s (design decision 4). Verify that 1.3's shape tests pass.
- [ ] 2.2 Add the gated window clause to `_PRIMARY`:
  - the `OVER (` parse action raises today's error when the mode is off;
  - with the mode on, it reads `PARTITION BY`, `ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]` and a `ROWS`/`RANGE` frame;
  - `OVER` comes last and at most once, and only after a call;
  - the callee is handled per design decision 5.

  Verify that 1.3 and 1.4 pass.
- [ ] 2.3 Thread `windows: bool = False` through `parse_expr`, `parse_template`, `parse_bound_expr`, `bind_variables`, `legacy_spans`, `nonossie_spellings` and `call_spans`. Cache `_read` on `(text, windows)`. Set the mode after `_STATE.reset`, under `_PARSE_LOCK`. Verify that 1.4's alternation tests pass.

## 3. Strict judgment

- [ ] 3.1 `read_filter` reads with `windows=True`, and drops its `IllegalWindowInFilterError` catch; `ossie_l3._bound` binds with `windows=True`. Verify that 1.2 passes.
- [ ] 3.2 Add the `WindowCall` visitor:
  - it sets `window` and visits the function's arguments, filter, partition and order values;
  - it refuses a non-Ossie window function or wrong arity, a nested window, and an out-of-order frame (design decisions 6–9);
  - a windowed aggregate does not mark `aggregate`.

  Verify that 1.1's window scenarios pass.
- [ ] 3.3 Add Ossie's aggregate argument shapes to `_aggregate`, used by plain and windowed aggregates (design decision 10). Verify that 1.1's aggregate-arguments scenario passes.

## 4. Finish

- [ ] 4.1 Run the local test scope (`la-affected-tests --base main`, plus `tests/test_dev2217_errors.py` and the Ossie conformance tests), ruff, `la-typecheck` and `la-arch-check`. All must be green.
