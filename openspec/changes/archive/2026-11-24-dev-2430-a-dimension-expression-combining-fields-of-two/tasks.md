## 1. Tests (pr-tests)

- [x] 1.1 Create `tests/test_dev2430_row_expression_home.py` (SQLite and DuckDB, the `tests/_dev2212_fixtures.py` seed plus the extra fields each scenario needs), one test per scenario of `specs/queries/semantics` › *Row-level expressions are homed or refused*; verify every refusal test fails on the current code and every legal-shape test (remedy, parent + child, filter, product hop) passes or fails only where the probe in design.md › Context says today's behaviour differs (parent + child time dimension).
- [x] 1.2 Cover design decisions 13, 15, 16, 17 explicitly: a derived leaf whose definition crosses an unproven hop, alone and inside a larger expression; a transform constituent grained on a sibling; product-hop precedence for dimension, time dimension, order key and a derived field; a sibling order key refused before the wrap and a parent + child order key still wrapped `max` (descending) / `min` (ascending). The derived-field product-hop branch is a strict xfail against DEV-2445.
- [x] 1.3 Add the `specs/queries/time-dimensions` scenarios (sibling leaves fail closed naming both; parent + child homed and associated) to the new file; verify they fail on current code.
- [x] 1.4 Align `tests/test_dev2119_expression_time_dimensions.py::TestExpressionTyping::test_leaf_across_a_fanning_hop_fails_closed` to assert the parent + child expression executes, associated (approved alignment, design decision 4); verify it fails on current code.
- [x] 1.5 Apply the approved Axiom 2 status-tag edit to `architecture/semantics.arc42.md` verbatim (design decision 11, second bullet) once the test file exists; verify `poetry run la-arch-check` passes.

## 2. Resolver (pr-implement)

- [x] 2.1 In `slayer/engine/population/home.py`, factor the candidate set and the determines-every-operand test out of `aggregate_home` into a shared core; `aggregate_home` keeps its anchor-first order (design decision 12); verify `tests/test_dev1832_home.py` and every other `aggregate_home` test pass unchanged.
- [x] 2.2 Add the Axiom 2.2 row-level home resolver on that core: leaves at their declared dataset or, across a fanning definition, at the datasets it reaches (decision 13), constituents via the existing grain expansion (decision 15), deepest determining candidate first; on failure return two operands and each one's first fanning or unproven hop from their common dataset; retire `row_expression_crossing`; verify with the tests of group 1.
- [x] 2.3 In `slayer/engine/elaborate_env.py`, replace `check_time_dimension_home` with one row-level home check raising `UnsafeRelationshipInputError` (location: the position; summary: both operands and hops; suggestion: aggregate one operand to a grain the other's dataset determines, or group by each field as its own dimension); a leaf whose closure cannot be analysed keeps its existing resolution error (typed parse, ambiguous path, unresolvable); update the `tests/_dev1871_raise_ledger.py` row (approved alignment); verify `tests/test_dev1871_raise_parity.py` passes.
- [x] 2.4 In `slayer/engine/elaborate.py`, right after the product-hop check, call the check for plain and computed dimensions, time dimensions and row-level order keys (before any host-grain wrap is built), after the product-hop check (decision 7), in every stage; verify group 1 passes.
- [x] 2.5 Evaluate every homed dimension, time dimension and row-level order key whose home lies below another operand's dataset as a column of its home (design decision 19); verify the *A parent and child expression reads NULL without a home row* scenario and `test_a_parent_and_a_child_time_leaf_equal_a_child_column`.

## 3. Spec, arc42 and docs (pr-implement)

- [x] 3.1 Apply the approved Axiom 2.2 carve-out to `architecture/semantics.arc42.md` verbatim (design decision 11, first bullet); verify `poetry run la-arch-check` passes.
- [x] 3.2 Add one sentence on the rule to the docs page documenting computed dimensions, and a breaking-change entry with the remedy to `docs/upgrading/2.0.md`; verify `tests/test_doc_examples.py` passes.

## 4. Gates (pr-implement)

- [x] 4.1 Run the local test scope (`la-affected-tests --base main` plus the files above); any existing test pinning a now-refused expression is raised for approval to align, never changed silently; verify all pass.
- [x] 4.2 Run `poetry run ruff check slayer/ tests/`, `la-typecheck` (no new errors vs the baseline), `poetry run la-arch-check` and `openspec validate dev-2430-a-dimension-expression-combining-fields-of-two --strict`; verify all clean.
