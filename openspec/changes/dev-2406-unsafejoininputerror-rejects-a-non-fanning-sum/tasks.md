## 1. Plan-stage harness edits

- [x] 1.1 Apply the approved D15 wording to `architecture/semantics.arc42.md` Axioms 2.4 and 2.6 (design.md › Decision 5); verify the two hunks equal `git show origin/main:architecture/semantics.arc42.md` lines 62–65 and 91.

## 2. Tests (pr-tests)

- [ ] 2.1 Ask the owner's approval to align the three tests pinning the old refusals (design.md › Decision 13) to main's versions (`git show d95e3d49b -- tests/test_dev1750_execution.py`, `git show f40d3554e -- tests/test_dev1900_home_path.py tests/test_dev1908_default_cancellation.py`), then align them; verify they fail before the fix.
- [ ] 2.2 Write `tests/test_dev2406_home_routing.py` covering every scenario of `queries/semantics` › "The home, not the query root, decides where an aggregation is computed" and the rewritten "A reverse hop to a dataset not on the path stays refused" scenario, by executed values on SQLite and DuckDB, generic names only (never the issue's customer-derived names), portable to main (design.md › Decision 12); verify each fails on 1.x before the fix (refusal or wrong value), the to-one-anchor and redundant-partition scenarios included.
- [ ] 2.3 Write `tests/test_dev2406_parenthesised_in.py` covering every new `queries/time-points` scenario in filters and in measure conditions, plus key-shape tests (parenthesised single string → value-list membership on TEXT, period membership on TIMESTAMP; parenthesised number → one-element list) through both parser entry points (`engine/syntax.py` filters, `core/formula.py` measures); the 1.x-only timestamp check in its own test named for that divergence; verify the TEXT/number scenarios fail before the fix and the timestamp/bare-string ones pass.

## 3. Route by resolved home (pr-implement)

- [ ] 3.1 Add `AggregateHome` / `AggregateHomes` (with `of`, `target_rooted` exactly per design.md › Decision 3, `carried`) to `slayer/ir/terms.py`, `home_kind` on `Aggregate`, `homes` on the elaborated compile inputs; follow main `f40d3554e` shapes; verify `la-typecheck` passes.
- [ ] 3.2 `slayer/engine/home.py`: `aggregate_home` returning kind + path, D15 candidate order, `resolve_aggregate_homes` returning `AggregateHomes` (unresolved-at-anchor when there is no model host); verify `tests/test_dev1832_home.py`, `tests/test_dev1931_default_home.py` pass.
- [ ] 3.3 Resolve homes once in `slayer/engine/elaborate.py` and pass them to `build_environment` / filter typing / `crossing_local_root_predicate`; verify elaboration tests pass.
- [ ] 3.4 Replace every anchor-reading routing check with `homes.target_rooted` (keys.py predicates, `combined_kind`, `is_partitioned_consumer`, `crossing_local_root_predicate`, discovery, `_has_inline_population_aggregate`, the cross-model slot backstop, `home_of`); verify `grep -n "source_anchor_path" slayer/engine slayer/core/keys.py` leaves only non-routing uses (naming, spine, anchors of definitions).
- [ ] 3.5 Carry homes through `_strip_redundant_partitions`, `_substitute_reaggregations` and the routed prebound (`_carried_homes`); verify the redundant-partition scenario and 2.2 pass.
- [ ] 3.6 Port any later main fix a producer kind needs when homed below the root (design.md › Risks); verify every 2.2 scenario passes.

## 4. Parenthesised single literal (pr-implement)

- [ ] 4.1 Capture a parenthesised single literal after `in` / `not in` from the source text before `ast.parse` / `ast.unparse` in `slayer/engine/syntax.py` and `slayer/core/formula.py`, carried on the parsed comparison; number/boolean → one-element tuple at parse time; verify 2.3 key-shape tests for numbers pass.
- [ ] 4.2 Bind a parenthesised single string as a time-point comparison flagged `parenthesised`; in elaboration, a non-temporal operand lowers it to one-element value-list membership (respecting `not in`), a temporal operand keeps period membership; verify all 2.3 tests pass.

## 5. Docs and gates

- [ ] 5.1 Docs: `docs/concepts/queries.md:808` and `docs/concepts/formulas.md:61` get main's D15 wording ("homed on the source's own model when that determines every operand, else the deepest dataset that does"); `docs/concepts/time.md:45` one sentence on `x in ('v')`; verify the docs build has no new warnings.
- [ ] 5.2 Run the local test scope (la:pr › Local test scope), `poetry run ruff check slayer/ tests/`, `la-typecheck`, `poetry run la-arch-check`; verify all green.
