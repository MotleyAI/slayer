## 1. Failing tests (pr-tests)

- [ ] 1.1 New `tests/test_transform_domain_in_aggregation.py` executing every scenario of the `queries/transforms` delta on SQLite and DuckDB with the hand-computed values given there. City seed: `city_engine` from `tests/test_transform_operand_grain.py`; Appendix B: `seed_engine` / `run` from `tests/_dev2212_fixtures.py`. Verify the source, parameter, filter and order scenarios fail on HEAD for the reason in design.md Context, and the "already evaluated over their own cells" guards pass already. Planning probe values on `main` `3bc68c4c3`:
  - issue shapes: North 3 / South 1, North 1 / South 3, North 3 / South 1;
  - finer sibling North 73;
  - `sum(rank)` North 3;
  - `max(percent_rank)` South 0.667;
  - `weight=rank(...)` with no dimensions 53.125;
  - guards already right: mixed source 70 / 140 / 50 / 60; cumsum with sibling 40, 105 / 70, 110; `time_shift` guards February 40 & 3 (North), 70 & 1 (South), January NULL; Appendix B EAST 1, NORTH 2.
- [ ] 1.2 Parameter coverage (design decision 8). Each issue shape and `percent_rank` as `weight=` of `weighted_avg(sum(orders.amount, partition_by=[orders.region, orders.city]), weight=T)` with no dimensions, the windowed shape grouped by month over `sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at])`. Values: 320/6, 325/6, 55, January 25 / February 34. Verify they fail on HEAD where the transform's cells differ from the carrier's rows.
- [ ] 1.3 Unit test of the plan-time invariant (design decision 5): a plan evaluating a transform over rows at a grain other than its operand grain raises at plan time; a plan whose grain equals the operand grain only after pruning a functionally determined member (e.g. a key determined over a to-one hop) is accepted. Verify it fails on HEAD (no invariant yet).
- [ ] 1.4 If any existing test pins the old carrier-row values, ASK the user's approval to align it (name each file and change) before editing — none expected: the planning probe ran the decision 1–3 rule over 183 transform / re-aggregation / golden files green.

## 2. One routing rule for transforms

- [ ] 2.1 Producer context carries an explicit set of in-place transform answers (design decision 2); `_route_producer` passes discovery exactly that set; the blanket non-dimension-metrics `answers` retires from `TransformRouting` and `routes()` (`slayer/engine/compile/discovery.py`). Verify the scenarios of 1.1 that do not involve a carrier at the transform's grain.
- [ ] 2.2 Declarers (design decisions 2–3): the transform's own domain producer (its answer), the shifted producer (its declared answer, not `domain_of`), and the re-aggregation carrier iff the transform is its only cell-supplying source constituent and the carrier grain equals its result grain (`_synthesize_reaggregation_producer` / `_build_carrier_attach`). Verify the issue scenarios, the Appendix B guard and the `time_shift` guards.
- [ ] 2.3 A transform routed out of a carrier attaches through the existing regroup path as a coarser constituent (design decision 4), no new carried-attach plumbing. Verify 1.1–1.2 and that `git grep -n "carried_attaches\|placeholder_originals" slayer/` gains no new call site.
- [ ] 2.4 Plan-time invariant (design decision 5): every transform evaluated over a plan's own rows has an operand grain equal to the plan grain after determination pruning, else an internal error at plan time. Verify 1.3.

## 3. Docs and harness

- [ ] 3.1 `docs/concepts/expressions.md` § "Transforms read their operand's cells": one sentence that the rule holds inside an aggregation (source or parameter), e.g. `max(rank(sum(x, partition_by=region)))` by region and city ranks the regions. Verify no `zensical.toml` change is needed.
- [ ] 3.2 Apply the approved arc42 tag edit verbatim (design decision 16) in `architecture/semantics.arc42.md`; verify `poetry run la-arch-check`.

## 4. Gates

- [ ] 4.1 Local test scope per `/la:pr`: `la-affected-tests --base main` plus the goldens and `tests/test_transform_operand_grain.py`, `tests/test_dev2218_*`, `tests/test_dev1832_*`, `tests/test_dev1847_*`, `tests/test_dev1946_*`, `tests/test_dev2419_*`, `tests/test_dev2457_measure_windows.py`, `tests/test_dev1868_*`, `tests/test_dev1958_*`; `poetry run ruff check slayer/ tests/`; `poetry run basedpyright` (no new errors vs baseline); `openspec validate dev-2494-a-transform-over-a-coarser-grained-operand --strict`. All green.
