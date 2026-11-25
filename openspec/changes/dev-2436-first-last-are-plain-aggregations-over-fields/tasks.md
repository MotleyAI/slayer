## 1. Tests (pr-tests stage)

- [ ] 1.1 Write `tests/test_dev2436_ranked_aggregation.py` (unit; executed on SQLite and DuckDB with independent hand / pandas oracles) covering every scenario of the delta specs' ADDED requirements in `queries/semantics` and the modified ranking-key / default-key / NULL-key requirements in `aggregations/functional-form`; verify every new test fails on the current branch for the intended reason (not import or fixture errors)
- [ ] 1.2 Value-parity tests for every shape the former transform reading accepted (measure, filter, order key, aggregation source, aggregation parameter, mixed source, nested in rank, inside a cumsum combination, stage operand, spine-backed operand with an empty last bucket, non-spine operand beside a sibling contributing a later bucket, computed dimension with grained inners, NULL value at the last bucket, mixed-grain operand, nested transforms, unrelated constituents); oracles computed independently, not captured from the engine; verify they pass on today's code where the shape already works
- [ ] 1.3 Stored-metric tests through on-disk `YAMLStorage` (row-ranked, aggregate-ranked via a saved inner, nested, filter / order key, spine) and a dataset-defined custom `first` / `last` shadowing the built-in, over rows and over an aggregate
- [ ] 1.4 Boolean `first` / `last` typing tests in filters, comparisons, nested expressions and the second-order form
- [ ] 1.5 Dry-run SQL-shape / golden tests on every Tier-1 dialect (MySQL and SQL Server included) for the second-order form: NULL-key ordering, a picked NULL value, consumed-key attach
- [ ] 1.6 Port the two `first` / `last` row-supply repros (inferred population) and the associated-ranked-pick scenario as executed tests
- [ ] 1.7 Ask the user for per-file approval to align existing tests that pin the transform reading, then align them: broadcast-warning asserts in `tests/test_dev1832_transform_source.py`, transform-kwarg refusals in `tests/test_dev1903_first_last_dispatch.py`, the collapse-remedy text in `tests/test_time_axis_every_position.py`, `TestCollapsingPlanStructure`, the "A temporal computed dimension is the default ranking key" value, goldens in `tests/golden/dev1748_first_last_baseline.json` and any golden pinning the `FIRST_VALUE` transform SQL; find the rest by running `la-affected-tests` scope after implementation and grepping tests for `AXIS_COLLAPSING_TRANSFORMS`, `lower_collapsing_constituents`, `collapsed`

## 2. Binding and typing

- [ ] 2.1 Remove `first` / `last` from `TIME_TRANSFORMS` and the transform allow-lists in `slayer/core/enums.py`; audit every `AXIS_COLLAPSING_TRANSFORMS` reference, move boolean / value-type preservation onto the ranked `AggregateKey` (`slayer/core/keys.py`), then delete the constant; verify with the 1.4 tests
- [ ] 2.2 In `slayer/engine/binding.py` `_bind_agg_call_in`, bind an attached-only operand as a ranked second-order `AggregateKey` (delete the transform route for `first` / `last`); keep custom `first` / `last` shadowing; verify with the dispatch scenarios
- [ ] 2.3 Replace `_is_row_level_ranking_key` with the one "determined by the home" predicate judged by closure at `_ranking_key_crossing` / `_checked_ranking_time_key`, extended to the operand dataset (grain members, attached values whose grain the operand determines; row form: attached aggregates and transforms broadcast onto the rows); verify with the key-legality scenarios
- [ ] 2.4 Default key over an aggregate = the active time bucket, else `TimeAxisError`; reword the `_axis_suggestion` collapse remedy for a nested ranked aggregation that consumed the bucket; keep the `date_trunc` remedy; verify with the time-axis scenarios

## 3. Grain

- [ ] 3.1 Apply the consumption rule where an aggregate's grain is computed (base = `partition_by=` else default — query dimensions over rows, operand grain over an aggregate; unwindowed drops the key on exact structural match); a consumed key is a requested grain (no broadcast / association warning, every mode); verify with the grain-rule scenarios
- [ ] 3.2 Delete the collapsing branch of `_operand_grain`, `lower_collapsing_constituents`, `_collapsed_axis` and `RegroupAttachPlan.collapsed`; verify the parity tests (1.2) still pass

## 4. Emission and row supply

- [ ] 4.1 Render the second-order ranked aggregation through the re-aggregation producer with the existing ranked kernel (`slayer/sql/render/ranked.py`, `ROW_NUMBER` over the operand cells); delete the `FIRST_VALUE` branch of `_render_window_transform_sql`; verify with 1.2 and 1.5
- [ ] 4.2 Row-form attached key: attach the value onto the rows (mixed-source attach) before `ROW_NUMBER`; verify with the attached-aggregate / attached-transform key scenarios
- [ ] 4.3 `window=` / `FILTER` over an aggregate operand raise `ReaggregationError` / `AggregationFilterError`; verify with their scenario
- [ ] 4.4 Delete `_BROADCAST_ONLY` in `slayer/engine/population/row_supply.py` (keep the `window` exclusion); verify with 1.6

## 5. Harness and docs

- [ ] 5.1 Apply the approved `architecture/semantics.arc42.md` edits verbatim (design.md decisions 23, 24); verify `poetry run la-arch-check` passes
- [ ] 5.2 Update `docs/concepts/datasets.md`, `docs/concepts/expressions.md` (drop the transform section and "same name, different layer"), `docs/concepts/references.md`, `docs/examples/04_time/time.md`, and help content `slayer/memories/help_content/03_aggregations.md`, `04_transforms.md`, `05_time.md`; verify `tests/test_doc_examples.py` and any help-content tests pass

## 6. Gates

- [ ] 6.1 Run the local test scope (`la-affected-tests --base main` plus goldens / conformance touched), `poetry run ruff check slayer/ tests/`, `la-typecheck`, `poetry run la-arch-check`, `openspec validate dev-2436-first-last-are-plain-aggregations-over-fields --strict`; all green
