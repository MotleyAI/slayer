## 1. Tests (pr-tests)

- [ ] 1.1 New stored-upgrade test file (YAML and SQLite backends, raw v10 documents via `tests/_stored_upgrade_fixtures.py`), one test per new `models/stored-upgrade` scenario: the issue example over the query-backed `rev_f`; the `revenue` model-metric clash, with `r10` checked against ten times the per-status `sum(amount)`; a stage over an earlier stage `s1`; `partition_by=su` staying bare; the quoted `"Amount"` alias. Each asserts the loaded spelling and the returned rows. Verify: every test fails on the current branch.
- [ ] 1.2 New live-conversion cases for the `queries/references` scenarios: live `revenue * 10`; the `amount` alias-equals-field filter; the pure alias `n` in a filter. Verify: the first two fail on the current branch and the third passes.
- [ ] 1.3 Law test: the static stage output-name helper equals the compiled stage schema's field-name set, over the cases of design decision 11. Verify: it fails only because the helper does not exist yet.
- [ ] 1.4 Run the existing stored-upgrade and legacy-conversion tests (`la-affected-tests`). If an existing test pins "the alias wins over a population field", stop and ask the owner for approval to align it, naming the file and the change.

## 2. Implementation (pr-implement)

- [ ] 2.1 Add the pure bare-name resolver in `slayer/core/query.py`, beside `declared_aliases` (design decisions 1, 7, 9). Verify: the 1.x unit cases in 1.1/1.2 reach it.
- [ ] 2.2 Add the static stage output-name helper in an engine module that `slayer/storage/qualified_references.py` already imports, with no new import arrow (decisions 4, 5, 8). Verify: the law test 1.3 passes and `poetry run la-arch-check` is green.
- [ ] 2.3 Route `_Stage._reference` through the resolver: entry positions for metrics, dimensions, time dimensions and the main time dimension; filter/order positions for filters and order fields and raw expressions. Population names: the host's fields and metrics, or the derived output columns for a query-backed host awaiting fields or an earlier stage. Verify: 1.1 passes.
- [ ] 2.4 Route the live converter (`_converts_reference`, `_converts_argument`) through the resolver. Split `AnchorNames.members` into field and metric names and fill them for stage and query-backed populations. Verify: 1.2 passes and existing live-conversion tests stay green.
- [ ] 2.5 Update `docs/upgrading/2.0.md`: one sentence in the stored-upgrade paragraph (~line 388) and in the root-relative paragraph (~line 911) stating the positional reading. Verify: `tests/test_doc_examples.py` stays green.
- [ ] 2.6 Run the local test scope (`la-affected-tests --base main` plus the stored-upgrade and doc-example suites), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs the baseline) and `poetry run la-arch-check`. Verify: all green.
