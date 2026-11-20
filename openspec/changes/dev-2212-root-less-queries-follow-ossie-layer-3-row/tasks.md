## 1. Tests first (pr-tests)

- [ ] 1.1 Add the Appendix B seed (customers with `segment`, orders, returns, visits, unrelated tickets, the orphan variant, saved metrics) as a shared fixture for SQLite and DuckDB, reusing `tests/_engine_helpers.seeded_exec_engine` and `tests/_dev1737_fixtures.seed_backend`. Verify: the fixture loads on both engines.
- [ ] 1.2 `tests/test_dev2212_row_supply.py`: every scenario of `queries/population` › Rows of an inferred query are the row supply (D1–D4, N1–N3, unions, count 0, dimension-only, filters, cross-dataset filter, unrelated, associated, partition extension, filtered-out and RLS-hidden rows, producer forms, absent vs all-NULL cells, key types), plus the unchanged cases (every query naming `population` returns its pre-change rows). Verify: fails for the right reason before implementation.
- [ ] 1.3 `tests/test_dev2212_home_routing.py`: `queries/references` scenarios "An aggregation reads its grain from its home" and "A filter restricting a home is routed from that home", inferred and anchored. Verify: fails (ambiguity today).
- [ ] 1.4 `tests/test_dev2212_raw_rows.py`: raw rows of the named population, else of the anchor read from dimensions only; an unknown-cardinality hop and an unconnected group fail closed; a finer filter never moves the anchor.
- [ ] 1.5 `queries/population` remaining scenarios: `population_inferred` (pinned anchor keeps rows inferred, refused without `population`, round-trip through save/load and SlayerQuery v18), the dimension anchor (product, unknown-cardinality hop, tie fatal only when needed, undefined-but-unneeded reports `null`), datasource scoping (measures-only, two datasources), reporting (`population`, `row_sources` contract on normal, dry run, explain, cache hit, refresh, REST, MCP JSON/markdown/CSV, Python client), pinning (A + marker on save, legacy pinned stage stays anchored, legacy unpinned refined), refinement rejects `population_inferred`.
- [ ] 1.6 `queries/semantics`: "Adding a measure to an inferred query only adds rows", "Filters restrict every home of an inferred query" scenarios; `queries/unrelated-datasets` new scenarios; `queries/time-spine` "A filter on a finer dataset keeps P" and "An orphan beside the spine is not yet a row".
- [ ] 1.7 Law harness (`tests/_law_harness.py`) root-less shapes and four law files: `tests/test_law_measure_monotonicity.py`, `tests/test_law_home_row_preservation.py`, `tests/test_law_filter_non_expansion.py` (with and without `population`), `tests/test_law_unit.py`. Each law runs both the single-source and the key-union shapes.
- [ ] 1.8 Strict xfails (reason DEV-2218): a rank-family transform and `lag` over a monthly time dimension keep their values in existing rows when a sibling measure adds rows to a root-less query.
- [ ] 1.9 ClickHouse integration case in `tests/integration/test_integration_clickhouse.py`: a key-union query with mixed nullable keys (runs in its path-gated CI workflow).
- [ ] 1.10 Existing tests contradicting the plan: ask the owner's per-file consent before aligning — `tests/test_dev1866_*`, `test_law_population_invariance.py` (scope to anchored mode), `test_saved_query_population_pinning.py`, `test_dev2015_population.py`; check and list the affected ones among `test_dev1909_population_pushdown.py`, `test_dev1958_shifted_population.py`, `test_dev1871_population_routing.py`, `test_law_grain_union.py`, `test_law_split_invariance.py`, `test_unrelated_datasets.py`, `test_dev2216_associate_default.py`, `test_dev2277_*`.
- [ ] 1.11 Codex review of the tests against this change. Verify: findings resolved with the owner.

## 2. Architecture harnesses (approved wording, verbatim from design.md decisions 5–6)

- [ ] 2.1 Apply the `architecture/semantics.arc42.md` edits (Axioms 2.9, 3.1, 12, 14, 15; Laws 3, 7, 8, 9, 10; §4 P1 tag flip). Verify: `poetry run la-arch-check` passes and every new `[enforced: test:…]` file exists.
- [ ] 2.2 Apply the `architecture/python.engine.arc42.md` P8 edit. Verify: `poetry run la-arch-check`.

## 3. Query model and storage

- [ ] 3.1 `SlayerQuery.population_inferred: bool = False` with the typed refusal without `population` and the terse Field description; refinement rejects it. Verify: 1.5 tests.
- [ ] 3.2 SlayerQuery stored version 17 → 18 (stamp, default `false`) in `slayer/storage/migrations.py`; pin it in the version tests. Verify: round-trip test.

## 4. Dimension anchor and row supply (engine.population)

- [ ] 4.1 Anchor inference from dimensions only (filters and metrics out), unknown-cardinality hops counted by association except raw rows, product of unconnected groups, unit with no dimensions, lazy evaluation (fatal only when needed). Keep the recommendation, the memory resolver and the probes on the same core. Verify: 1.5, 1.4.
- [ ] 4.2 Datasource scoping over dimension, filter and metric anchors (saved metrics expanded). Verify: 1.5.
- [ ] 4.3 Row-supply classification per metric: full-grain operands (home determines or associates every dimension over a join path), coarser operands (`partition_by=`, product hop, broadcast fallback) extended by the anchor; only `metrics` supply. Verify: 1.2.

## 5. Planning and SQL

- [ ] 5.1 Route each aggregation's grain dimensions and the filters restricting its home from the home (`queries/references`), in both modes; inferred filters restrict every home and the anchor, never choose. Verify: 1.3, 1.6.
- [ ] 5.2 Row-supply plan in `slayer/ir/planned.py` + `slayer/engine/compile/stages.py`: one source → it is the base; several → key union of their cells, each producer attached on the null-safe grain, coarser ones on their grain subset, extended cells semi-joined to the anchor's cells; the unit reuses the one-row base. Window endpoints and shifted producers read the row supply's cells. Verify: 1.2, 1.7.
- [ ] 5.3 Key-union rendering in `slayer/sql/generator.py` as sqlglot AST: `UNION` (distinct) of branch key columns, each cast to its bound dimension type; never chained FULL JOINs. Verify: 1.2 key-type scenario on SQLite/DuckDB, 1.9 in CI.
- [ ] 5.4 Raw-row mode over the named population or the anchor, provable determination only. Verify: 1.4.
- [ ] 5.5 Spine queries: P = the anchor of the other dimensions, filters restrict by association. Verify: 1.6.

## 6. Reporting, saving and surfaces

- [ ] 6.1 `population` (anchor, `a × b`, `null`) and `row_sources` on `SlayerResponse`, REST `QueryResponse`, the Python client, every execution path (normal, dry run, explain, cache hit, refresh); MCP prints both only when inferred. Verify: 1.5.
- [ ] 6.2 Dimensionless `unrelated_dataset` symmetric rule with `population: null`. Verify: 1.6.
- [ ] 6.3 Pinning on save writes the anchor + `population_inferred: true` (unit/undefined anchors stay unpinned); legacy pinned stages stay anchored; refinement of legacy unpinned stages pins in memory. Verify: 1.5.
- [ ] 6.4 `EMPTY_DETERMINATION` no longer raised for measures-only queries; retire the reason only if nothing raises it. Verify: 1.5.

## 7. Docs

- [ ] 7.1 `docs/concepts/queries.md` (Population, Choosing a root, Cross-dataset metrics; one sentence on a named population and orphan home rows), MCP `population` Field description and the new `population_inferred` Field, `help.*` memories. Verify: docs examples run (`tests/test_docs_*` where present).
- [ ] 7.2 `docs/reference/{mcp,rest-api,python-client}.md`: `population`, `population_inferred`, `row_sources`. Verify: grep shows no stale description.
- [ ] 7.3 `docs/upgrading/2.0.md` section with before/after for each break (D1/D2 rows disappear — add a measure from the dimension's dataset or name `population`; filters no longer move the population; measures-only root-less queries answer; saved stages pin the anchor + marker). Verify: page linked in `zensical.toml` nav.

## 8. Gates

- [ ] 8.1 `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline, never `--writebaseline`), `poetry run la-arch-check`, `npx likec4 validate architecture`, `openspec validate --all --strict`.
- [ ] 8.2 Local test scope green (`la-affected-tests`); full suites green in CI.
- [ ] 8.3 After merge: comment on DEV-2218 and DEV-2217 that the row supply is on `main`.
