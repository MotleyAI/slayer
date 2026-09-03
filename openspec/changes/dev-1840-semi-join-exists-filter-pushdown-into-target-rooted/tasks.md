# Tasks — DEV-1840 semi-join filter pushdown

## 1. Tests first (TDD — land the failing suite)

- [x] 1.1 Fixture module `tests/_dev1840_fixtures.py`: forward-only LEFT-join graph (no mirrored reverse edges), a declared-reverse variant, a two-FK ambiguity model, a multi-hop composite-key chain with adversarial conflicting rows, and a derived (Mode-A SQL) column crossing a 1:N hop; verify by importing it and executing a smoke query on SQLite.
- [x] 1.2 Planner disposition tests `tests/test_dev1840_disposition.py`: three-way classification (inline / semi-join / dropped) incl. expanded Mode-A deps, OR/NOT mixed-locality exclusions, multi-branch exclusion, ambiguous-inversion exclusion, unreachable unchanged; verify each maps to the delta-spec scenario it pins and fails for the right reason (feature missing).
- [x] 1.3 Grouping + IR tests `tests/test_dev1840_grouping.py`: first-hop grouping (one EXISTS per branch, conjuncts AND-ed), hop descriptors carry oriented join_pairs + node identity (repeated-model alias binding; a literal self-join declaration — previously silently unaddressable — is now rejected at `SlayerModel` validation with an informative error, pinned in `tests/test_models.py::TestSelfJoinRejected`).
- [x] 1.4 Executed-value tests `tests/test_dev1840_execution.py` (SQLite + DuckDB): 1:N pushdown restricts population w/o cardinality change; same-row grouping counterexample; EXISTS ≡ inline on m:1 (declared vs undeclared, same data); zero-related-rows; result rows still honor the filter; multi-hop composite-key adversarial values; ranked, windowed, and nested computed-dimension producers (zero and multiple matching rows); verified all fail pre-implementation.
- [x] 1.5 Strict/metadata tests `tests/test_dev1840_strict_metadata.py`: strict passes on pushable, errors on excluded; pushed filters absent from warnings; dropped-filter warning intact for unreachable/ambiguous/out-of-scope; verify against the delta-spec strict scenarios.
- [x] 1.6 Golden SQL `tests/test_dev1840_golden_sql.py` + `tests/golden/dev1840_sql_baseline.json`: EXISTS shapes across seven Tier-1 dialects (ClickHouse shape asserted by the gating suite); baseline blessed pre-implementation so `inline/` cases (incl. the composite-key N:1 shape the corpus lacked) pin today's bytes; each `exists/` flip enters ALLOWED_DELTAS at implementation time.
- [x] 1.7 ClickHouse gating tests `tests/test_dev1840_clickhouse_gate.py`: recursive semi-join predicate (incl. nested-producer-only), guard raises the general error < 25.4 / unknown on dry-run/execute/explain, settings attached ≥ 25.4, UNION placement via the shared attach helper, coexistence with RLS-attached settings; verified with a mocked version probe.
- [x] 1.8 Update the three consented DEV-1836 pins (filter_inheritance ×2, broadcast_strict strict-drop) to the new semantics; the strict-unreachable replacement lives in `test_dev1840_strict_metadata.py` (with edge inversion the DEV-1836 graph keeps no unreachable filter shape); the rest of the DEV-1836 suites pass unchanged.

## 2. Implementation

- [ ] 2.1 `SemiJoinFilter` + hop-descriptor models in `slayer/engine/planned.py`, field threaded through producer synthesis to the producer `PlannedQuery` (nested included); verify 1.3 IR tests pass.
- [ ] 2.2 Scoped inversion helper in `slayer/engine/join_safety.py` (stored edge, else unique forward-edge inversion, via-host instance path; ambiguity → None) — correlation-only, unused by safe classification; verify its unit tests and 1.2 ambiguity cases pass.
- [ ] 2.3 Three-way `_conjunct_disposition` + expanded-dependency resolution + D2 scope walk + D3 first-hop grouping in `slayer/engine/stage_planner.py`; verify 1.2/1.3 pass.
- [ ] 2.4 EXISTS emission in `slayer/sql/generator.py` (producer base WHERE; allocator aliases; correlated outer refs; scope-closure asserts); verify 1.4 SQLite/DuckDB and 1.6 goldens pass.
- [ ] 2.5 Metadata/strict flip in `slayer/engine/query_engine.py` (pushed conjuncts leave dropped-filter surfaces); verify 1.5 passes.
- [ ] 2.6 ClickHouse: recursive predicate + generalized preflight/guard + plan-driven settings finalization shared with `session_policy`; verify 1.7 passes.
- [ ] 2.7 Full non-integration suite green (`poetry run pytest -m "not integration"`) and lint clean (`poetry run ruff check slayer/ tests/`).

## 3. Docs & corpus

- [ ] 3.1 Rewrite the filter-inheritance bullet in `docs/architecture/composable-attach.md` (three-way table) and the cross-model filter/strict/warnings text in `docs/concepts/queries.md`; verify no stale "dropped" wording for pushable filters remains (grep).
- [ ] 3.2 `openspec validate dev-1840-semi-join-exists-filter-pushdown-into-target-rooted --strict` green; divergence ledger of value/error flips recorded in the change folder for approval.
