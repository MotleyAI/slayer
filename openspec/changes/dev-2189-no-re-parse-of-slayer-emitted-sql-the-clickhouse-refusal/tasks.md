## 1. Tests first (pr-tests)

- [ ] 1.1 Runtime law harness `tests/_emitted_reparse_law.py` + conftest autouse install (design D8); self-tests in `tests/test_law_no_emitted_reparse.py`: planted re-parse in `_prepare_pipeline` fails, one three calls below it fails, validators pass, parse after the execution returns passes, every target resolves, a query-backed type probe renders only its final probe statement (D7). Verify: self-tests pass against the harness; the full unit suite FAILS on today's code at the seven known sites
- [ ] 1.2 RLS parity matrix `tests/test_dev2189_rls_parity.py` (D9): policies × shapes × dialects, reference = reconstructed text path, rows compared on SQLite/DuckDB/Postgres (pytest-postgresql, unit). Verify: green on today's code where both paths exist (it pins today's behaviour), wired to the AST entry so it fails to import/run until 2.3 lands
- [ ] 1.3 Fail-closed matrix over the five policy callers (D9): each refusal kind keeps its outcome (raise / type probe `{}`) and no SQL client method runs after a policy failure. Verify: green on today's code, green after
- [ ] 1.4 Cache applicability before/after tests (D5): Snowflake `TABLE(GENERATOR(...))`, ClickHouse `numbers(...)`, qualified physical tables, CTE-name collision, RLS hop tables counted. Verify: same applicable keys today and after
- [ ] 1.5 `expected_columns` scenarios on mangling/fitting dialects (BigQuery, T-SQL, over-limit Postgres) asserting canonical keys in projection order. Verify: green today and after
- [ ] 1.6 Mechanical input changes only (no assertion changes) in `tests/dialects/test_server_profile_hooks.py` `TestCorrelatedSubqueryRefusal`, `tests/test_session_policy.py`, `tests/test_carrier_scope_matrix.py`, `tests/test_dev1840_clickhouse_gate.py`, `tests/test_query_cache.py`, `tests/test_joined_path_dimension_names.py`: feed trees (parse the literal in the test). Verify: diff shows input plumbing only

## 2. Implementation (pr-implement)

- [ ] 2.1 Generator: `PlannedStatement` build step (settings attached) + `_finish_statement` as the single render with RLS-aware scope validation (D1, D4); `generate_planned_stages` = build + render. Verify: statement-assembly suites green
- [ ] 2.2 One `physical_tables(statement)` walk in `slayer/sql` with RLS's rule (D5). Verify: 1.4 green
- [ ] 2.3 `apply_session_policy` on trees, text entry deleted; fail-closed code unchanged (D3). Verify: `test_session_policy` + 1.2 + 1.3 green
- [ ] 2.4 `correlated_subquery_refusal(profile, *, statement)` + tree predicate (D2). Verify: `TestCorrelatedSubqueryRefusal` green
- [ ] 2.5 `build_response_metadata` from the tree; delete `expected_columns_from_sql` (D6). Verify: 1.5 green
- [ ] 2.6 Cache: `applicable_keys(tables=…, dialect=…)` from `_Prepared.tables`; delete `parse_referenced_tables` (D5). Verify: `test_query_cache` + 1.4 green
- [ ] 2.7 Engine pipeline: `_Rendered` always carries the statement (drop `sql` / `as_statement`), `_prepare_pipeline` order per D1, `_Prepared.tables`; refresh-key scan and side stats build trees and render once after the policy (D3). Verify: 1.1 law green on these paths
- [ ] 2.8 Split `_expand_query_backed_model` (D7): AST-level expansion for `get_column_types` (probe spliced as stages), render kept for save-time validation. Verify: existing type-probe suites + the D7 law test green
- [ ] 2.9 arc42 python.sql §3.1: add the approved tag line "[enforced: test:tests/test_law_no_emitted_reparse.py]" verbatim. Verify: `poetry run la-arch-check` green
- [ ] 2.10 Docs: grep `docs/` for `apply_session_policy`, `expected_columns_from_sql`, `parse_referenced_tables`; no user-facing change expected. Verify: grep clean or one-sentence updates
- [ ] 2.11 Full gates: `poetry run pytest -m "not integration" -n 8`, the CI integration invocation (with `-n 8`), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors), `poetry run la-arch-check`. Verify: all green; ClickHouse integration runs unchanged in its CI workflow
