## 1. Tests (spec-tests stage — all must fail or be pinned before implementation)

- [ ] 1.1 `tests/test_law_no_statement_reparse.py` + autouse fixture in `tests/conftest.py` (design D7): flags an `exp.Query` render inside the builders' extent. Self-tests: fires on render→parse and render→text-edit→parse inside a builder, passes on AST-only composition, and fails loudly if a wrapped builder name is missing. Verify: the self-tests pass, and the full unit suite is red at today's round-trip sites.
- [ ] 1.2 One top-level WITH (spec `sql/statement-assembly`): a paginated `cumsum` chain with `order` + `limit` on every Tier-1 dialect has exactly one `WITH` at the statement top, none inside a derived table, and dialect pagination. T-SQL `offset` without `order` gives `ORDER BY (SELECT NULL)` + `OFFSET … FETCH NEXT`. ORDER BY over a non-projected field (hidden alias) and the BigQuery dotted ORDER BY resolve against the outer scope. SQLite/DuckDB results match. Verify: red on the non-T-SQL dialects today.
- [ ] 1.3 Re-point the `emit_outer_wrap` unit tests (owner-approved) in `tests/dialects/test_base.py`, `test_tsql.py`, `test_mysql.py`, `test_bigquery.py`, `tests/test_dev1746_pagination.py` and `tests/dialects/test_dev1934_outer_order_column.py` to the unified builder / compiled-query SQL, keeping each intent: TOP/FETCH NEXT, OFFSET-needs-ORDER, bracket/backtick quoting, hidden-alias ORDER BY, CTE order preserved, BigQuery dotted ORDER BY. Delete only those whose sole subject is the removed hook signature. Verify: the re-pointed tests are red where they now assert the hoisted shape.
- [ ] 1.4 Re-point the six `_filtered` layer-boundary tests in `tests/test_sql_generator.py` (~1764–1795, ~2024–2072, ~8245–8306; owner-approved) so the POST predicate is the `WHERE` of the final select over the last chain step, never in `base`. Add: a POST filter over a non-projected composite dimension (resolves to a tail column; DuckDB rows), and an OR-conjunct mix (grouping kept; DuckDB rows). Verify: red today.
- [ ] 1.5 WHERE/HAVING as AST (design D5): several same-phase filters including an `OR` and a Mode-A model filter compile to one `AND` of grouped conjuncts. Golden + DuckDB execution. Verify: the golden pins the new parenthesisation, or is unchanged.
- [ ] 1.6 Query-backed model (spec scenario, design D6): a model with dotted, over-limit output names expanded for BigQuery and T-SQL has backing SQL whose output columns equal the fitted `Column.sql` names; expanded for DuckDB it executes and rows are keyed by column names. `build_flat_rename_wrapper` accepts an `exp.Select` and has no `projection_aliases`/text path. Verify: red today (signature).
- [ ] 1.7 MySQL sample statistics (spec `aggregations/trailing-window`): MySQL `covar_samp` / `corr` with `window='90d'` emit `VAR_SAMP` and no `VARIANCE(` in three separate cases: producer hoist in a plain query, inside a non-root stage, and inside the root stage. DuckDB + SQLite execution of both against hand-computed sample covariance / correlation (reuse an existing test if it already pins exactly this). A MySQL integration case in `tests/integration/test_integration_mysql.py`. Verify: the MySQL cases are red today.
- [ ] 1.8 Delete-or-retype checks: `SqlDialect` / `TsqlDialect` no longer define `emit_outer_wrap`; `generate_from_planned` has no `as_ast` parameter; `slayer.sql.naming` has no `FILTERED_ALIAS`. Verify: red today.

## 2. Builders return AST (design D1)

- [ ] 2.1 Add `_finish_statement` and route `generate_from_planned` / `generate_planned_stages` (single- and multi-stage) through it. Verify: unit suite green except the tests from §1.
- [ ] 2.2 Make the windowed, cross-model combined and plain branches return `exp.Select`; delete `as_ast`, its fallback parse and `_parse_cte_body`. Verify: 1.8 (`as_ast`) green.

## 3. One outer-wrap builder + POST filter (design D2, D3)

- [ ] 3.1 Unify the chain path's outer wrap with the plain path's builder (WITH hoisted, `apply_pagination`); delete `emit_outer_wrap` (base + T-SQL), `_offset_ordering_fallback` and `_emit_planned_outer_wrap`, and `_outer_order_column` if unused. Verify: 1.2, 1.3 and the DEV-1934 dialect-hook law green.
- [ ] 3.2 POST conditions as `List[Expression]` → `WHERE` on the chain's final select; delete the f-string and `FILTERED_ALIAS`. Verify: 1.4 and 1.8 green.

## 4. Splits, producers, WHERE/HAVING (design D4, D5)

- [ ] 4.1 Stage / root splits and the producer attach on AST; `build_flat_rename_wrapper(inner: exp.Select)`; delete the repair-only `unmangle_dotted_table_refs` calls. Verify: 1.7 MySQL seams green.
- [ ] 4.2 WHERE/HAVING via `exp.and_`; delete `_SQL_AND_JOINER` if unused. Verify: 1.5 green.

## 5. Query-backed model (design D6)

- [ ] 5.1 `_build_planned_stages_ast`; the engine wraps the unrewritten AST and finishes once with `[*projection_result_keys, *flat names]`; the wrapper's decode path is gone. Verify: 1.6 green.

## 6. Goldens, law, docs, gates

- [ ] 6.1 Re-bless goldens after reviewing the diff: transform-chain statements (hoisted `WITH`, `_filtered` removed, whitespace), MySQL `corr`/`covar_samp` → `VAR_SAMP`, and D5 parenthesisation only. Any other byte change → stop and investigate. Verify: the golden suites are green and the diff review is recorded in the commit message.
- [ ] 6.2 Law 1.1 green over the full unit suite. If a value-layer `Query` render (DEV-1972) trips it → STOP and ask, never allowlist silently. Verify: the full unit suite is green with the fixture active.
- [ ] 6.3 Update the `tsql.py` module docstring (drop the `emit_outer_wrap` bullet) and any `docs/` mention of `emit_outer_wrap` / `_filtered` (search first; expected none). Verify: a grep for `emit_outer_wrap|FILTERED_ALIAS|_filtered` over `slayer/` and `docs/` is empty.
- [ ] 6.4 Full unit suite, the CI-style integration suite, `ruff check slayer/ tests/`, basedpyright (baseline not grown, and shrunk in touched files) and `tools/arch_check.py`. Verify: all green.
