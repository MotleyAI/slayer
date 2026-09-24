## 1. Tests (spec-tests stage — all must fail or be pinned before implementation)

- [x] 1.1 `tests/test_sql_template.py`: placeholder discovery + render — precedence (`{x}*c`, `a/{x}`, `-{x}`, `{x} IS NULL`, function arg with `x=a+b`), string-literal / quoted-identifier / line+block-comment / escaped-quote inertness, Unicode before a placeholder, adjacent placeholders, `{ value }`, keyword placeholder, DuckDB struct literal untouched, repeated placeholder independent copies, unbound → typed error, unparseable / unclosed brace → typed error at construction, qualifier / alias / function-name position → typed error, natural sentinel-name collision, repeated + concurrent renders leave the cached AST unchanged; verify by running the file (red).
- [x] 1.2 Formula-agg generator tests: `weighted_avg` + custom-agg goldens across Tier-1 dialects (compound `Column.sql` value keeps grouping; re-bless only paren/whitespace), string-literal-inert scenario, unbound-placeholder query error; SQLite/DuckDB execution equivalence for re-blessed goldens.
- [x] 1.3 Percentile `p` tests: `0`, `1`, `0.50` (verbatim), `5e-2`, `-0`, `(0.5)` accepted; NaN, overflow, out-of-range, string, column, arithmetic rejected with the existing messages.
- [x] 1.4 Dialect-hook goldens: stat 1-arg / covar 2-arg (incl. MySQL/T-SQL decomposition with a parent-pointer structural check), median, approx-count-distinct on every Tier-1 + Tier-2 dialect (pinning today's emission), `build_date_trunc` incl. SQLite week/quarter, `WEEK_SUNDAY`, expression operands needing the TIMESTAMP cast, BigQuery/T-SQL overrides.
- [x] 1.5 Picked-value path regression: DEV-1832 filtered + cross-model picked parameters render without an emit/re-parse round trip (golden + execution).
- [x] 1.6 Save-time tests (spec `models/save-validation`): unparseable formula rejected at MCP `create_model`, REST create, CLI create, and MCP `edit_model` (original intact); query-time-only placeholder accepted; qualifier-position placeholder rejected; unresolvable datasource falls back to the generic dialect; check runs before any storage mutation.
- [x] 1.6a MCP `create_model` with `aggregations`: a valid custom aggregation persists and is queryable; combined with `query` → error.
- [x] 1.6b `_outer_order_column` / `emit_outer_wrap(projected=...)`: an ORDER BY over a hidden hoist resolves via the projected alias set (all dialects incl. T-SQL / BigQuery dotted aliases); no `inner_sql` scan.
- [x] 1.7 `tests/test_law_ast_dialect_hooks.py` (type law, design §9) + negative self-test with a synthetic dialect taking a `str` operand; land the approved `sql.arc42.md` §3.1 edit in the same commit; verify `poetry run python tools/arch_check.py` green.

## 2. Parse pipeline + SqlTemplate

- [ ] 2.1 Extract `parse_expression(sql, *, dialect)` from `Generator._parse`; `_parse` delegates; full unit suite green.
- [ ] 2.2 Implement `slayer/sql/sql_template.py` (design §2–3); 1.1 green.

## 3. Generator

- [ ] 3.1 `_resolve_value_ast` / AST `_resolve_agg_param`; all callers incl. picked-value `MAX`; 1.5 green.
- [ ] 3.2 `_build_formula_agg` via `SqlTemplate`; delete `_paren_fragment`; 1.2 green.
- [ ] 3.3 Percentile `p` validation (design §5); 1.3 green.

## 4. Dialect hooks

- [ ] 4.1 Retype + convert every aggregate hook and `_build_covar_decomposition` (design §6); drop `parse`; add `StatAgg1Name` / `StatAgg2Name`.
- [ ] 4.2 Replace `approx_count_distinct_template` with `approx_count_distinct_native` (design §7) across all dialects.
- [ ] 4.3 `build_date_trunc` / `build_time_offset_expr` retyped; SQLite week/quarter as AST; 1.4 + 1.7 green.

## 5. Save-time check + MCP door

- [ ] 5.1 `QueryEngine.save_model` formula parse check (design §8); 1.6 green.
- [ ] 5.2 MCP `create_model` `aggregations` parameter + docstring (design §10); 1.6a green.

## 5b. Outer-wrap ORDER BY

- [ ] 5b.1 `emit_outer_wrap(projected=...)` + typed `_outer_order_column` (design §11); 1.6b + 1.7 green.

## 6. Docs + gates

- [ ] 6.1 One sentence in `docs/concepts/models.md` (aggregations section): custom formulas are parse-checked at save; one sentence wherever MCP `create_model` args are documented: it accepts `aggregations`.
- [ ] 6.2 Full unit suite, CI-style integration suite, `ruff check slayer/ tests/`, basedpyright (baseline not grown), `tools/arch_check.py` — all green.
