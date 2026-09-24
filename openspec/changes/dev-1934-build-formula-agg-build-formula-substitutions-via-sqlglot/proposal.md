## Why

The aggregation render path passes SQL around as text: `_build_formula_agg` substitutes
`{value}`/`{param}` by `str.replace` and re-parses, `_resolve_value_sql` emits an AST only
for every caller to re-parse it, and the dialect aggregate hooks f-string SQL and take a
`parse` callback. This violates `sql.arc42.md` §3.1 / `system.arc42.md` §3.6 (SQL is built
as AST; no text round-trips) and lets placeholder text leak into string literals and
precedence depend on hand-added parens.

## What Changes

- New `SqlTemplate` (sql root): a formula template parsed once, placeholders found by
  token (string literals / comments inert), each placeholder a collision-free sentinel in
  expression position; `render(bindings)` substitutes copies of binding ASTs structurally,
  parenthesising an operator-shaped binding under an operator parent.
- One expression-parse pipeline (`parse_expression`) shared by the generator and
  `SqlTemplate`.
- Generator: value / parameter resolution returns AST; `_build_formula_agg` renders via
  `SqlTemplate`; `_paren_fragment` deleted; the picked-value `MAX(...)` stops re-parsing;
  percentile `p` validated as a numeric literal (optionally signed / parenthesised).
- Dialect hooks (`build_median`, `build_percentile`, `build_approx_count_distinct`,
  `build_stat_agg_1arg`, `build_covar_2arg`, covar decomposition, `build_date_trunc`,
  `build_time_offset_expr`) take typed AST / closed-Literal operands and no `parse`
  callback; SQLite week/quarter truncation built as AST.
- `approx_count_distinct_template` removed: exact `COUNT(DISTINCT)` by default, a
  native-approx flag emitting sqlglot `ApproxDistinct`, Oracle / T-SQL keep the anonymous
  name.
- Save-time: a model whose aggregation formula does not parse is rejected at every engine
  create/edit door.
- MCP `create_model` gains an `aggregations` parameter (parity with `edit_model` / REST),
  so a custom aggregation can be declared at creation.
- `SqlDialect._outer_order_column` resolves a hidden-hoist ORDER BY column against the
  inner statement's projected alias set (supplied by the generator through
  `emit_outer_wrap`) instead of scanning `inner_sql` text.
- Law test `tests/test_law_ast_dialect_hooks.py` + approved `sql.arc42.md` §3.1 edit.
- Out of scope: the rest of statement assembly / `emit_outer_wrap`'s `inner_sql: str` (DEV-1965).

## Capabilities

### New Capabilities
- `aggregations/formula-templates`: how `{value}`/`{param}` placeholders in an aggregation
  formula are recognised and substituted, and when a template is rejected.

### Modified Capabilities
- `models/save-validation`: adds the save-time aggregation-formula parse check and the
  MCP `create_model` `aggregations` parameter.

## Impact

- Code: `slayer/sql/generator.py`, new `slayer/sql/sql_template.py`, all
  `slayer/sql/dialects/*.py` aggregate / date-trunc hooks and `_outer_order_column`,
  `slayer/engine/query_engine.py` (`save_model`), `slayer/mcp/server.py` (`create_model`).
- Goldens: re-blessed only where canonical AST emission changes parens / whitespace.
- Architecture: `architecture/sql.arc42.md` §3.1 gains an `[enforced:]` tag (approved);
  no `.c4` / `index.yaml` change.
- MCP `create_model` gains an optional `aggregations` argument; internal dialect-hook
  signatures change.
