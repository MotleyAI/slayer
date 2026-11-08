## 1. Tests first (pr-tests stage)

- [x] 1.1 Parser unit tests for the `Placeholder` node: `{k}` in every D1 position (root, arithmetic/unary, comparison, whole `in` / `not in` RHS, aggregation source/args/kwargs incl. `sum({k})`, transform args/kwargs, scalar args, CASE / `iif` branches); `canonical_measure_text` renders `{k}`; `{a, b}` stays unsupported; verify they fail before implementation
- [x] 1.2 Construction tests: string dimension `"amount * {k}"`, call-form order `sum(amount) * {k}`, `date_range: ["{start}", null]` construct; time-dimension granularity `{g}` (dict and `"{g}(ordered_at)"`) and column `{col}` (dict and `"month({col})"`) raise the "variables can't name a granularity / column" error; verify they fail before implementation
- [x] 1.3 `ir/variables` unit tests: every D2 surface substituted (python regime, `O'Brien` escaping, list tuple); `{? ?}` rejected on measure / dimension / order with the reworded message; `_template` set on changed measures and computed dimensions and absent when unchanged; rebuild preserves `model_fields_set`, inline / extension / full `source_model`, version, and a refined saved query; post-substitution `_dedupe_time_dimensions` and date-range shape re-run; `extract_placeholder_names` covers every surface; replace `tests/test_variables_planner.py::test_measure_formula_text_not_substituted` and rewrite the module docstring's stale scope claim; verify they fail before implementation
- [x] 1.4 Executed tests (SQLite + DuckDB, values asserted) for every `queries/variables` scenario: measure, both computed-dimension forms, both order forms, operand positions, quoted escaping, saved measure on the source, saved measure through a join → `UnresolvedPlaceholderError`, variable `date_range`, malformed substituted bound, template-derived key (both spellings), equal template measures merge, filter / order by template name and by substituted formula text, computed-dimension template name kept and still unnamed, query-backed model column names stable across `k=10` / `k=20`, undefined variable, save refusals and the exact backing SQL per D5 (flip the three existing fill-pinning tests `tests/test_query_backed_models.py::TestCreateModelFromQuery::test_save_time_placeholder_fill_for_unresolved_var`, `tests/test_query_backed_typed_expansion.py::TestSavePath::test_save_with_undefined_var_uses_placeholder_fill`, `tests/test_query_backed_models.py::TestQueryBackedColumnTypes::test_get_column_types_with_required_unbound_variable` to the new contract: refused save; no types for a stored model without a default), `{{k}}` → typed placeholder error; verify each fails for the right reason before implementation
- [x] 1.5 Inspection test: `extract_model_variables` / `inspect_model` reports a saved-measure placeholder as required, or as optional when defaulted; verify it fails before implementation
- [x] 1.6 Codex review of the test suite against this change; fold findings, then verify the suite is red only on DEV-2041 behaviour

## 2. Parser and error

- [x] 2.1 `UnresolvedPlaceholderError(SlayerError)` in `core/errors.py` with the message from the spec; verify the error unit test passes
- [x] 2.2 `Placeholder` node in `engine/syntax.py`, admitted in every D1 position, rendered by `canonical_measure_text`, handled or failed closed by every `ParsedExpr` walker / dispatcher; binding raises `UnresolvedPlaceholderError`; verify 1.1 passes and the full unit suite stays green

## 3. Construction

- [x] 3.1 Construction-time classification accepts templates (dimension strings, order candidates); `date_range` placeholder bounds skip the shape check; granularity / column placeholder rejection; verify 1.2 passes

## 4. Substitution and naming

- [x] 4.1 `_template` `PrivateAttr` on `ModelMeasure` and `ComputedDimension`; `apply_variables_to_query` substitutes every D2 surface, sets `_template`, and rebuilds per D2; `extract_placeholder_names` widened; reword the `{? ?}` rejection; verify 1.3 passes
- [x] 4.2 `bind_inputs`: an unnamed measure's public name from the template canonical alias (text path), `canonical_alias` = the substituted alias when it differs, unnamed dedupe kept; computed-dimension explicitness against the template (~L961, ~L1224); verify the naming and reference tests in 1.4 pass
- [x] 4.3 Saved `ModelMeasure.formula` substitution in `substitute_model_sql_surfaces` (python regime); `model_placeholder_names`, `model_needs_substitution_pass`, `extract_model_variables` widened; delete `dry_run_placeholders` and the `0` fill; refuse the save per D5; verify 1.4's saved-measure and save tests and 1.5 pass

## 5. Docs and wrap-up

- [x] 5.1 Docs: `docs/concepts/queries.md` ("Filter Variables" → "Variables" across every surface, plus the `variables` field-table row), `docs/concepts/models.md` (saved measure formulas), `docs/examples/14_variable_substitution` (`.md` + notebook, re-executed with `jupyter nbconvert --execute --inplace`), `SlayerQuery.variables` field description, `slayer/mcp/server.py` agent instructions, `slayer/memories/help_content/*.md`; one sentence on D5's save requirement in `docs/concepts/models.md`; grep docs for "for filter substitution" / "filters only"; verify `zensical.toml` nav needs no change
- [x] 5.2 Full unit suite, integration suite (CI invocation), `ruff check slayer/ tests/`, `basedpyright` (no baseline growth), `la-arch-check` (pinned); verify all green
