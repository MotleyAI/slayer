## Why

`{var}` placeholders substitute only in `SlayerQuery.filters` and the four Mode-A model surfaces. `rate:avg * {amount} / 10000` fails with `unsupported AST node Set`, so every "fee at a given amount" question needs its own hand-written formula, and the error doesn't point at the cause. The restriction is inherited, not principled: formula surfaces use the same Mode-B parser as filters.

## What Changes

- `{var}` substitutes in every Mode-B formula surface: query measure formulas, computed-dimension expressions (string and object form), order expressions, and saved `ModelMeasure.formula` on the models whose Mode-A surfaces already substitute. Python escaping regime, as for filters.
- Time-dimension `date_range` bounds substitute and are then shape-checked; a bound holding a placeholder is accepted at construction. Time-dimension granularities and columns are not substitutable, and a placeholder there raises a clear error.
- Unnamed measures and computed dimensions take their result names from the pre-substitution template, so result columns (including a saved query-backed model's columns) never depend on variable values.
- The Mode-B parser recognises `{name}` as a placeholder, so templates classify correctly at construction. An unsubstituted placeholder that reaches binding raises a typed "looks like a variable placeholder" error instead of `unsupported AST node Set`.
- Unchanged semantics carried to the new surfaces: `{? ... ?}` blocks are rejected; an undefined variable raises the filter error; list values render a Mode-B tuple. Literal-only enforcement is out of scope (DEV-2044); joined / cross-model lineages are out of scope (DEV-1678).
- Model variable inspection lists saved-measure placeholders.
- **BREAKING:** saving a query-backed model renders it exactly as execution does with no runtime variables; a placeholder without a default refuses the save, naming the variable. The save-time `0` fill is removed.

## Capabilities

### New Capabilities
- `queries/variables`: which query and model surfaces substitute `{var}`, with which regime, naming from the template, construction-time acceptance, and the error surface.

### Modified Capabilities
- `queries/date-range`: a bound containing a `{var}` placeholder is accepted at construction and shape-checked after substitution.

## Impact

- `slayer/engine/syntax.py` (placeholder node, every `ParsedExpr` dispatcher), `slayer/engine/binding.py` / `bind_inputs.py` (fail-closed error, template-derived names).
- `slayer/core/query.py` (construction-time classification, `date_range` acceptance, `extract_placeholder_names`, `extract_model_variables`, block-rejection wording), `slayer/core/models.py` / `query.py` (template private attribute), `slayer/core/errors.py` (typed error).
- `slayer/ir/variables.py` (substituted surfaces, rebuild, saved-measure pass, `0` fill removed), `slayer/engine/query_engine.py`, `slayer/engine/plan.py`, `slayer/engine/bundle_builder.py`, `slayer/ir/source_bundle.py` (`dry_run_placeholders` removed; save refuses undefaulted variables).
- Docs: `docs/concepts/queries.md`, `docs/concepts/models.md`, `docs/examples/14_variable_substitution`, MCP agent instructions, help memories.
