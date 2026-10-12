## 1. Tests (pr-tests stage)

Every equivalence test asserts the same columns, rows, result keys and SQL as the canonical twin,
executed on SQLite and DuckDB (unit tests, not `integration`).

- [x] 1.1 Heal 1 tests (`queries/measure-naming` › Measures may be named after transforms): `change`-named measure vs the `chg` control — identical rows, exact key `<model>.change`, SQL byte-identical after alias substitution; filter / order / outer-stage references; saved measure `rank` bare vs `rank(...)`; dbt, Cube and OSI importers keep a `rank` measure; custom `Aggregation` named `cumsum` still rejected. Verify: tests fail on main.
- [x] 1.2 Heal 3 tests (`queries/date-functions` ADDED + MODIFIED): every alias × {function, `date_part` part, `extract` part} × {dimension, filter, aggregation input, order key}; case folding; wrong alias arity; `week(x)` still a bucket; `extract` over nested operands; unknown `extract` part; `extract` / `from` text inside string literals; `dow` / `dayofweek` / `weekday` explanatory error as function, `date_part` part and `extract` part; custom aggregation `weekday` still works; custom aggregation or granularity named `isodow` / `extract` rejected. Scanner edge cases: newlines, Unicode-adjacent identifiers (`myextract(`), unbalanced input.
- [x] 1.3 Unknown-function tests (`aggregations/functional-form` › Unknown and custom aggregation names defer to binding): `bogus(price)`, `bogus(*)` raise "Unknown function 'bogus'" with categories and close match; construction-time custom filter still deferred.
- [x] 1.4 Heal 4 tests (`aggregations/functional-form` › Syntax boundaries are preserved): `count(distinct x)` / `COUNT(DISTINCT x)` ≡ `count_distinct(x)`; expression source with `partition_by=`; `count(distinct coalesce(a, b))`; quoted `distinct`; `sum(distinct x)`, `count(distinct *)`, `count(distinct a, b)` raise the targeted DISTINCT error.
- [x] 1.5 Heal 5 tests (`aggregations/functional-form` › The method-call spelling is an aggregation spelling): `order_items.count(*)`, `order_items.sum(price * qty)`, `order_items.percentile(price, 0.9)`, `order_items.month(created_at)`, `path.first(row_col)`, uppercase alias (`order_items.COUNTD(id)`) ≡ functional twins; `weighted_avg(price, qty)`, dotted source leaf, `partition_by=`, `path.round(x)` raise the functional-spelling error.
- [x] 1.6 Heal 6 tests (`queries/positions` ADDED, `mcp/tool-surface` ADDED): object-form expression ≡ string form in query `order` and refinement `order`; object with `model` keeps the invalid-name error; MCP `query` schema shows `column` as string | object and no `raw_formula`; a stored order carrying `raw_formula` loads and sorts unchanged.
- [x] 1.7 Heal 7 tests (`queries/partitioned-aggregates` MODIFIED): `partition_by=[month(ordered_at)]` ≡ `partition_by=[ordered_at]` for an aggregation and a rank-family transform; month+year time dimensions pick month; joined time column, custom granularity and stage time column; `week(ordered_at)` vs a month time dimension raises the bucket-naming `PartitionKeyError`.
- [x] 1.8 Heal 8 tests (`queries/saved-query-refinement` MODIFIED): named / unnamed / string-shorthand restatement ≡ refinement without it; per-field (`label`, `type`, `description`, `meta`) omitted / equal / explicit null / differing; restatement under another name appends; a refinement also declaring a different entry named like the saved one still conflicts; existing conflict scenarios unchanged.
- [x] 1.9 Update existing tests whose logic changes (approved in pr-plan): `tests/test_models.py:380`, `tests/test_mcp_query_arg_errors.py` (invalid identifier instead of `rank`), `tests/test_dbt_converter.py:1492`, `tests/test_osi_converter.py:1006`, `tests/test_cube_boundaries.py:28`, `tests/test_functional_aggregations.py:271`; mechanical "Unknown aggregation" → "Unknown function" text in the 5 files asserting it.

## 2. Implementation (pr-implement stage)

- [ ] 2.1 Delete `ModelMeasure._reject_transform_shadowing` and the importer skips (D9); verify 1.1 passes.
- [ ] 2.2 Add the date-part alias table and reserved names in core; wire the alias function and `date_part` part (D2); verify the 1.2 alias cases pass.
- [ ] 2.3 Add the `extract` and `count(distinct …)` token rewriters in `parse_expr` (D3); verify 1.2 extract cases and 1.4 pass.
- [ ] 2.4 Unknown-function and ambiguous-weekday errors at binding (D4); verify 1.3 and the 1.2 weekday cases pass.
- [ ] 2.5 Method-call aggregation conversion (D5); verify 1.5 passes.
- [ ] 2.6 Order column object form and schema hook (D6); verify 1.6 passes.
- [ ] 2.7 Granularity-call partition keys (D7); verify 1.7 passes.
- [ ] 2.8 Refinement restatement (D8); verify 1.8 passes.
- [ ] 2.9 Docs, one sentence each: `docs/concepts/references.md` (aliases, `extract`), `docs/concepts/queries.md` (method call, object-form order expression, `partition_by` on a time bucket), the aggregation concept page (`count(distinct x)`), help topic `slayer/memories/help_content/03_aggregations.md` (drop "never `count(distinct x)`"); verify `grep` finds no stale guidance.
- [ ] 2.10 Run `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors), `poetry run la-arch-check`; all green.
