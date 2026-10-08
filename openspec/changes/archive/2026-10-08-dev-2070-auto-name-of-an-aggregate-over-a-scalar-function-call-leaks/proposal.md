## Why

An unnamed expression aggregate whose source contains a date-literal comparison, a nested aggregate or a nested transform gets a result key built from an internal key's repr (`sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))` → `regions.customers.orders.iif_op_operand_columnkey_pat_4a289c3a_alse_1_0_sum`), which agents cannot predict or reference. The cause is structural: the leaf is rendered by a partial copy of the formula-text renderer that falls back to the frozen key repr for every kind it does not handle (system §3.13), and the set of expression source kinds is kept twice, so a top-level date comparison takes a different naming path from every other comparison.

## What Changes

- The derived leaf of an expression aggregate is the sanitized formula text of its bound source for every operand kind — date-literal comparisons (fixed or relative points, bucketed operands), nested aggregates and nested transforms included — never an internal representation. Decimal literals spell as plain decimals (`0.0000001`, not `1e_7`) and `null` as `null`.
- **BREAKING**: row-level column operands are spelled relative to the expression's home path, which already prefixes the result key: `sum(customers.spend - 1)` from `orders` becomes `orders.customers.spend_1_sum` (was `orders.customers.customers_spend_1_sum`). Attached constituents (nested aggregates / transforms) keep their own spelling. Root-homed expressions are unchanged.
- **BREAKING**: a top-level date comparison source names like every other comparison: `sum(ordered_at >= '2024-02-01')` → `orders.ordered_at_2024_02_01_sum` (was `orders.sum_ordered_at_2024_02_01`).
- Garbled keys of the shapes above become readable; previously readable root-homed keys are unchanged except those with a decimal or `null` operand (`orders.amount_1e_7_sum` → `orders.amount_0_0000001_sum`). The length cap with stable hash fold, parametric / partition suffixes, `name=` override and the duplicate-key error are unchanged.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `aggregations/expression-aggregation`: the "Expression result keys are deterministic" requirement gains the every-operand-kind formula-text rule, the home-relative operand spelling and the date-comparison rule, with scenarios for each.

## Impact

- `slayer/core/refs.py` (`_value_key_display` deleted; `EXPRESSION_SOURCE_KINDS` gains `TimePointCmpKey`; `key_display` docstring), `slayer/engine/binding.py` (`_BOUND_EXPRESSION_SOURCE_KINDS` deleted), `slayer/sql/naming.py` (new home of the expression-leaf derivation with the home-relative renderer), callers in `slayer/sql/generator.py`, `slayer/engine/key_metadata.py`, `slayer/engine/response_meta.py`.
- Saved queries whose filters / order reference an old auto-name of a pathed expression aggregate, a top-level date comparison, or an expression with a decimal or `null` operand by its derived key must switch to the new key (auto-names are not persisted, so no migration applies).
- Tests: new executed SQLite + DuckDB naming tests; `tests/test_dev1871_alias_stability.py` pins of the repr fallback rewritten / removed (approved); `tests/golden/dev1859_sql_baseline.json` aliases containing `source_columnkey…` re-blessed.
- Docs: one sentence in `docs/concepts/formulas.md` (Naming).
- No arc42 / LikeC4 change.
