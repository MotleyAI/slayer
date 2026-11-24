## Why

A stored 1.x filter written with a single `=` (the documented 1.x operator, e.g. `status = 'paid'`) is judged "not in the grammar its version claims", so the SQL-string upgrade skips it: `status = "paid"` keeps its double quotes and 2.0 reads `"paid"` as an identifier, changing the filter's meaning, and `status = 'ok'` logs a false warning. The gate approximates 1.9.1's reader by hand (only its colon and `*` passes), so every other 1.x spelling Python rejects drifts the same way. A 1.x store must load in 2.0.0 with its 1.x meaning (DEV-2372), so this gates 2.0.

## What Changes

- The stored-text gate decides "old grammar" with a frozen copy of 1.9.1's whole pre-parse chain, per surface: on a query filter the SQL operator spellings (`=`, `<>`, `NULL`, keyword-cased `AND` / `OR` / `NOT` / `IS` / `IN`, `[NOT] LIKE '…'`, `||`); on every Mode-B surface `CASE … END`, `extract(<part> from x)`, `count(distinct x)`, colon aggregations, `*` arguments and `{var}` placeholders.
- Such text gets the existing token-exact SQL-string upgrade and no warning; text 1.9.1 refused (`BETWEEN`, `::`, `CAST`, `FILTER (WHERE …)`, `{? ?}` blocks, a measure's `||`, `LIKE "…"`) is still left unchanged with the warning.
- The hand-written colon / star approximation, including its spaced-colon tolerance no release accepted, is replaced.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/stored-upgrade`: "Stored Mode-B text is upgraded to the SQL string and identifier rules" defines the grammar of the time as every spelling that release accepted on that surface, with scenarios for `=`, the SQL filter spellings and the 1.x expression spellings.

## Impact

- `slayer/storage/sql_string_migration.py` (gate), new `slayer/storage/legacy_grammar.py` (frozen 1.9.1 acceptance), `slayer/storage/rank_direction_migration.py` (walker tells the gate which fields are filters).
- Stored SlayerQuery ≤ v6, SlayerModel ≤ v15 and Memory ≤ v4 documents; no API, dependency or docs change.
