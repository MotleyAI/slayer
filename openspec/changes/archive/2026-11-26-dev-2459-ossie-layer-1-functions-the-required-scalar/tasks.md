## 1. Tests first (pr-tests)

- [x] 1.1 Flip the gate rows in `tests/_dev2446_required_manifest.py`: every DEV-2459 `gap` row and the `REGEXP_COUNT` / `RADIANS` / `DEGREES` `refused` rows become `ok`, except `REGEXP_COUNT` refused on MySQL (no translation) and on the gate's SQL Server 2022; the gate test fails on them until implemented
- [x] 1.2 Parse and twin tests for every spelling (`IF`, `NVL`, `NVL2`, `STARTSWITH`, `ENDSWITH`, `CHARINDEX`, `TRUNCATE`, `TO_DATE`, `TO_TIMESTAMP`, `TIMESTAMP_NTZ '…'`, `DAYOFWEEK` / `MILLISECOND` parts): same result key and SQL as the canonical twin; wrong arities refused naming the function
- [x] 1.3 One executed scenario per function on DuckDB and SQLite (date-functions, scalar-functions and regex spec scenarios), including the `MILLISECOND` cases `.123`, `.001999`, `.999999`, exact second, pre-epoch and DATE, and `DAYOFWEEK` on a Sunday and a Monday
- [x] 1.4 Library-matrix cases in `tests/test_dev2120_library_matrix.py` for every new function on every Tier-1 dialect, plus emitted-SQL checks for `millisecond` / `day_of_week` on every Tier-1 dialect
- [x] 1.5 Refusal tests: `dayofweek(x)`, `millisecond(x)` (errors name the part spellings), `dow` / `weekday` parts, `TO_DATE(s, fmt)`, `CHARINDEX` with 3 args, `round` / `trunc` with fractional literal and DOUBLE-column digits, `ZEROIFNULL` / `NULLIFZERO` over a TEXT field (at save and at query time)
- [x] 1.6 `regexp_count` tests: counts and `'i'`, NULL input, empty-match pattern (4, or the typed refusal), MySQL refusal, PostgreSQL 14 / 15 and SQL Server 2022 / 2025 boundaries on execution and dry run
- [x] 1.7 Strict-binding tests (`ossie-l3`): the new functions admitted at Ossie's argument counts; one-argument `TRUNC`, wrong-arity `TRUNCATE`, 3-argument `CHARINDEX` and `REGEXP_COUNT`, `starts_with` and `DAYOFWEEK(x)` refused with `E_INVALID_QUERY`
- [x] 1.8 Library-growth tests: custom aggregations named `nvl`, `if`, `truncate`, `to_date`, `millisecond` and `dayofweek` keep rendering the model's aggregation (`tests/test_library_growth.py` covers every newly reserved name)
- [x] 1.9 Registry law: `OSSIE_CALL_SPELLINGS` and `SCALAR_SPELLINGS` derive from the one registry, and the parser's builder table has exactly the rewrite entries

## 2. Registry and parser

- [x] 2.1 Add the Ossie spelling registry to `slayer/core/keys.py` (dispatch kind, Ossie argument counts); derive `SCALAR_SPELLINGS` and `ossie_conformance.OSSIE_CALL_SPELLINGS`; verify with 1.9
- [x] 2.2 Add the spelling builders to `slayer/core/syntax.py` (`IF`, `NVL`, `NVL2`, `STARTSWITH`, `ENDSWITH`, `CHARINDEX`, `TRUNCATE`, `TO_DATE`, `TO_TIMESTAMP`), with the written-call record on the `CAST`-producing ones; verify with 1.2 and 1.8
- [x] 2.3 `TIMESTAMP_NTZ` keyword in the typed-literal grammar, parsing to the `timestamp` kind; verify with 1.2 and the literal scenarios
- [x] 2.4 `DatePart.MILLISECOND` and the part-only vocabulary (`dayofweek` → `day_of_week`), read by `date_part` / `extract` part parsing only; `dayofweek(x)` / `millisecond(x)` refusals naming the part spellings; verify with 1.2 and 1.5

## 3. Library functions and typing

- [x] 3.1 `SCALAR_SPECS`: `trunc` 1–2 args (Ossie 2), `zeroifnull` / `nullifzero` (numeric), `radians` / `degrees` (DOUBLE), `regexp_count` (INT, Ossie 2); verify with 1.4
- [x] 3.2 The shared integer-digits check for `round(x, d)` / `trunc(x, d)` (plan time for literals, type for columns) and `trunc(x, d)` result typing as `round(x, d)`; verify with 1.5
- [x] 3.3 Save-time numeric check for `zeroifnull` / `nullifzero` through the existing numeric-function type error; verify with 1.5

## 4. Dialects

- [x] 4.1 `trunc(x, d)` hook on every Tier-1 dialect (SQLite registered function; emulation where the native form differs); verify with the trunc scenarios in 1.3 / 1.4
- [x] 4.2 `millisecond` part on every Tier-1 dialect (SQLite reads the microsecond-preserving timestamp text; truncation everywhere); verify with 1.3 / 1.4
- [x] 4.3 `zeroifnull` / `nullifzero` / `radians` / `degrees` lowering on every Tier-1 dialect; verify with 1.4
- [x] 4.4 `regexp_count` per dialect (MySQL refusal, PostgreSQL 15 and SQL Server 2025 gates, empty-match handling); verify with 1.6
- [x] 4.5 `docs/database-support.md` per-dialect function lists agree with the code; verify with the existing agreement test

## 5. Docs and records

- [x] 5.1 `docs/interfaces/ossie-compliance.md`: drop the rows this change closes, update the per-database counts, replace the "Ossie does not fix DAYOFWEEK / MILLISECOND" paragraph with the rulings; verify with `tests/test_dev2220_compliance_page.py`
- [x] 5.2 `docs/concepts/references.md` (functions, parts, the weekday sentence), `docs/concepts/expressions.md`, `docs/concepts/time.md`, agent help `slayer/memories/help_content/07_queries.md`; verify `tests/test_doc_examples.py`
- [x] 5.3 `docs/upgrading/2.0.md`: `round(x, d)` needs whole-number digits
- [x] 5.4 Comment on DEV-2452 recording the `DAYOFWEEK` (ISO), `MILLISECOND` (component) and no-function-form rulings, with links to DEV-2486 and DEV-2487

## 6. Gates

- [x] 6.1 Local test scope passes (`la-affected-tests` + the gate, library matrix and doc tests); `ruff`, `basedpyright` (no new errors), `la-arch-check` green
