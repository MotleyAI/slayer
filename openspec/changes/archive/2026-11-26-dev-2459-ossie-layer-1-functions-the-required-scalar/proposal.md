## Why

Ossie Layer 1 (`OSSIE_SQL_2026`) lists REQUIRED scalar functions that Mode B still refuses, so a valid Ossie expression using them fails in SLayer, and the `DAYOFWEEK` and `MILLISECOND` parts have no meaning because Ossie defines none. Storage is about to become Ossie documents in a final format, so the meaning of every stored expression must be fixed now.

## What Changes

- Ossie spellings become parse-time spellings of existing library nodes: `IF` (= `IFF` = `iif`), `NVL` (= `ifnull`), `NVL2(e, a, b)` (= `iif(e IS NOT NULL, a, b)`), `STARTSWITH` / `ENDSWITH` (= `starts_with` / `ends_with`), `CHARINDEX(sub, s)` (= `instr(s, sub)`), `TRUNCATE` (= `trunc`), `TO_DATE(s)` (= `CAST(s AS DATE)`), `TO_TIMESTAMP(s)` (= `CAST(s AS TIMESTAMP_NTZ)`).
- New library functions: `zeroifnull(x)` and `nullifzero(x)` (numeric), two-argument `trunc(x, d)`, `radians(x)`, `degrees(x)` and `regexp_count(x, p[, 'i'])`.
- `TIMESTAMP_NTZ '…'` is a timestamp literal, the same as `TIMESTAMP '…'`.
- `EXTRACT(DAYOFWEEK FROM x)` / `DATE_PART('dayofweek', x)` mean ISO numbering, Monday 1 to Sunday 7 (ruling; proposed upstream).
- New `date_part` part `millisecond`: the millisecond component, 0 to 999 (ruling; proposed upstream).
- `dayofweek(x)` and `millisecond(x)` have no function form and are refused with an error naming the part spelling; `dow` and `weekday` stay refused.
- **BREAKING** `round(x, d)` and `trunc(x, d)` require an integer-valued `d`; `round(x, 1.5)`, which gave a database-dependent answer, is refused with a typed error.
- The strict (`ossie-l3`) binding admits exactly these constructs at Ossie's argument counts, read from one registry of Ossie spellings.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/date-functions`: `millisecond` part; `dayofweek` as a part word; the weekday-name refusals; `TIMESTAMP_NTZ` literals; `TO_DATE` / `TO_TIMESTAMP`.
- `queries/scalar-functions`: Ossie conditional and string-search spellings; `zeroifnull` / `nullifzero`; `trunc(x, d)`, `truncate`, `radians`, `degrees`; integer digits for `round` / `trunc`; `regexp_count` version gate and MySQL refusal; format parsers.
- `queries/regex`: `regexp_count`.

## Impact

- `slayer/core/syntax.py` (spelling table, typed-literal keyword, part vocabularies), `slayer/core/keys.py` (library specs, the Ossie spelling registry), `slayer/core/enums.py` (`DatePart.MILLISECOND`, part-only words), `slayer/core/ossie_conformance.py` (derived admission).
- Binding and type checking for the numeric and integer-digits rules; `slayer/sql/render/row_expr.py` and `slayer/sql/dialects/*` for the new functions and the `millisecond` part.
- Tests: the Ossie gate manifest, the library matrix, parse, strict-validator and library-growth tests.
- Docs: `docs/interfaces/ossie-compliance.md`, `docs/concepts/references.md`, `docs/concepts/expressions.md`, `docs/concepts/time.md`, `docs/database-support.md`, `docs/upgrading/2.0.md` (the `round` digits refusal), agent help `slayer/memories/help_content/07_queries.md`.
