## Context

See proposal.md - Why. `DATEADD`, `DATEDIFF`, `CURRENT_TIMESTAMP` / `CURRENT_TIMESTAMP()` and `IFF` already pass on every Tier-1 database (`ok` rows of `tests/_dev2446_required_manifest.py`); this change adds library-matrix cases for them only where missing.

Ossie spellings already collapse into library nodes in the parser's spelling table (`DATEADD` → `date_add`, `IFF` → `iif`, `CURRENT_TIMESTAMP` → `now`), a spelled call keeps the written callee (`written` / `written_args`) so binding can read it as a model's own aggregation of that name (storage P3.2, `tests/test_library_growth.py`), and a saved dataset keeps the author's text (engine P8). Three lists decide what the strict binding admits and must agree by hand today: `SCALAR_SPECS.ossie_arg_counts`, `ossie_conformance.OSSIE_CALL_SPELLINGS` and the parser's spelling table.

Principles that apply: system P8 (one reader of Mode B), P11 (fail at save time); engine P2 (each spelling collapses to one node), P8 (models persist verbatim); semantics 2, 2.1, 2.2 (one library, one meaning on every dialect, typed refusal) and 4 (Ossie superset); sql P1 (AST), P2 (dialect quirks only in `dialects/`), P9 (fail closed), P14 (logical types). No arc42 or `.c4` edit is needed; the deltas modify the cross-cutting `queries` spec group.

## Goals / Non-Goals

**Goals:**
- Every REQUIRED Ossie scalar function listed in the issue evaluates with Ossie's meaning on every Tier-1 dialect, or is refused with a typed error where a dialect has no translation.
- The strict binding admits exactly what runs, by construction.
- The `DAYOFWEEK` and `MILLISECOND` meanings are fixed before the final storage format, and proposed upstream.

**Non-Goals:**
- `TO_DATE(s, fmt)` / `TO_TIMESTAMP(s, fmt)` and `TO_CHAR` (Ossie EXPERIMENTAL).
- `dayofweek(x)` / `millisecond(x)` function forms; `dow` / `weekday` stay refused.
- `MILLISECOND` as a `date_add` / `date_diff` / `date_trunc` unit (Ossie lists it only as an extraction part).
- Time-of-day (`CURRENT_TIME`, `TIME`), owned by DEV-2460; windows (DEV-2457); aggregates (DEV-2458).

## Decisions

1. **Spellings reduce to existing nodes.** `IF`, `NVL`, `NVL2`, `STARTSWITH`, `ENDSWITH`, `CHARINDEX` (arguments swapped), `TRUNCATE`, `TO_DATE`, `TO_TIMESTAMP` are entries of the parser's spelling table producing `iif`, `ifnull`, `iif(e IS NOT NULL, …)`, `starts_with`, `ends_with`, `instr`, `trunc` and `CAST` nodes. Alternative: new library functions per spelling, rejected (engine P2, one node per meaning). `TO_DATE` / `TO_TIMESTAMP` produce a `CAST`, which carries no written callee today; they get the same written-call record so a model's aggregation of that name still wins.
2. **`zeroifnull` / `nullifzero` are library functions, not spellings.** As spellings of `coalesce(x, 0)` / `nullif(x, 0)` they would lose the numeric argument contract (`coalesce` is not numeric), so `ZEROIFNULL(status)` would coerce instead of raising the numeric-function type error (`queries/mixed-type-coercion`). As `numeric=True` library functions lowered by the renderer to `COALESCE(x, 0)` / `NULLIF(x, 0)` they keep it, and saving checks it. (codex)
3. **One registry of Ossie spellings.** In `core.keys`, each entry records its dispatch (a parser rewrite or a canonical library name) and its Ossie argument counts (empty for non-Ossie aliases such as `strpos` and the JSON spellings). `SCALAR_SPELLINGS` and `OSSIE_CALL_SPELLINGS` derive from it; a law test asserts the parser's builder table has exactly the rewrite entries' keys. This makes "the strict validator accepts exactly the functions that run" structural. (codex refined the shape)
4. **Part-only words are a separate vocabulary.** `DATE_PART_ALIASES` entries are callable by construction, so `dayofweek` (→ `day_of_week`) goes in a part-only map read by `date_part` / `extract` part parsing and never by call dispatch; `millisecond` is a canonical `DatePart` without a function form. `dayofweek(x)` keeps the ambiguous-weekday refusal, now also naming `EXTRACT(DAYOFWEEK FROM x)`; `millisecond(x)` gets the analogous refusal. (codex)
5. **`TIMESTAMP_NTZ '…'` parses to the existing `timestamp` literal kind**, so date-only promotion to midnight, validation and rendering are shared; the author's spelling survives only in stored text. (codex)
6. **`trunc(x, d)` mirrors `round(x, d)`**: the same result-type rule, negative digits, and one dialect hook (a registered SQLite function; native `TRUNC` / `TRUNCATE` / `ROUND(x, d, 1)` or emulation per dialect).
7. **One digits rule for `round` and `trunc`.** `d` must be integer: a literal checked at plan time, a non-literal by type. `round(x, 1.5)` raised a raw binder error on DuckDB and gave `12.3` on SQLite. **BREAKING** for `round(x, <fractional>)`, documented in `docs/upgrading/2.0.md`. (codex finding; scope by user)
8. **`regexp_count`** reuses the regex pattern language and `'i'` flag. PostgreSQL meaning on edge cases (`regexp_count('abc', 'x*')` = 4, verified on PostgreSQL 16); a dialect that cannot reproduce an empty-matchable pattern's count refuses that pattern at plan time. Version gates: PostgreSQL 15, SQL Server 2025; MySQL refuses (no translation).
9. **`millisecond` on SQLite** reads the microsecond-preserving timestamp text, not `strftime('%f')`; truncation, not rounding, on every dialect. (codex)

## Decisions log

1. `DAYOFWEEK` is ISO, Monday 1 … Sunday 7 (user). Options: ISO; Sunday 0 … Saturday 6 (Snowflake default, PostgreSQL `DOW`, DuckDB); keep refused until Ossie rules. Taken: ISO, the only numbering consistent with Ossie's Monday-start week (`DATE_TRUNC('week')`, apache/ossie PR 816), chosen by ossie-clickhouse and assumed by the ThoughtSpot converter, and SLayer's `day_of_week`. Proposed upstream: DEV-2486.
2. `MILLISECOND` is the millisecond component, 0 … 999 (user). Options: the component; seconds × 1000 including the fraction (PostgreSQL, DuckDB, Spark). Taken: the component, as Ossie defines the sibling `SECOND` as the 0-59 component; ossie-clickhouse chose it. Proposed upstream: DEV-2487.
3. `dayofweek(x)` and `millisecond(x)` have no function form and are refused with a pointer (user). Options: full aliases; parts only. Taken: parts only; the function names mean other numberings on MySQL, Databricks, Snowflake and DuckDB.
4. `REGEXP_COUNT`, `RADIANS`, `DEGREES` are in scope (default): the issue lists them; leaving them out has no pro but less work.
5. The functions that already pass get only missing matrix cases (default; verified as `ok` gate rows).
6. Ossie spellings reduce to existing library nodes or `CAST` (default; engine P2, new concepts reduce to old ones).
7. `trunc(x, d)` mirrors `round(x, d)`: negative digits, typing, hook shape (default).
8. Format-string forms of `TO_DATE` / `TO_TIMESTAMP` stay refused with the unknown-function error (default; the issue names only the one-argument forms, Ossie marks them EXPERIMENTAL, `queries/scalar-functions` already refuses format parsers).
9. `ZEROIFNULL` / `NULLIFZERO` read their argument as a number; TEXT keeps the numeric-function type error, checked at save time (default; Snowflake's signature, system P11, `queries/mixed-type-coercion`).
10. `regexp_count` takes PostgreSQL's meaning on edge cases and is refused where untranslatable (default; DEV-2120 reference-meaning rule, semantics 2.2).
11. One registry for Ossie spellings, derived sets and a law (default; structural fix for three hand-synced lists).
12. `zeroifnull` / `nullifzero` are numeric library functions rather than spellings of `coalesce` / `nullif` (codex).
13. Part-only words are a vocabulary separate from callable aliases (codex).
14. Every new name yields to a model's own aggregation of that name under the existing library-growth law; the `CAST`-producing spellings get the written-call record (codex; the rest of the finding rejected (codex) — the yield mechanism and law already exist).
15. The registry records dispatch kind and Ossie argument counts; the law compares the parser table with the rewrite entries only (codex).
16. `TIMESTAMP_NTZ` literals parse to the `timestamp` kind; tests cover date-only, fractional, malformed and impossible values (codex).
17. `MILLISECOND` tests: `.001999` → 1, `.999999` → 999, exact second, pre-epoch, DATE, on SQLite and DuckDB, and emitted SQL on every Tier-1 dialect (codex).
18. `REGEXP_COUNT` tests: PostgreSQL 14 / 15 and SQL Server 2022 / 2025 boundaries on execution and dry run, empty-match and `'i'` cases (codex).
19. `trunc(x, d)` digits must be integer-valued (codex).
20. `round(x, d)` gets the same integer-digits rule in this change (user). Options: fix here with one shared check; `TRUNC` only and file a `ROUND` issue. Taken: here — one rule and one code path for one argument shape.
21. The strict-binding test keeps one-argument `TRUNC` refused and adds wrong-arity `TRUNCATE` (codex).

## Risks / Trade-offs

- [Upstream Ossie later rules a different `DAYOFWEEK` numbering or `MILLISECOND` meaning] → the rulings are proposed upstream first (DEV-2486, DEV-2487) with the precedent cited; a contrary ruling would need a stored-expression migration, which DEV-2452 owns.
- [`round(x, 1.5)` stops running on SQLite] → its value was arbitrary and DuckDB already rejected it; the migration guide names the change.
- [An engine cannot reproduce PostgreSQL's empty-match count] → the pattern is a literal, so the refusal is decided at plan time, never a silent different count.
- [Two weekday words, `dayofweek` and `day_of_week`] → they mean the same; only the function forms differ, and the refusal names the spelling to use.
