## Context

See proposal.md › Why. Probe results (DuckDB and SQLite, `tests/_dev2215_fixtures.py` seed) behind each row:

| Expression | SQLite | DuckDB |
|---|---|---|
| `CAST(qty + 4 AS BOOLEAN)` / `CAST('true' AS BOOLEAN)` | 5 / 0 | true / true |
| `CAST(12.345 AS DECIMAL(10,2))` | 12.345 | 12.35 |
| `CAST(2.5 AS INT)` / `CAST(-2.5 AS INT)` | 2 / -2 | 3 / -3 |
| `ROUND(1250, -2)` / `ROUND(1234.5678, -1)` | 1250 / 1235 | 1300 / 1230 |
| `TRY_CAST(5 AS BOOLEAN)` | NULL | NULL |
| `CAST(12.5 AS DECIMAL)` | 12.5 | 12.500 |
| `CAST('abcdef' AS VARCHAR(3))` | `'abcdef'` | refused (`CastTypeError`) |
| field `expression: AGG(amount)` | saved, runs as `amount` | same |

Strict (`ossie-l3`) filters: `LOG(10, x)` and `CAST(… AS TIMESTAMP_NTZ)` refused (`LOG` is flagged non-Ossie by name because of its one-argument form; `TIMESTAMP_NTZ` is missing from the cast-type list); `APPROX_PERCENTILE(x, p)` and `percentile_disc(x, p)` give `E_AGGREGATE_IN_WHERE`; `ABS(x, 1)` raises a raw `ValueError` (arity is checked at lowering, after the strict judgement).

Structural causes:
- **Casts**: an author's `CAST` is emitted as the engine's native cast once the type spelling survives (`cast_target`); `_CAST_FAMILIES` lets SQLite spell `BOOLEAN` as `INTEGER`. The casts spec delegated value semantics to the database, contradicting semantics §4.2.1 (one meaning on every dialect) and §4.4 (Ossie superset).
- **`ROUND`**: SQLite's native `round` ignores a negative digits argument; no matrix case pinned negative digits.
- **`AGG`**: argument checks run only where a metric binds (`field_dependency`, `binding._bind_evaluated`); a field's expression never meets them, and lowering drops the evaluated flag.
- **Strict**: hand-kept name-only allowlists in `ossie_conformance.py` (`ScalarSpec.ossie` per name, `OSSIE_AGGREGATE_SPELLINGS`, `OSSIE_CAST_TYPES`) drift from the spec and from what SLayer evaluates.

Field `filter` and dataset `filters` are Mode A (semantics §4.2): `AGG` there already reaches the database verbatim (`no such function: AGG`).

Principles that bind the work: semantics §4.2.1, §4.2.2, §4.4; system §3.8 (one Mode-B reader), §3.11 (fail at save time, query-time backstop); python.sql §3.1 (AST end to end), §3.2 (dialect quirks only in `dialects/`), §3.9 (fail closed). No arc42 or `.c4` change.

## Goals / Non-Goals

**Goals:**
- One conversion definition for author casts, lowered per Tier-1 dialect, shared by `CAST` and `TRY_CAST`.
- Admission lists of the strict judgement derived from declared data (Ossie argument counts, SLayer's aggregation registry), cross-checked against the vendored spec by tests.
- A permanent guard: every REQUIRED construct of the vendored spec is pinned as admitted-with-Ossie's-value or as a DEV-2282 gap.

**Non-Goals:**
- Additive Layer-1 gaps (`TRUNC(x, d)`, `PERCENTILE_DISC`, `APPROX_PERCENTILE`, time-of-day type, windows in fields, …): DEV-2282.
- Native mode's raw `ValueError` for a wrong argument count: DEV-2447.
- `AGG` in query positions: the native query language is a documented superset; Ossie Layer 3 queries carry no expression position but the filter, where strict already refuses `AGG`.
- Casts SLayer emits itself (declared column types, `read_column`, JSON reads, list coercion, boolean restoration): unchanged.
- Invalid cast input (text not a literal of the target, out-of-range values): stays the database's own outside SQLite.

## Decisions

1. **Non-integral number → integer or decimal casts round half away from zero; floating-point ties are the database's own** (user). Ossie's CAST section names target types only; ANSI SQL:2003 leaves round-vs-truncate implementation-defined. Half away from zero is the one rounding rule Ossie fixes (`ROUND`'s ties) and DuckDB/Snowflake/BigQuery/Trino/Postgres-NUMERIC's native rule. Rejected: truncation (SQLite/SQL Server/ClickHouse native) — against the majority, and makes `DECIMAL(p, 0)` and `INT` casts of one value disagree.
2. **Stored `AGG(x)` field expressions convert on load (`AGG(x)` → `x`) with a logged warning; fresh input is refused** (default). Refusing on load breaks stored data that runs today; 2.0's policy is upgrade paths, never removals. Converting keeps today's value and the result is valid Ossie. The SlayerModel v30 migration unwraps only `AGG(`; a pre-v20 `agg(` was already rewritten to `agg_aggregation(` by v20 and is left alone.
3. **Every Tier-1 dialect, not only DuckDB and SQLite** (default): semantics §4.2.1 and the issue title. Verified live on SQLite, DuckDB, Postgres; MySQL/ClickHouse/SQL Server/Trino in their integration suites; emission only for Snowflake/BigQuery.
4. **`TRY_CAST` follows `CAST`** (user): `TRY_CAST(x AS T)` is `CAST(x AS T)` where that cast converts, else NULL; text-input rules unchanged.
5. **Three `TRY_CAST` test alignments approved** (user): `tests/_cast_fixtures.py` `TRY_CAST_TARGET_CASES` (`CAST(1.5 AS DOUBLE)`→INT and `1.5`→INT become 2); `tests/test_try_cast_integer_targets.py` `test_fractional_double_column` → `{1: 1, 2: 2, 3: 2, 4: 3, 5: 1}` and the JSON `{"v": 1.5}` → SMALLINT case → 2.
6. **Strict wrong-arity → `E_INVALID_QUERY` in scope; native error → DEV-2447** (user), with its own worktree.
7. **Bare `DECIMAL` and `VARCHAR(n)` casts in scope** (user).
8. **Bare `DECIMAL` / `NUMERIC` = `DECIMAL(38, 0)`; `DECIMAL(p)` = `DECIMAL(p, 0)`** (user): ANSI SQL:2003's omitted scale is 0; precision 38 is the common maximum (BigQuery `BIGNUMERIC(38, 0)`). Rejected: `DECIMAL(38, 9)` — a number of our choosing, contradicting ANSI.
9. **`VARCHAR(n)` truncates to `n` characters** (default): ANSI's explicit cast; Postgres/SQL Server/MySQL/Trino natively. Refusing would break input that runs today. Characters are Unicode code points (codex).
10. **Invalid cast input stays the database's own** (default): Ossie is silent; existing spec.
11. **SQLite: text that is not a boolean word or numeric, and decimal overflow, yield NULL** (default): the existing SQLite timestamp-cast rule (`TRY_CAST`'s value; never a numeric prefix).
12. **Mode-A positions untouched** (default): Ossie — "Expressions in other dialects are not affected".
13. **`CAST(… AS TIME)` stays refused under strict** (default): DEV-2282 owns the time-of-day type.
14. **DEV-2220's "strict xfails" do not exist on main** (verified by search); the REQUIRED-construct table replaces them (default).
15. **The REQUIRED-construct table is the class guard** (default).
16. **AGG in query positions stays a native extension** — rejected (codex): Ossie leaves query-position expressions undefined (§4.4 permits an extension there); strict already refuses `AGG` in its filter.
17. **Migration-chain tests for v30** (codex): from before v20 (stays `agg_aggregation(…)`), from v20–29 (unwrapped), and with a custom `agg` aggregation.
18. **Author casts get their own lowering path** (codex): a dedicated author-cast hook, called only from the render of an author's `CAST` / `TRY_CAST` (`render_cast` in `slayer/sql/render/row_expr.py`); `build_cast` / `build_try_cast` as used by JSON reads, list coercion and other internal sites keep their meaning. Regression tests for declared boolean result casts, aggregate boolean restoration, `read_column`, JSON `RETURNING` of text, list element coercion and temporal comparisons.
19. **Integer `TRY_CAST` sequence** (codex): NULL passes → round half away from zero → range check on the rounded value → native cast guarded inside the `CASE` so constant folding never sees an overflowing cast. Tests at ± half a unit around each signed bound, narrow integers, BIGINT, NaN/infinity where the dialect has them, ClickHouse `Nullable`.
20. **BOOLEAN result contract** (codex): a nullable logical boolean in the dialect's own representation (integer 1/0 on SQLite, MySQL, ClickHouse; `BIT` on SQL Server); existing boolean-as-int restoration at output and aggregation boundaries is kept. Tests for NULL, zero, negative, decimal, filter truth, `= TRUE`, dimension output and aggregate input on ClickHouse, MySQL and SQL Server.
21. **Numeric text grammar and overflow** (codex): numeric text for a decimal target is `TRY_CAST`'s existing number grammar (optionally signed decimal, optional fraction and exponent, spaces trimmed); precision overflow is invalid input (database's own; NULL on SQLite). "One meaning" covers valid, in-range input.
22. **Ossie argument counts are a separate field** (codex): `ScalarSpec` gains the argument counts Ossie defines, consulted only by the strict judgement; `min_args` / `max_args` (native binding) are unchanged. Declared in `slayer/core/keys.py`, never parsed from the spec at runtime; a registry test cross-checks them against the vendored spec's Syntax column. This also refuses, under strict, argument counts the spec does not list: `ROUND(x)`, `TRUNC(x)`, `SUBSTRING(s, start)`, `REGEXP_LIKE` / `REGEXP_REPLACE` with flags; `OSSIE_ARITIES` goes. Consequence (Ossie-determined, mechanical): `tests/test_dev2217_filter.py`'s `round` and `trunc` cases flip to refused and `test_a_scalar_is_admitted_exactly_when_flagged_ossie` judges per argument count.
23. **Aggregate admission from the canonical parse** (codex): admitted only when the written spelling is one Ossie defines, it normalises to a SLayer built-in aggregation, and its shape (arguments, `WITHIN GROUP`, `FILTER`) is supported. Tests for aliases (`STDDEV`, `VARIANCE`, `APPROX_COUNT_DISTINCT`), percentile spellings and shapes, `FILTER`, wrong argument counts.
24. **The REQUIRED-construct guard is a manifest** (codex): every construct of the spec's REQUIRED tables plus the prose-defined requirements (`FILTER`, the `AGG` reservation, numeric semantics, `CAST` / `TRY_CAST`, window NULL order as a DEV-2282 gap), each mapped to its test row; the registry test also fails on a "(REQUIRED)" heading the manifest does not cover.
25. **A JSON number converts as a number** (user): `json_value(… RETURNING INT)` ≡ `TRY_CAST(json_value(…) AS INT)` reads a JSON number with a fraction as a number and rounds it half away from zero (`{"v": 1.5}` → 2) on every Tier-1 dialect; a JSON string stays text (`"1.5"` → NULL). `tests/test_dev2446_internal_casts.py::test_json_returning`'s `1.5` row aligns to 2.
26. **BigQuery decimal casts** (user): BigQuery refuses parameterized types in `CAST`, so `DECIMAL(p, s)` rounds to `s` half away from zero and casts to `NUMERIC` / `BIGNUMERIC`; `TRY_CAST` yields NULL past `p`, as on ClickHouse.
27. **Exponent text on Trino / SQL Server goes through a double** (user): their own decimal cast reads no exponent; an exact reader is dialect-specific SQL for a rare input, so the digits past a double's precision are documented as lost.
28. **A fractional JSON number reads as a double** (user): RFC 8259 §6 limits JSON number interoperability to IEEE doubles, so a JSON number with a fraction converts through the database's double; an integral JSON number reads exactly (out of range → NULL).

## Risks / Trade-offs

- [Breaking value changes on 2.0: SQLite casts and `round`, integer casts on SQLite/SQL Server/ClickHouse, bare `DECIMAL` on DuckDB/SQLite/Postgres/BigQuery, `VARCHAR(n)` on SQLite, fractional `TRY_CAST`] → one migration-guide section per break, each with before/after; release-note lines on DEV-2220.
- [The new author-cast path could leak into internal casts] → separate hook (decision 18) plus regression tests per internal cast kind.
- [Dialects not runnable locally (Snowflake, BigQuery) are emission-checked only] → emission tests assert the rounding / truncation shape; the per-dialect integration suites run the cast matrix where CI has the database.
- [Floating-point ties stay engine-dependent] → documented, as `ROUND` already is; exact-numeric ties are pinned.
- [Strict refuses forms it admitted before (`ROUND(x)`, `TRUNC(x)`, `SUBSTRING(s, i)`, regex flags)] → Ossie-determined; native mode unchanged; listed in the migration guide.

## Migration Plan

SlayerModel v29 → v30 (`AGG(x)` in a field's Mode-B expression unwrapped, logged warning). No rollback beyond the usual: a v30 store is read by this release onward.
