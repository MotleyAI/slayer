## Context

See proposal.md — Why. DEV-1999 already types `date_trunc(unit, x)` by `x` in `core.keys.temporal_type` (`TimeTruncKey` → source type), so the issue's three examples bind and run today. What is wrong is emission. `SqlDialect.build_bucket(col_expr, granularity)` has no operand type, and each dialect returns its engine's own type:

- Postgres `DATE_TRUNC` on a DATE gives `timestamptz` (checked live, PG 16).
- DuckDB gives a TIMESTAMP.
- MySQL `STR_TO_DATE` gives a DATE for a DATETIME month.
- SQLite gives `'YYYY-MM-DD'` for every day-or-coarser unit.
- Snowflake keeps the source type, so `HOUR` on a DATE is a DATE (checked live).
- ClickHouse `dateTrunc` returns `Date` or `DateTime` by unit.
- The base `build_bucket` for custom granularities always computes in TIMESTAMP.

Values of mixed DATE / TIMESTAMP are never aligned. The time-spine join compares a TIMESTAMP spine bucket with the axis bucket by null-safe equality (`render/joins.build_grain_joinback_condition`, which receives untyped expression pairs). BigQuery's `bucket_comparand` patches one instance of this (time shift).

Principles in force:
- system P4 (AST), P10 (one typing), P11;
- sql P1 (hooks take typed, non-`str` operands), P2 (quirks only in `dialects/`), P5 (one ValueKey renderer pair), P7 (one join-back builder), P9 (fail closed);
- core P3 / P4 (total traversal, fail-closed dispatch);
- new sql P14 (decision 6).

## Goals / Non-Goals

**Goals:**
- One rule for a bucket's type, in `core`.
- Every dialect emits exactly that type.
- One rule for a DATE meeting a TIMESTAMP, decided in `core` and applied by the renderers, the join-back builder and the spine.
- The class is pinned by P14 and its enforcing test.

**Non-Goals:**
- Time zones and zoned physical types beyond the naive DATE/TIMESTAMP contract (DEV-2000).
- Exporting buckets to Ossie as standard SQL (DEV-2266).
- SQLite storage spelling in literal comparisons keeps DEV-1999's path; only the canonical form's precision changes.
- Backporting to 1.x.

## Decisions

1. **Premise.** The typing half of the issue is done (DEV-1999). The remaining scope is emission, mixed DATE/TIMESTAMP, SQLite spelling-independent temporal comparisons, and the time-points spec text. Alternative: re-implementing the typing — rejected, it exists. (default)

2. **Bucket type rule:** DATE iff the source is DATE and the unit (a custom granularity's base) is day or coarser; else TIMESTAMP. It is one function in `core` (beside `granularity_parts` / `nests_into`), read by `temporal_type` and `value_type`, so stage schemas, plan types and time-point checks follow it. Alternatives: always TIMESTAMP (date buckets would show as midnight timestamps, needs promote-and-trunc everywhere, and loses the DATE resolution time points check); source type with sub-day-of-DATE refused (breaks legal `hour` time dimensions on DATE columns and the sub-day spine over a DATE axis). (user)

3. **DATE meeting TIMESTAMP reads as midnight.** The DATE side is promoted through the dialect's `promote_to_timestamp` in:
   - comparisons, `in` and null-safe equality;
   - grain join-backs, the spine join included.

   This is in this change because rule 2 would otherwise regress `date_trunc('day', ts) = d` and the SQLite spine join. Checked against Ossie (apache/ossie `core-spec`): it defines no DATE/TIMESTAMP coercion and defers to plain SQL, which reads a DATE as midnight. That's consistent with DEV-2280's midnight reading of date-only strings. Alternative: join-backs only — still regresses a filter that works today. (user)

4. **Literals convert to the column's type; the column never converts.** Time-point lowering already does this. A spine bound applied to a DATE axis becomes the DATE literal `ceil_day(bound)`, which is exact for `>=` and `<`, and sargable. (default)

5. **Conditionals.** The DATE value arms of `coalesce` / `ifnull` / `greatest` / `least` / `iif` / `CASE` typed TIMESTAMP are promoted. (user)

6. **New principle in `architecture/python.sql.arc42.md`** (user, approved wording):
   > 14. **Emitted types are logical types**: on every dialect a rendered value has the type its key derives (system P10), and a DATE meeting a TIMESTAMP reads as its midnight instant. [enforced: test:tests/test_dev2018_logical_types.py]

7. **One operand-coercion door.** `core.keys` gains the DATE-in-TIMESTAMP-position rule beside `numeric_boolean_positions`. Both renderers apply both rules through one function that generalises `numeric_parts`, so boolean→int and DATE→TIMESTAMP share one call path in `render_value_key` and `render_row_expression`. Alternative: a separate pass per rule at each call site — rejected, two paths that must agree by hand. (default)

8. **Typed bucket arithmetic; `bucket_comparand` deleted.** `bucket_offset` / `bucket_step` take the bucket's own type, so a shifted bucket has its unshifted bucket's type. BigQuery's `bucket_comparand`, which exists only to bridge that mismatch, is deleted along with its two generator call sites (time shift, consecutive periods). (default)

9. **`build_bucket(col_expr, granularity, operand: DataType)` returns exactly `bucket_type(operand, granularity)`.** It's a typed hook (sql P1). For custom granularities, the arithmetic runs in TIMESTAMP, then the result is demoted through a new `demote_to_date` hook (default `CAST(… AS DATE)`) when the rule says DATE.

   Per-dialect obligations, each verified by executed or emission tests:
   - **Base / Postgres / DuckDB:** truncate `CAST(x AS TIMESTAMP)`; cast DATE results back. Never `timestamptz`.
   - **SQLite:** a TIMESTAMP bucket is canonical `YYYY-MM-DD HH:MM:SS` text at every unit; a DATE bucket is `YYYY-MM-DD`.
   - **MySQL:** day-or-coarser buckets of a DATETIME are cast to DATETIME.
   - **ClickHouse:** results are wrapped to `Date` / `DateTime64`.
   - **Snowflake, T-SQL, Trino, BigQuery:** the source is promoted before a sub-day bucket of a DATE.
   - **BigQuery zoned `TIMESTAMP`:** reads through `read_column` exactly once.
   - **`week_sunday`:** its recursion keeps the original operand type.
   - **Tier-2:** inherits base.

   (default)

10. **Ships on main (2.0) as a BREAKING output-type change; no 1.x backport.** (default)

11. **No new capability.** Deltas go on `queries/date-functions`, `queries/time-points` and `queries/mixed-type-coercion`; their attachment in `architecture/model.c4` is unchanged. (default)

12. **pg facade.** `facade/translator._record_time_grain` advertises the bucket's type (through the core rule), not the source column's, so OIDs and value encoding match what's returned. (codex)

13. **`nullif` keeps its first argument's type.** A mixed `nullif(a, b)` renders as `CASE WHEN <a as midnight> = b THEN NULL ELSE a END`, so the comparison reads as midnight without retyping the result. This follows the existing spec ("nullif is not a value-returning conditional"). Alternative: promoting its first argument — rejected, it changes `nullif`'s documented type. (codex)

14. **Every generator bucket site is typed by construction.** `_build_date_trunc` takes the `TimeTruncKey` plus the scope's `column_type`, not a bare expression. Its call sites are:
    - base grouping;
    - trailing-window dimensions;
    - ranked expressions;
    - regroup rendering;
    - time-shift re-bucketing;
    - consecutive periods;
    - the spine series.

    None can omit or guess the type. (codex)

15. **Typed join-back pairs.** `_keyed_pairs` keeps each member's key. `build_grain_joinback_condition` takes `(left, left_type, right, right_type)` pairs and applies decision 3. Time-shift and consecutive-period bucket members carry their key, not `None`. A temporal pair with an unknown type fails closed. (codex)

16. **Coverage.** Coverage beyond the plan's first matrix:
    - BigQuery zoned-TIMESTAMP bucket and spine emission;
    - a two-stage propagation and re-bucketing test;
    - trailing windows, `whole_periods_only`, consecutive periods and ±`time_shift` over DATE and TIMESTAMP;
    - `week_sunday` boundaries;
    - SQLite storage spellings and invalid text through buckets and comparisons;
    - Tier-2 inherited emission.

    Goldens are re-blessed only after confirming each changed line comes from a bucket or a promotion. (codex)

17. **Rejected Codex finding: "mixed-type coercion exceeds the tracker scope; split it out".** Rejected because the user put it in scope (decisions 3 and 5): rule 2 alone would regress working filters and the spine join. (rejected (codex))

18. **SQLite temporal comparisons are spelling-independent.** On SQLite, any comparison, `in`, null-safe equality or join-back between two non-literal temporal values renders both sides through SLayer's canonical-timestamp function (six fractional digits), so distinct instants stay distinct to the microsecond. That covers DATE-only, TIMESTAMP-only and mixed pairs. Literal bounds keep DEV-1999's sargable path. Alternative: guarantee canonical storage only — rejected, the headline SQLite fix would hold for one spelling only. No existing issue covered it (closest DEV-1381, TEXT date formats for encoders). (user)

## Risks / Trade-offs

- [ClickHouse `dateTrunc` return types vary by version] → explicit `toDate` / `toDateTime64` wrappers; the ClickHouse integration workflow runs the type matrix.
- [BigQuery has no credentials locally] → emission tests only; the DATE-vs-DATETIME join is removed structurally (decisions 3 and 8), not by probing.
- [SQLite column-vs-column comparisons lose index use (decision 18)] → SQLite only, and those comparisons are rarely indexable anyway.
- [Visible output types change (BREAKING)] → noted in the proposal and docs. Goldens and notebooks are regenerated from executed results, never hand-edited.
- [Promotion makes a DATE column non-sargable in a column-vs-column comparison] → the literal path (decision 4) keeps bounds sargable.

## Migration Plan

No stored data changes. Saved queries and models keep their text, and only emitted SQL and returned value types change. Rollback is reverting the change.
