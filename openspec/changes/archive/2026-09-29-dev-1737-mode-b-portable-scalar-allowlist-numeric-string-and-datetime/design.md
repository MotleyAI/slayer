## Context

See proposal.md (Why). Constraints that shape the approach:

- Mode-B scalars are `ScalarCallKey(name, args)`, rendered by the single
  `render_scalar_call` (sql.arc42 P5); `SCALAR_FUNCTIONS` + `SCALAR_FUNCTION_ARITY`
  (`core/keys.py`) are the typed allowlist, while `core/formula.py::SCALAR_PASSTHROUGH` is a
  hand-synced second set also used by OSI to carry SQL functions over verbatim.
- sqlglot 30.17's generic date nodes do not render portable SQL (probed: SQLite `YEAR()`,
  Postgres `DAY_OF_WEEK()`, per-dialect `DATEDIFF` semantics, T-SQL `GETDATE()` for
  `CURRENT_DATE`), so every date primitive needs a SLayer-owned dialect hook.
- SQLite (3.45 here) stores dates as text: no `%V`/`%u`; `DATE()` drops time; month
  modifiers overflow. Three date-arithmetic paths exist today (`build_time_offset_expr`,
  `duration_interval_exprs` + `add_intervals_expr`, and none for Mode B).
- `month(col)` already means the month bucket in dimensions (functional `gran(col)` form).
- There is no general expression typing; declared `Column.type` is reachable in the binder
  and at render (`ScopeFrame.column_type`, stage schemas).

Applicable principles: system P6 (AST), P10 (edited, see D8); sql P1 (typed hook operands),
P2 (quirks only in `dialects/`), P5 (one ValueKey renderer), P9 (fail closed); engine P1
(typed stages), P2 (spellings collapse at parse), P9 (algebra type errors raise in the checker
as `QueryTypeError`); core P1 (keys carry identity only — operand types are derived, never
stored). No new key kind is introduced, so existing `children()` / `map_children()` / generic
walks already cover nested date calls. No new import edges: `temporal_type` sits in `core`.

## Goals / Non-Goals

**Goals:** one pinned meaning per function/unit on all 8 Tier-1 dialects; one date-arithmetic
primitive per dialect; one scalar spec table; typing shared by checker and renderer.

**Non-Goals:** time zones (DEV-2000); relative/period filters (DEV-1999); Mode-B `date_trunc`
(`gran(col)` covers bucketing); 2-arg `ltrim`/`rtrim` (DEV-1793); typing string literals outside
date positions (`created_at >= '2024-01-01'` is unchanged).

## Decisions

**D1 — Generic unit-parameterised functions.** `date_part` / `date_diff` / `date_add` /
`current_date` / `now` (+ parse-only `interval`). Alternatives: one name per part (`day_of_week(ts)`,
`months_between`, …; ~20 reserved names, more wrappers) and bare `year(ts)` (collides with the
`gran(col)` bucket meaning). Units reuse the `TimeGranularity` vocabulary; extraction parts are a
new `DatePart` enum in `core/enums.py`. Unit/part arguments are validated and lower-cased in the
parser (pure syntax), stored as a string scalar arg of the `ScalarCallKey`, and converted to the
enum at render.

**D2 — `interval` is a parse-time rewrite.** The parser rewrites `x + interval(n,u)`,
`interval(n,u) + x`, `x - interval(n,u)` into `ScalarCall('date_add', …)` (subtraction negates
`n`: a numeric literal folds to its negation, anything else becomes unary minus) before any
other structure can capture `interval`; chains fold left. A second parser pass rejects any
surviving `interval` call with an error pointing to `date_add`. `interval` is in the spec table
(so it is reserved and its arity is checked) but can never reach binding.

**D3 — Typing.** One pure `temporal_type(key, *, column_type)` in `core` returns
`DATE | TIMESTAMP | None` bottom-up (rules: specs "Date operands must be temporal"). ISO
literals get *typed identity* rather than context-dependent typing: while binding a temporal
operand slot — and the value args of `coalesce`/`ifnull`/`nullif`/`greatest`/`least`/`iif`
nested in one — the binder converts an ISO-shaped string literal into a `datetime.date` /
`datetime.datetime` literal (`normalize_scalar` / `Scalar` extended; malformed → `QueryTypeError`).
Everything downstream is then bottom-up and context-free. The checker (`elaborate_env`) raises a
new `QueryTypeError` subclass for non-temporal operands and non-numeric counts; the renderer calls
the same function with `ScopeFrame.column_type` to pick dialect branches; `key_metadata`'s
`measure_key_type` and `dimension_key_metadata` gain a scalar-result-type branch (INT for
`date_part`/`date_diff`, `temporal_type` for the rest), so response types flow for measures and
expression dimensions, local, joined and stage-backed. Alternative rejected: untyped
always-TIMESTAMP (wrong BigQuery/T-SQL SQL, DATE results displayed as timestamps, no fail-closed).

**D4 — Typed dialect hooks, composed base.** New `SqlDialect` hooks, all taking typed operands
only (sql P1): `build_date_part(part: DatePart, expr, operand: DataType)`,
`build_date_diff(unit: TimeGranularity, start, end, operand: DataType)`,
`build_date_add(expr, count: Expression, unit: TimeGranularity, operand: DataType)`,
`build_current_date()`, `build_current_timestamp()`,
`build_temporal_literal(value, dt: DataType)`. Base `build_date_diff` composes from parts and
existing truncation, exactly:
- year: `year(e) - year(s)`; quarter: `4*(year(e)-year(s)) + quarter(e)-quarter(s)`;
  month: `12*(year(e)-year(s)) + month(e)-month(s)`;
- week / week_sunday: `day_gap(trunc_w(s), trunc_w(e)) / 7` with the identically anchored
  `build_date_trunc` (exact division — the gap is a multiple of 7);
- day: `day_gap(date(s), date(e))`; hour/minute/second: epoch-second gap of the unit-truncated
  values divided by 3600/60/1 (exact).
ISO `week` / `iso_year` compose via the week's Thursday (`ts + (4 - isodow) days`) where a
dialect lacks a trustworthy native. A dialect may substitute a native form only where the
emission + execution tests pin identical results (including reversed operands).
Operand-type branches: mixed DATE/TIMESTAMP diff operands are promoted to TIMESTAMP before
dispatch; BigQuery uses `DATE_ADD`/`DATE_DIFF` only for DATE with day-or-coarser units and
`TIMESTAMP_*` otherwise (a DATE is promoted before sub-day units); Postgres/DuckDB cast a
day-or-coarser `date_add` on a DATE back to DATE; T-SQL promotes DATE to `DATETIME2` for
sub-day units; `day_of_week` on T-SQL derives from a fixed Monday epoch, never `DATEFIRST`;
sub-day parts of a DATE return 0 on every dialect.

**D5 — One date-arithmetic primitive per dialect.** `build_date_add` replaces
`build_time_offset_expr` (time_shift join-back, `week_sunday` truncation) and the window
duration path: `duration_interval_exprs` is deleted and the generator folds `build_date_add`
over the parsed duration parts (window units map onto `TimeGranularity`). Alternative
rejected: keep three paths (SQLite overflow/sub-day bugs stay, paths drift).

**D6 — SQLite `slayer_date_add` UDF.** Registered in `SqliteDialect.register_udfs` (DEV-1317
precedent). Parses `YYYY-MM-DD` and `YYYY-MM-DD HH:MM:SS[.ffffff]` (space or `T`), clamps
month-ends, keeps date-only text date-only for day-or-coarser units, returns NULL on NULL /
malformed input (matching SQLite's own date functions, which the other SQLite hooks use).
Alternative rejected: pure-SQL clamp (`CASE` on `strftime('%d')`, operand repeated 4–6×).

**D7 — Count normalisation.** Literal counts are checked in the binder (integral non-bool
number; NULL/string/fractional rejected). A computed count is rendered once as
`CAST(<trunc>(n) AS <integer>)` through the existing `trunc` scalar rendering, in the base
`build_date_add` caller, so every hook receives an integer-valued `Expression`. Counts whose
declared type is provably non-numeric are rejected in the checker.

**D8 — One scalar spec table.** `SCALAR_SPECS` (a mapping of name → frozen `ScalarSpec`
Pydantic model: `min_args`, `max_args`, `sql_passthrough`) in `keys.py`. Derived:
`SCALAR_FUNCTIONS` (all names; the legacy formula parser now recognises calls against it),
`SCALAR_FUNCTION_ARITY`, and `SCALAR_PASSTHROUGH` (moved to `keys.py`: names whose Mode-B call is
literally the same SQL function — OSI's verbatim carry-over). Date functions, `interval`, `like`,
`iif` have `sql_passthrough=False`. An import-time check keeps each derived set a subset of the
table. system.arc42 §3.10 wording becomes "one canonical scalar allowlist, extended never
forked" (approved).

**D9 — Clock cache bypass.** `CLOCK_FUNCTIONS = {current_date, now}`. The engine computes a
`reads_clock` flag from every emitted stage's planned keys (generic key walk) and carries it on
the prepared pipeline result; cache get, put, and refresh re-population all skip when it is set.

## Risks / Trade-offs

- [Golden churn: `time_shift`, `week_sunday` and trailing-window SQL change on several dialects]
  → re-bless byte-equivalence goldens; add all-dialect parity tests (multi-part durations,
  positive/negative frames, quarter/week offsets, chained intervals, half-open comparisons).
- [SQLite UDF: dry-run SQL is not runnable in a bare sqlite3 shell; per-row Python cost in
  time_shift joins] → already true for `ln`/`median`/`percentile`; documented.
- [Dialects we cannot execute locally (Snowflake, BigQuery; MySQL/ClickHouse/SQL Server when no
  server)] → exact per-dialect emission tests for every function × unit; integration execution
  tests where a server runs; natives only where pinned.
- [TEXT-declared date columns on SQLite are now rejected by date functions] → error message
  names the fix (declare DATE/TIMESTAMP), consistent with time dimensions.
- [Malformed stored text on SQLite yields NULL] → documented as SQLite-specific.
- [`current_date()`/`now()` time zone differs per backend (SQLite UTC)] → documented; zones
  are DEV-2000.
