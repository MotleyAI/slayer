## Context

See proposal.md for the motivation. State of the code this design starts from:

- `DataType.TIME` (`Time`) exists and is stored as written, but its engine kind is the opaque one (`slayer/core/enums.py`).
- The grammar refuses `TIME '…'` (`slayer/core/syntax.py`), and bare `current_time` parses as a field reference. Strict mode lists it as unsupported (`slayer/core/ossie_conformance.py`).
- A cast types through `cast_type_of` / `slayer_type_of` and is refused by `cast_type_supported` on SQLite and ClickHouse (`slayer/sql/dialects/`).
- `now()` / `current_date()` render through `_render_clock` (`slayer/sql/render/row_expr.py`): under a zone, a literal of the stage's one clock reading.
- Result cells are read by `dialect.result_readers(description)` (`slayer/database/client.py`), which sees the driver's cursor metadata but no planned type.
- Reflection's `_TYPE_NAME_MAP` maps `TIME` to `DateTime` (`slayer/sql/dialects/reflection.py`); `time without time zone`, `TIMETZ` and `Time64(6)` fall to `String`.
- ClickHouse has a server-profile gate for correlated subqueries only: a minimum version, a default-on version, a setting probe, and a `SETTINGS` attachment triggered by that statement shape (`slayer/sql/dialects/clickhouse.py`, `slayer/engine/query_engine.py`, `slayer/sql/generator.py`).

Principles that bind this change: system P9 (one result codec), P10 (one typing), P12 (one clock reading per execution); core P4 (one key kind per meaning), P5 (typed errors); sql P1 (AST-built), P2 (dialect quirks only in `dialects/`), P9 (fail closed), P14 (emitted types are logical types); semantics Axiom 9 (closure: an operator looks only at its operands' types) and expression layers 2.1 (one meaning on every dialect) and 2.2 (no translation → typed refusal). No arc42 or model edit: no new package and no new import edge.

### Probe facts (2026-11-26)

- **SQLite:** `time('10:30:00')` and `CURRENT_TIME` return text `HH:MM:SS`; `time('bogus')` and `time('25:00:00')` return NULL, but `time('24:00:00')` returns `'24:00:00'`; `time()` drops a fraction and `%f` keeps only milliseconds. Timestamps already go through registered functions producing `YYYY-MM-DD HH:MM:SS.ffffff`.
- **DuckDB:** `TIME` is microsecond; `hour()` / `extract` / `date_part` take a `TIME`; `date_trunc` and `extract(year …)` reject it; `CAST(DATE AS TIME)` errors; `TRY_CAST('25:00:00' AS TIME)` is NULL; `CAST(TIME '10:30:45.5' AS VARCHAR)` is `'10:30:45.5'`; `current_time` is `TIME WITH TIME ZONE`, so the clock must come from `now()`.
- **ClickHouse 25.12**, and 25.8 with `SETTINGS enable_time_time64_type = 1` on the statement, through the repo's `clickhouse+http` driver:
  - `CAST(text AS Time)`, `CAST(timestamp AS Time)`, comparison, `IN`, grouping, `min`, `greatest`, `coalesce`, `if`, `uniqExact`, `groupArray`, `argMin` work.
  - `Time` holds whole seconds: `CAST('10:30:45.123' AS Time)` fails to parse; `Time64(6)` carries the fraction.
  - `toHour` / `EXTRACT` / `dateDiff` / `date_trunc` reject `Time`.
  - `Time` is a signed duration: `'25:00:00'`, `-1` and `3600` cast to it; a column can hold `25:00:00` and `-01:30:00`.
  - `CAST(date AS Time)` is `00:00:00`.
  - `accurateCastOrNull('bogus', 'Time')` is NULL.
  - The driver returns `Time` values as text.
- **ClickHouse 25.8 without the setting:** every `Time` expression fails with "Time type is not allowed". The setting passed as a URL query parameter was ignored by the driver; a `SETTINGS` clause works.
- **Not probed on a live server:** PostgreSQL, MySQL, SQL Server, Trino, Snowflake, BigQuery. Their behaviour here is from documentation; the integration and gate suites are the check.

## Goals / Non-Goals

**Goals:**
- One type and one meaning for a time of day on all nine Tier-1 databases, with the three Ossie constructs and the operations in `specs/queries/time-of-day`.
- A result cell of a time-of-day column is a time of day on every output.

**Non-Goals:**
- A time of day as a time axis (granularity, `date_range`, spine), and `dimension.is_time` defaulting for `Time` (the `format-is-time-*` rows stay with DEV-2464).
- Date functions on a time of day beyond `hour`, `minute`, `second`; arithmetic on it.
- A time type with a time zone: it stays opaque.
- Any change to what is stored for a field.
- An added ClickHouse container in CI.

## Decisions

Tags: `(user)` chosen by the user, `(default)` taken as the only surviving option, `(codex)` a Codex plan-review finding folded in.

1. **SQLite answers with ISO text; ClickHouse answers natively.** Weighed: refuse on both (the issue's Acceptance line); SQLite text with ClickHouse refused; SQLite text with ClickHouse native. Taken: the last. `(user)`
2. **ClickHouse is supported from 25.6, with `enable_time_time64_type = 1` sent on 25.6–25.11.** Weighed: 25.12 only. Follows the correlated-subquery precedent and covers the 25.8 line. `(user)`
3. **No ClickHouse container is added.** The existing 25.4 fixture tests the refusal, 25.8 the setting path, the gate the default path. `(user)`
4. **The Ossie gate's ClickHouse container moves from `24-alpine` to `25.12`.** Weighed: keep 24 and record the rows as a version refusal. `(user)`
5. **A stored value that is not a time of day fails the query** with a typed error naming the column and the value. Weighed: return the database's text unchanged. `(user)`
6. **`Time` fields stop being opaque**, with the listed operations, profiling and typed outputs; `format-time-dimension` and `format-time-aggregate` move from DEV-2464 to this change. Forced by Axiom 9: the literal and the field have one type. `(user, approved extension)`
7. **`hour`, `minute`, `second` take a time of day; every other date function refuses it.** Weighed: refuse all (Ossie's signatures name only timestamps). `(user, approved extension)`
8. **Microsecond precision, with the `TIMESTAMP` literal's time-part grammar.** Follows the existing timestamp rule, SQLite's canonical text included. `(default)`
9. **`current_time()` is the time of day of `now()`**: the same clock reading and zone rule. The only reading consistent with system P12. `(default)`
10. **Cast sources:** text, timestamp and time convert; date, number, boolean and list are refused at plan time. Engines disagree today (ClickHouse gives midnight for a date, DuckDB errors), and 2.1 requires one meaning. `(default)`
11. **A string compared with a time of day is a time literal**, as the time-point rule reads a string against a date. `(default)`
12. **The result-cell error applies on every database**, SQLite included: one rule. `(default)`
13. **Tier-2 keeps what it renders today; MariaDB follows MySQL.** No regression, and 2.2 for the new function. `(default)`
14. **Bare `CURRENT_TIME` is a keyword**, as `current_date` is; Ossie requires the bare form. `(default)`
15. **Requirements live in a new `queries/time-of-day` capability** under the existing cross-cutting `queries` group, with the four contradicted requirements modified. `(default)`
16. **Date functions and arithmetic beyond the three parts are refused, with no follow-up issue**: nothing in Ossie asks for them. `(default)`
17. **Result cells are normalised by the plan's column types**, after the driver readers, in one step that converts to a time of day and raises the typed error. The driver reader cannot do it: it has no planned type, and SQLite reports a time as plain text. The metadata-driven readers stay for raw SQL and driver decoding. `(codex)`
18. **ClickHouse gets its own time-type capability in the server profile**: a version check, a probe of the setting on 25.6–25.11, a typed refusal before rendering, and a `SETTINGS` attachment on any statement that computes or reads a time of day, with profile acquisition triggered by such a key. The correlated-subquery gate does none of this for other shapes. `(codex)`
19. **Computed time-of-day values on ClickHouse are `Time64(6)`**; a reflected `Time` column is read as it is. `Time` cannot hold the fraction the type promises (sql P14). `(codex)`
20. **Reflection types database time-of-day columns as `Time`** on every Tier-1 database; a zoned time type stays opaque. A stored field of another type over such a column is a validation type conflict, untouched by a load and re-typed by a re-ingest. Without it no ingested dataset has a `Time` field. `(codex)` `(user, approved extension)`
21. **`TRY_CAST(… AS TIME)` keeps the existing Postgres exception** (a server older than 16, or of unknown version, gets a plain `CAST`). `(codex)`
22. **`Time` stays out of the temporal set** (`is_temporal`, `TEMPORAL_DATATYPES`) and gets its own predicate, so no time-axis path picks it up; every exhaustive kind table gains the kind and fails closed otherwise. `(codex)`
23. **The string-as-time-literal rule covers every comparison slot**: `IN` members, both `BETWEEN` bounds, null-safe comparison and `nullif`. `(codex)`
24. **Each output surface is tested in every direction it has**: Flight forward and reverse mapping and row conversion; Postgres facade type 1083 in text and binary and as a parameter; facade catalog and information-schema metadata. `(codex)`
25. **All five manifest rows are covered on every Tier-1 dialect**, not only `current_time` and the parts. `(codex)`
26. **The three existing spec statements that say the opposite are modified**, not only added to: `Time` behaving as opaque (ossie-vocabulary), a `TIME` cast typing as UNKNOWN, and SQLite refusing it (casts). `(codex)`
27. **`current_time` gets a rename gate, and the dataset, query and memory versions step.** `models/stored-upgrade` requires a gate for every name a custom granularity may not take, strictly above every earlier gate, and this change reserves `current_time`. The gate is dataset 32, query 20, memory 15, reached by a version step that changes no document. A stored custom granularity named `current_time`, written below the gate, is renamed `current_time_granularity` on upgrade, as every reserved name is. `(default)`
28. **A stored default time dimension of a non-temporal type is refused at query time with the save-time error.** A dataset stored without the save-time check (an older store, a hand-edited file) that names a `Time`, or any other non-temporal, field as its `default_time_dimension` fails a query that uses it as the time-spine axis with `DefaultTimeDimensionTypeError`, naming the dataset, the field and its type, before any SQL is executed. Today it fails with an untyped render error. System P11 already requires the query-time backstop. `(user)`

### Existing tests aligned with the plan (approved by the user, 2026-11-26)

- `tests/test_dev2101_grammar_parse.py`: remove the case asserting `created_at > TIME '10:00'` is rejected.
- `tests/test_dev2280_sqlite_casts.py`: remove the three plain-`TIME` rows from the refused casts; the `TIMESTAMP(3)` and `WITH TIME ZONE` rows stay.
- `tests/dialects/test_clickhouse.py`: `test_clickhouse_refuses_a_time_of_day_cast` keeps only `TIME WITH TIME ZONE`.
- `tests/test_enums.py`: `DataType.TIME` joins the members asserted not opaque.
- `tests/test_ossie_shapes_datatypes.py`: `test_behaviour_follows_the_old_kind` drops the `raw_dt: Time` / `UNKNOWN` pair and its query.
- `tests/_dev2446_required_manifest.py`: the five `TIME_OF_DAY` gap rows become supported rows; the `TIME_OF_DAY` family and `TIME_CASTS` go.
- `tests/_dev2220_compliance_gaps.py`: the `format-time-dimension` and `format-time-aggregate` rows and probes go.
- `tests/integration/_ossie_gate_servers.py`: the ClickHouse image becomes `clickhouse/clickhouse-server:25.12`.

Any other existing test found to contradict the plan needs the user's approval by name before it is changed.

## Risks / Trade-offs

- [Gate rows unrelated to this change shift on ClickHouse 25.12] → Stop and show the user each shifted row before touching it; never re-baseline silently.
- [Six engines are unprobed; a native `TIME` may differ in precision, in `TRY_CAST` of out-of-day text, or in the driver's value form] → The integration suites and the gate execute every construct per engine; a difference is a dialect translation, never a per-engine meaning.
- [A ClickHouse profile that forbids changing settings, on 25.6–25.11] → A typed refusal naming the setting, tested.
- [Existing datasets hold `DateTime` or `String` over a `TIME` column] → Reported by validation, re-typed by re-ingest, documented in the upgrade notes; loads never write.
- [A field named `current_time` stops resolving unquoted] → Documented as a breaking change; the quoted form reads it.
- [MySQL and ClickHouse hold durations in the same type] → `TRY_CAST` checks the range, and a result cell outside the day fails the query.
