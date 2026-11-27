## 1. Tests (written first; each verified to fail on the current code)

- [x] 1.1 Type and grammar: the `TIME` literal (forms, fraction, invalid text), `current_time` in three spellings, a quoted `current_time` field, planned types, on DuckDB and SQLite; verify by running the new test file and seeing it fail
- [x] 1.2 Casts: text / timestamp / zoned timestamp / time sources, refused sources, `TRY_CAST` of `'bogus'` and `'25:00:00'`, time to text, time to another type, `TIME(p)` and zoned time typing, executed on DuckDB and SQLite; verify the file fails
- [x] 1.3 Operations: dimension, order, every comparison form, conditionals, the eight aggregations, the refused operations, the three parts in every spelling, the refused date functions; and the time-axis refusals (granularity, `date_range`, default time dimension, spine) and no zone conversion; verify the file fails
- [x] 1.4 String-as-time-literal in every slot (`=`, `<`, `IN`, `BETWEEN`, `IS DISTINCT FROM`, `nullif`) and its typed error, executed on DuckDB and SQLite; verify the file fails
- [x] 1.5 Results: plan-typed normalisation of text, duration and native driver values; the typed error for an out-of-day value and for non-time text on SQLite; the codec's JSON and text forms; verify the file fails
- [x] 1.6 Output surfaces: Flight forward and reverse mapping and row conversion; Postgres facade type 1083 in text, binary and as a parameter; facade catalog and information-schema metadata; verify each test fails
- [x] 1.7 Per-dialect emission and the library matrix: all five manifest constructs and the three parts on every Tier-1 dialect, MariaDB as MySQL, Tier-2 refusals for `current_time()` and the parts; verify the dialect tests fail
- [x] 1.8 ClickHouse gate, as unit tests on the server profile: 25.12 (no setting), 25.8 (setting attached, with no correlated subquery in the statement), 25.4 and unknown version (typed refusal, no SQL), setting forbidden by the profile, a query with no time of day unaffected; and `Time64(6)` for computed values; verify they fail
- [x] 1.9 Integration: ClickHouse on the existing 25.4 and 25.8 fixtures (refusal, setting path, a column holding `25:00:00`), MySQL (a column holding `25:00:00`, `TRY_CAST('25:00:00' AS TIME)`); no new container
- [x] 1.10 Ingestion and drift: `TIME`, `TIME(p)`, `TIME WITHOUT TIME ZONE`, ClickHouse `Time` / `Time64(p)` as `Time`, a zoned time type as `Opaque`; a stored `DateTime` field over a `TIME` column reported by validation, untouched by a load, re-typed by a re-ingest with its other attributes kept; verify the file fails
- [x] 1.11 Strict Ossie mode accepts the three constructs; sample profiling shows a `min .. max` range for a `Time` field; verify both fail
- [x] 1.12 Apply the eight approved alignments listed in design.md ("Existing tests aligned with the plan"), and nothing else in existing tests without the user's approval by name; verify the aligned tests fail only for the missing implementation
- [x] 1.13 Time-spine axis: a dataset stored without validation whose `default_time_dimension` is a `Time` field, and one whose is a `String` field, each fail a spine query with `DefaultTimeDimensionTypeError` and no SQL; verify the test fails with the untyped render error

## 2. Type and grammar

- [x] 2.1 Add the time-of-day engine kind for `DataType.TIME`, outside the temporal set, with its own predicate; extend every exhaustive kind table (canonical type, default aggregations, cast names, facade, Flight, Postgres facade) so each fails closed on an unhandled kind; verify 1.1 and `tests/test_enums.py` pass
- [x] 2.2 Parse `TIME '…'` as the temporal-literal kind and bind it with the typed time-literal error; verify the literal tests of 1.1 pass
- [x] 2.3 Add `current_time` to the scalar library and the clock functions, with the bare keyword and the call spelling; mark its queries uncacheable as `now()`; verify the clock tests of 1.1 pass
- [x] 2.4 Type checking: comparisons, conditionals, aggregations, the three parts, the refusals, the string-as-time-literal rule in every slot; verify 1.3 and 1.4 pass
- [x] 2.5 Add `current_time` to the names reserved at the existing Ossie Layer 1 rename gate (dataset 32, query 20, memory 15); verify `tests/test_library_growth.py` passes
- [x] 2.6 Refuse a non-temporal stored `default_time_dimension` used as the time-spine axis with `DefaultTimeDimensionTypeError` before rendering; verify 1.13 passes

## 3. Casts and rendering

- [x] 3.1 Type a cast to `TIME` / `TIME(p)` as a time of day and a zoned time as opaque; refuse date, numeric, boolean and list sources and casts from a time to a non-text type; verify 1.2 passes on DuckDB
- [x] 3.2 Dialect hooks for the literal, the clock, the casts, time to text and the three parts on DuckDB, PostgreSQL, MySQL, SQL Server, Trino, Snowflake and BigQuery, all AST-built inside `dialects/`; `TRY_CAST` range check where the type holds durations; verify the emission tests of 1.7 pass
- [x] 3.3 SQLite: canonical `HH:MM:SS.ffffff` text through registered functions for the literal, the casts, the clock and reads of stored text; verify 1.1–1.4 pass on SQLite
- [x] 3.4 ClickHouse: `Time64(6)` for computed values, the parts built from the value's seconds, and the server-profile time-type capability (version check, setting probe, typed refusal before rendering, `SETTINGS` attachment, profile acquisition triggered by a time-of-day key); verify 1.8 passes
- [x] 3.5 Tier-2: literal and cast keep today's rendering; `current_time()` and the parts refused unless translated; MariaDB as MySQL; verify the matrix tests of 1.7 pass

## 4. Results and outputs

- [x] 4.1 Normalise result cells by the plan's column types after the driver readers: convert to a time of day, raise the typed error naming column and value; verify 1.5 passes
- [x] 4.2 Flight, Postgres facade and facade catalog carry and read back the time type; verify 1.6 passes
- [x] 4.3 Sample profiling of a `Time` field as a range; strict Ossie mode tables (`TIME` cast type, `current_time` no longer unsupported); verify 1.11 passes

## 5. Ingestion and drift

- [x] 5.1 Reflection and the column-type probe type time-of-day columns as `Time` and zoned time types as `Opaque`, on every Tier-1 dialect; verify the ingestion tests of 1.10 pass
- [x] 5.2 Drift validation reports the type conflict; a re-ingest re-types the field keeping its other attributes; a load never writes; verify the drift tests of 1.10 pass

## 6. Compliance, docs and gates

- [x] 6.1 Run the Ossie gate with ClickHouse on 25.12; if any row outside this change's seven shifts, stop and show the user each one before changing anything; verify the gate passes
- [x] 6.2 Regenerate or update `docs/interfaces/ossie-compliance.md` (the seven rows, the ClickHouse version measured) and `docs/interfaces/ossie-layer-3.md`; verify the compliance-page test passes
- [x] 6.3 Docs: `docs/concepts/datasets.md` (type and aggregation tables, ingestion of `TIME`), `references.md`, `time.md`, `expressions.md`, `docs/database-support.md` (matrix row, ClickHouse 25.6 / 25.12), the `Time` sentence in `docs/upgrading/2.0.md`, upgrade notes for the three breaking changes, and the MCP help topics listing functions; verify the docs guard tests pass
- [x] 6.4 Run the tests `la-affected-tests --base main` prints plus the goldens this change reaches; verify all pass
- [x] 6.5 Run `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` against the baseline, `poetry run la-arch-check` and `openspec validate dev-2460-ossie-layer-1-time-of-day-current-time-time --strict`; verify all are clean
