## Purpose

Defines time zones: the zone a dataset's timestamps are stored in, the zone a query buckets, compares and returns them in, and the one conversion between them, so a local-day bucket is right on every Tier-1 dialect while a query without a zone behaves exactly as before.

## ADDED Requirements

### Requirement: A query zone is an optional IANA zone on each stage

A query stage SHALL accept an optional `time_zone`: an IANA zone name (including `UTC`). The final stage's `time_zone` SHALL be the query's zone; a stage's effective zone SHALL be its own `time_zone`, else the query's zone, else none. An unknown zone name SHALL be refused with a typed error naming the value when the query is constructed or the field assigned. Under `conformance="ossie-l3"` a `time_zone` SHALL be refused as not a part of an Ossie query.

Scenario data for this capability (executed on SQLite and DuckDB unless a scenario says otherwise): datasource `shop` with `time_zone: UTC`; table `orders(id, amount, ordered_at, paid_at, local_slot, order_day)`, with `ordered_at` a naive timestamp (`DateTime`) holding UTC wall-clock values, `paid_at` an instant (`DateTimeTz`; DuckDB `TIMESTAMPTZ`, SQLite ISO text with offset `+00:00`) equal to `ordered_at` as an instant, `local_slot` a naive civil timestamp (`DateTime`, `time_zone: civil`) and `order_day` a `Date`. Rows: (1, 10, 2025-01-01 03:30, …, 2025-01-01 09:00, 2025-01-01), (2, 20, 2025-01-01 06:00, …, 2025-01-01 09:00, 2025-01-01), (3, 30, 2025-01-31 23:30, …, 2025-01-31 09:00, 2025-01-31), (4, 40, 2025-03-09 06:30, …, 2025-03-09 09:00, 2025-03-09). In `America/New_York` the rows' `ordered_at` reads 2024-12-31 22:30, 2025-01-01 01:00, 2025-01-31 18:30 and 2025-03-09 01:30; in `Asia/Tokyo` 2025-01-01 12:30, 2025-01-01 15:00, 2025-02-01 08:30 and 2025-03-09 15:30.

#### Scenario: A local-day bucket
- **WHEN** a query with `time_zone: America/New_York` has the time dimension `date_trunc('day', ordered_at)` and the metric `sum(amount)`
- **THEN** the rows are (2024-12-31, 10), (2025-01-01, 20), (2025-01-31, 30), (2025-03-09, 40)

#### Scenario: A stage zone overrides the query zone
- **WHEN** a two-stage query's first stage sets `time_zone: Asia/Tokyo` and buckets `date_trunc('month', ordered_at)` with `sum(amount)`, and the final stage sets `time_zone: America/New_York` and lists that stage's rows
- **THEN** the months are 2025-01 → 30, 2025-02 → 30, 2025-03 → 40, each month start reaching the final stage as a Tokyo wall-clock value converted to New York wall clock

#### Scenario: Unknown zone refused
- **WHEN** a query sets `time_zone: Mars/Olympus`
- **THEN** construction fails with the typed time-zone error naming `Mars/Olympus`

#### Scenario: Ossie conformance refuses a zone
- **WHEN** a query with `time_zone: UTC` runs with `conformance="ossie-l3"`, fresh, as a saved-query refinement, and over a query-backed dataset whose stored stage sets a zone
- **THEN** the fresh query and the refinement are refused as not a part of an Ossie query, and the stored stage's zone is refused the same way

### Requirement: A query without a zone is unchanged

A query whose every stage has no effective zone SHALL emit exactly the SQL and return exactly the values, types and warnings it did before time zones existed, whatever source zones its datasets declare, and including fields that ingestion or load now type `DateTimeTz`.

#### Scenario: Source zones alone change nothing
- **WHEN** queries with no `time_zone` bucket, filter, extract, window, shift and project `ordered_at`, `paid_at` and `local_slot`, with and without the datasource's `time_zone` and the fields' `time_zone`
- **THEN** the emitted SQL and results equal those of the same queries with every `time_zone` removed

#### Scenario: Re-typed fields change nothing without a zone
- **WHEN** queries with no `time_zone` filter, cast, truncate, join on and derive from a BigQuery `TIMESTAMP`, a MySQL `TIMESTAMP` and a ClickHouse `DateTime('UTC')` field, once typed `DateTime` and once `DateTimeTz`
- **THEN** the emitted SQL is byte-identical and the results equal

### Requirement: A DateTime field's source zone

A `DateTime` field's source zone SHALL be its own `time_zone`, else its datasource's `time_zone`, else undeclared. A field's `time_zone` SHALL be an IANA zone name or `civil`; `civil` SHALL mark zone-free civil time, which the datasource's `time_zone` does not apply to. A field `time_zone` SHALL be accepted only on a `DateTime` field whose expression is a plain column, and SHALL be refused with a typed error on a `DateTimeTz`, `Date` or non-temporal field and on a derived field, both when the field is constructed and when the attribute is assigned. A datasource's `time_zone` SHALL be an IANA zone name. A `DateTimeTz` field has no source zone: it is an instant.

#### Scenario: Field zone wins over the datasource zone
- **WHEN** `ordered_at` sets `time_zone: Asia/Tokyo` in datasource `shop` (`time_zone: UTC`) and a query with `time_zone: UTC` lists row 1's `ordered_at`
- **THEN** it is 2024-12-31 18:30

#### Scenario: Zone on a DateTimeTz field refused
- **WHEN** a dataset is saved with `paid_at` setting `time_zone: UTC`, or `time_zone` is assigned on that field object
- **THEN** both fail with the typed error naming `paid_at` and that an instant has no source zone, and the object is unchanged

#### Scenario: Zone on a derived or non-timestamp field refused
- **WHEN** a field `local_paid = date_add(ordered_at, 1, 'hour')` or the `Date` field `order_day` sets `time_zone: UTC`
- **THEN** each fails with the typed error naming the field

#### Scenario: civil skips the datasource zone
- **WHEN** a query with `time_zone: America/New_York` lists `local_slot`
- **THEN** the values are the stored 2025-01-01 09:00, 2025-01-01 09:00, 2025-01-31 09:00 and 2025-03-09 09:00, with no warning

### Requirement: Under a zone every dataset timestamp is read as the zone's wall clock

Under an effective zone Q, every read of a dataset timestamp SHALL yield Q's wall clock as a naive timestamp, converted once at the read: a `DateTimeTz` field from its instant, a `DateTime` field from its source zone; a `civil` field and a `Date` field SHALL pass through unchanged. Every position SHALL see only the read value: time dimensions and `date_trunc` buckets, custom granularities (whose naive origin is read in Q), filters, `date_range`, relative tokens, `whole_periods_only` snapping, `date_part` and its aliases (`hour`, `day_of_week`, …), `date_diff`, `date_add`, trailing windows, window transforms, `time_shift`, `consecutive_periods`, `first` / `last` and their ranking keys, `min` / `max`, join keys, raw rows and the time spine. A `DateTimeTz` field read under Q SHALL be returned as Q wall clock.

#### Scenario: Every position sees the local value
- **WHEN** queries with `time_zone: America/New_York` use, over `ordered_at` and over `paid_at`: a day time dimension; `date_trunc('month', …)` as a dimension; a custom granularity `fiscal_week` (origin `2024-12-30 00:00`, base `week`); `date_range: ["2025-01-01", "2025-01-31"]`; `hour(…)`; `day_of_week(…)`; `date_diff('day', …, '2025-01-01 00:00')`; `date_add(…, 1, 'day')`; a 1-day trailing window; `time_shift` by 1 month; `consecutive_periods`; `first(amount)` ordered by the field; `min` / `max`; a join on the field; raw rows; and the spine over the month time dimension
- **THEN** each result equals the one computed from the New York wall-clock values in the scenario data, executed on SQLite and DuckDB, for a negative zone (`America/New_York`) and a positive one (`Asia/Tokyo`)

#### Scenario: A UTC timestamp near midnight lands in the local day
- **WHEN** a query with `time_zone: America/New_York` filters `ordered_at in '2024-12-31'`
- **THEN** exactly row 1 is kept

#### Scenario: DateTimeTz returned as local wall clock
- **WHEN** a query with `time_zone: Asia/Tokyo` lists row 3's `paid_at`
- **THEN** the value is the naive 2025-02-01 08:30

#### Scenario: Date passes through
- **WHEN** a query with `time_zone: Asia/Tokyo` groups by `order_day`
- **THEN** the days are the stored dates

### Requirement: An undeclared naive read warns

Under an effective zone, a read of a `DateTime` field with no declared source zone SHALL be read as stored and SHALL add one structured `time_zone_undeclared` warning per field, identified by dataset, field and join path, collected across every stage, producer and query-backed dataset of the execution, surfaced both as a Python warning and in the response's warnings, and naming the field and both remedies (a field `time_zone`, including `civil`, or a datasource `time_zone`).

#### Scenario: One warning per field
- **WHEN** datasource `shop` has no `time_zone`, and a query with `time_zone: America/New_York` reads `ordered_at` as a time dimension, a filter, a derived field's input and under an alias, through two stages
- **THEN** the values are the stored ones and the response carries exactly one `time_zone_undeclared` warning, naming `orders.ordered_at`

#### Scenario: Dry run warns too
- **WHEN** the same query runs with `dry_run`
- **THEN** the same single warning is returned

### Requirement: Derived fields and dataset metrics convert their reads

Under an effective zone, a derived field's expression and a dataset metric's or aggregation's expression, including its `FILTER (WHERE …)` condition, SHALL read dataset timestamps as the zone's wall clock, so each means exactly what its expression written inline in the query means. Dialect-specific expression entries SHALL see the same converted values. A bare source column inside an expression that no field maps SHALL be read as stored with the `time_zone_undeclared` warning; a derived timestamp computed from no timestamp read SHALL be civil.

#### Scenario: A derived local date
- **WHEN** a field `order_date = CAST(ordered_at AS DATE)` is grouped by under `time_zone: America/New_York`
- **THEN** the groups equal those of `CAST(ordered_at AS DATE)` written in the query: 2024-12-31, 2025-01-01, 2025-01-31, 2025-03-09

#### Scenario: A dataset metric's filter converts
- **WHEN** a dataset metric `late_orders = count(*) FILTER (WHERE hour(ordered_at) >= 18)` is queried under `time_zone: America/New_York`
- **THEN** it is 2, equal to the same metric written in the query

#### Scenario: An unmapped column warns
- **WHEN** a derived field reads a source column `created_raw` that no field maps, under a query zone
- **THEN** the column is read as stored and the warning names `created_raw`

### Requirement: Predicates defining a dataset see stored values

A field's `filter`, a dataset's `filters` and row-level security predicates SHALL read stored values under every query zone, including through derived fields they reference, so a dataset's rows and access never depend on the query zone.

#### Scenario: A dataset filter is zone-independent
- **WHEN** dataset `orders` has the filter `ordered_at >= '2025-01-01 04:00'` and queries with no zone, `time_zone: America/New_York` and `time_zone: Asia/Tokyo` count its rows
- **THEN** each count is 3

#### Scenario: A field filter through a derived field
- **WHEN** a field `paid_amount` has the filter `day_part >= 6` where `day_part = hour(ordered_at)`, and a query with `time_zone: America/New_York` sums `paid_amount`
- **THEN** the sum is 90, computed from the stored hours 3, 6, 23 and 6

#### Scenario: Row-level security is zone-independent
- **WHEN** a session policy restricts `orders` to `ordered_at = '2025-01-01 03:30:00'` and a query with `time_zone: Asia/Tokyo` counts rows
- **THEN** the count is 1

### Requirement: Unpinned stages and query-backed datasets take the query's zone

A stage without its own `time_zone` SHALL evaluate in the query's zone, and so SHALL the stored stages of every query-backed dataset the query reads, unless they set their own. A query-backed dataset read under a zone SHALL therefore return what its stages return run in that zone, as a table's fields do.

#### Scenario: One zone for the whole query
- **WHEN** a two-stage query buckets `date_trunc('day', ordered_at)` in its first stage and sets `time_zone: America/New_York` only on its final stage
- **THEN** the first stage's days are New York days

#### Scenario: A query-backed dataset follows the reader
- **WHEN** a query-backed dataset `daily` buckets `ordered_at` by day with no zone, and a query with `time_zone: America/New_York` reads it
- **THEN** its days are 2024-12-31, 2025-01-01, 2025-01-31, 2025-03-09

### Requirement: Stage outputs carry their zone

A stage's timestamp output SHALL carry the zone state its timestamp reads determine: undeclared when any read is undeclared, else the stage's zone when any read is converted, else civil. A reader in another zone SHALL convert an output in a zone to its own; an output's repeated local hour SHALL convert by the DST rule. A query-backed dataset's field from a stage that pins a zone SHALL carry that `time_zone`.

#### Scenario: A pinned stage converts into its reader
- **WHEN** a stage with `time_zone: Asia/Tokyo` lists `ordered_at` and a final stage with `time_zone: UTC` reads it
- **THEN** the values are the stored UTC values

#### Scenario: Mixed civil and converted
- **WHEN** a stage under `time_zone: America/New_York` outputs `greatest(ordered_at, local_slot)`
- **THEN** the output carries the New York zone

#### Scenario: A repeated hour through a pinned stage
- **WHEN** instants 2024-11-03 05:30Z and 06:30Z pass through a stage pinned to `America/New_York` (both 01:30 local) and a final stage with `time_zone: UTC` reads them
- **THEN** both read as 2024-11-03 06:30, the later occurrence

### Requirement: Re-bucketing across zones is refused

Bucketing a field that is already bucketed in one zone under a different effective zone SHALL fail with the time-dimension field error, naming the field and both zones, at plan time.

#### Scenario: A UTC month re-bucketed in New York
- **WHEN** a stage pinned to `UTC` buckets `date_trunc('month', ordered_at)` and a final stage with `time_zone: America/New_York` buckets that output by year
- **THEN** planning fails with the time-dimension field error naming the field, `UTC` and `America/New_York`

### Requirement: Under a zone one clock reading is taken in the zone

The engine clock SHALL be read once per execution as an instant; a naive reading (an injected naive clock, `SLAYER_NOW`, the default) SHALL be taken as host-local. Under an effective zone Q, that instant taken in Q SHALL drive relative tokens, the `whole_periods_only` cut-off, the time spine's implied upper bound, and `now()` / `current_date()`, which SHALL render as literals of Q's wall clock and date. A query without a zone SHALL keep its clocks unchanged.

#### Scenario: Relative tokens in the zone
- **WHEN** the clock reads the instant 2025-01-01 03:30Z and a query with `time_zone: America/New_York` filters `ordered_at = 'today'`
- **THEN** it keeps exactly row 1, today being 2024-12-31 in New York

#### Scenario: now() and tokens agree
- **WHEN** the clock reads 2025-01-01 03:30Z and a query with `time_zone: America/New_York` has the dimensions `current_date()` and `now()` and the filter `ordered_at < 'tomorrow'`
- **THEN** `current_date()` is 2024-12-31, `now()` is 2024-12-31 22:30:00, and the filter keeps row 1

#### Scenario: whole_periods_only and the spine in the zone
- **WHEN** the clock reads 2025-03-01 02:00Z and a monthly query with `time_zone: America/New_York`, `whole_periods_only` and the spine runs
- **THEN** the results equal those of the same query without a zone under a clock reading the naive 2025-02-28 21:00 over the New York wall-clock values

### Requirement: DST resolves by the Postgres rule

Converting a naive value from a zone SHALL read a wall-clock time skipped by a forward transition with the offset before the transition, and a wall-clock time repeated by a backward transition as its later occurrence. This SHALL hold on SQLite, DuckDB and Postgres; on BigQuery, Snowflake and SQL Server the engine's own rule applies.

#### Scenario: A skipped time
- **WHEN** a `DateTime` field with `time_zone: America/New_York` holds 2025-03-09 02:30 and a query with `time_zone: UTC` lists it
- **THEN** it is 2025-03-09 07:30

#### Scenario: A repeated time
- **WHEN** it holds 2024-11-03 01:30 under the same query
- **THEN** it is 2024-11-03 06:30

#### Scenario: A 25-hour day
- **WHEN** hourly instants from 2024-11-02 00:00Z to 2024-11-05 00:00Z are bucketed by day under `time_zone: America/New_York`
- **THEN** the 2024-11-03 bucket holds 25 hours of rows

### Requirement: Converted time bounds stay prunable

A filter comparing a converted dataset timestamp, read directly or through a `date_trunc` lowered to bounds, with a resolved time point SHALL also compare the stored column with the bound widened by two days, in the stored column's type; the result SHALL be identical. No widened bound SHALL be added to a frame bound, nor where the dialect cannot render the stored-type literal.

#### Scenario: Widened bounds present
- **WHEN** a query with `time_zone: America/New_York` filters `ordered_at >= '2025-01-01' and ordered_at < '2025-02-01'`
- **THEN** the SQL also compares the stored `ordered_at` with 2024-12-30 00:00 and 2025-02-03 00:00, and the rows are 2 and 3

#### Scenario: Not on frame bounds
- **WHEN** the same query has a trailing window
- **THEN** the window frame carries no widened bound

### Requirement: Zone conversion renders on every Tier-1 dialect

Zone conversion SHALL render on SQLite, DuckDB, Postgres, MySQL, ClickHouse, SQL Server, Trino, Snowflake and BigQuery, for naive and instant fields, with literals, custom-granularity origins, spine bounds and window bounds rendered in the converted value's type (BigQuery `DATETIME`, ClickHouse `DateTime64(6, Q)`). On SQL Server, a zone with no Windows equivalent SHALL fail with a typed error naming it. On MySQL, a zone the server cannot resolve SHALL fail with a typed error naming it and the remedy (loading the time-zone tables) before the statement runs; the check SHALL go through the engine's database client once per zone and client. On SQLite, malformed stored text SHALL read as NULL. A Tier-2 dialect SHALL fail with a typed error only when a conversion is needed.

#### Scenario: Emission per dialect
- **WHEN** a query with `time_zone: America/New_York` buckets, filters and windows `ordered_at` and `paid_at` and is rendered for each Tier-1 dialect
- **THEN** each rendering is valid for that dialect and converts each read once

#### Scenario: Executed on Postgres
- **WHEN** the scenario data runs on Postgres (`timestamp` and `timestamptz`) under a session zone of `Asia/Kolkata`
- **THEN** the local-day and DST scenarios return the same values as on DuckDB

#### Scenario: MySQL without time-zone tables
- **WHEN** a MySQL server without loaded time-zone tables runs a query with `time_zone: America/New_York`
- **THEN** it fails before the statement runs with the typed error naming the zone and the remedy, and a second query with the same zone on the same engine costs no further check

#### Scenario: Unmapped SQL Server zone
- **WHEN** a query with a zone absent from the Windows mapping is rendered for SQL Server
- **THEN** rendering fails with the typed error naming the zone

#### Scenario: Tier-2 fails only when converting
- **WHEN** queries with `time_zone: UTC` run on a Tier-2 dialect, one reading `ordered_at` and one reading only `local_slot` and `order_day`
- **THEN** the first fails with the typed error and the second runs

### Requirement: A zoned cast is converted at the cast

Under an effective zone, a cast to a zoned timestamp SHALL yield the zone's wall clock of the instant it denotes. BigQuery's `TRY_CAST(… AS TIMESTAMPTZ)` SHALL keep its current meaning.

#### Scenario: A zoned cast in the zone
- **WHEN** a query with `time_zone: Asia/Tokyo` has the dimension `CAST('2025-01-01 00:00:00+00:00' AS TIMESTAMPTZ)` on DuckDB
- **THEN** it is the naive 2025-01-01 09:00

### Requirement: The response reports the zone

A response SHALL carry `time_zone`: the query's zone when one is set, else null.

#### Scenario: Zone reported
- **WHEN** a query with `time_zone: America/New_York` runs, and another without one
- **THEN** their responses carry `America/New_York` and null

### Requirement: Zones are settable on every surface

The query zone SHALL be accepted by the REST query endpoint and its run-by-name refinement, the MCP `query` tool and the CLI query JSON. A datasource's zone SHALL be settable by MCP `create_datasource` / `edit_datasource` and the CLI `datasources create --time-zone`, and a field's by the MCP dataset tools; inspect output SHALL show a datasource's and a field's zone. A field's `time_zone` SHALL be stored in the field's SLayer extension and exported with it; a datasource's in its configuration. Each SHALL round-trip unchanged.

#### Scenario: MCP and REST
- **WHEN** MCP `query` and `POST /query` run the local-day scenario with `time_zone: America/New_York`
- **THEN** both return its rows

#### Scenario: Round trip
- **WHEN** a dataset with `ordered_at` (`time_zone: Asia/Tokyo`) and `local_slot` (`time_zone: civil`) is saved, loaded, exported to Ossie and imported, and a datasource with `time_zone: UTC` is saved and loaded
- **THEN** each zone is unchanged, and the exported document validates against the Ossie schema with the zones in the SLAYER extension
