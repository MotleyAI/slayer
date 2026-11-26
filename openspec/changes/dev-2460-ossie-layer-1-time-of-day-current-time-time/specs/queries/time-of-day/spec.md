## Purpose

The time-of-day type: the `TIME` literal, `current_time()` and casts to `TIME`, what a time of day can do in a query, how each database holds it, and how it is returned and ingested.

## ADDED Requirements

### Requirement: Time of day is an ordered type without a date or a zone

A time of day SHALL be a value from `00:00:00` to `23:59:59.999999` at microsecond precision, with no date and no time zone, ordered by clock time. A field declared `Time`, a `TIME` literal, `current_time()` and a cast to `TIME` SHALL have this one type and SHALL behave alike in every position. A time of day SHALL NOT be a time axis: it SHALL be refused with a typed error as a time dimension with a granularity, as a `date_range` target, and as a dataset's default time dimension or time-spine axis. A query `time_zone` SHALL NOT convert a `Time` field.

#### Scenario: A field and a literal behave alike
- **WHEN** a query on DuckDB and on SQLite has the dimension `shifts.starts_at` over a `Time` field and the filter `shifts.starts_at >= TIME '09:00:00'`
- **THEN** it returns one row per distinct start time at or after 09:00, each a time of day

#### Scenario: Not a time axis
- **WHEN** a query names a `Time` field as a time dimension with granularity `hour`, or gives it a `date_range`
- **THEN** each fails with a typed error naming the field and its type

#### Scenario: No zone conversion
- **WHEN** a query with `time_zone: America/New_York` selects a `Time` field holding `09:30:00`
- **THEN** the value is `09:30:00`

### Requirement: The TIME literal

`TIME '<text>'` SHALL be accepted in every Mode-B position and SHALL yield the time of day `<text>` names, where `<text>` is `HH:MM`, `HH:MM:SS` or `HH:MM:SS.f` with one to six fractional digits. Any other text — a date, a zone or offset, an hour above 23, a minute or second above 59 — SHALL be rejected at bind time with the typed time-literal error naming the literal.

#### Scenario: Literal as a dimension
- **WHEN** a dimension is `TIME '10:30:00'` on each Tier-1 database
- **THEN** its planned type is `Time` and its value is the time of day 10:30:00

#### Scenario: Fraction kept
- **WHEN** dimensions are `TIME '10:30:00.123456'` and `TIME '10:30'`
- **THEN** their values are 10:30:00.123456 and 10:30:00

#### Scenario: Invalid literal
- **WHEN** a dimension is `TIME '24:00:00'`, `TIME '10:30:00+02:00'` or `TIME '2024-01-15'`
- **THEN** each is rejected at bind time naming the literal

### Requirement: current_time reads the query's clock

`current_time()`, bare `CURRENT_TIME` and `CURRENT_TIME()` SHALL be one portable library function taking no arguments and returning a time of day: the time of day of `now()`. Without a time zone it SHALL be evaluated by the database from the clock `now()` reads; under an effective time zone it SHALL be the time of day of the engine's one clock reading in that zone's wall clock, rendered as a literal. A query computing it SHALL bypass the result cache exactly as one computing `now()` does. A call with an argument SHALL be rejected. A field named `current_time` SHALL be read only when written quoted.

#### Scenario: Three spellings
- **WHEN** dimensions are `current_time()`, `CURRENT_TIME` and `CURRENT_TIME()` on DuckDB and on SQLite
- **THEN** each has planned type `Time` and a value between the times of day read just before and just after the query

#### Scenario: Under a zone
- **WHEN** the clock reads the instant `2025-01-01 03:30:00Z` and a query with `time_zone: America/New_York` has the dimensions `current_time()` and `now()`
- **THEN** `current_time()` is 22:30:00 and equals the time-of-day part of `now()`

#### Scenario: Never cached
- **WHEN** a query with the dimension `current_time()` is executed twice with caching enabled
- **THEN** both executions reach the database

#### Scenario: A field named current_time
- **WHEN** a dataset has a field named `current_time` and a query selects `"current_time"`
- **THEN** it reads the field

### Requirement: Casts to and from a time of day

`CAST(x AS TIME)`, `x::TIME` and `TRY_CAST(x AS TIME)` SHALL convert, with one meaning on every database that answers: text naming a time of day (the literal's forms) to that time; a timestamp to its time-of-day part, read after any zone conversion of the timestamp; a time of day to itself. A `Date`, numeric, boolean or list source SHALL be rejected at plan time with the typed cast error naming the source type. Text that names no time of day SHALL follow the database's own `CAST`; `TRY_CAST` SHALL yield NULL for it, and for text naming a clock value outside the day (`'25:00:00'`), on every Tier-1 database, save the Postgres servers `queries/casts` excepts. A time of day cast to a character type SHALL yield `HH:MM:SS`, followed by its fraction only when the fraction is not zero; a time of day cast to any other type SHALL be rejected at plan time with the typed cast error. `TIME(p)` SHALL keep its precision where the database can express it and SHALL otherwise be rejected as an inexpressible type.

#### Scenario: Text to time
- **WHEN** dimensions are `CAST('10:30:00' AS TIME)`, `'10:30:00'::TIME` and `TRY_CAST('10:30:00' AS TIME)` on each Tier-1 database
- **THEN** each has planned type `Time` and value 10:30:00

#### Scenario: Timestamp to time
- **WHEN** a dimension is `CAST(created_at AS TIME)` over a row whose `created_at` is `2024-01-15 10:30:45`
- **THEN** its value is 10:30:45 on DuckDB and on SQLite

#### Scenario: Zoned timestamp to time
- **WHEN** a query with `time_zone: America/New_York` has the dimension `CAST(seen_at AS TIME)` over a `DateTimeTz` field holding the instant `2025-01-01 03:30:00Z`
- **THEN** its value is 22:30:00

#### Scenario: Refused sources
- **WHEN** a dimension is `CAST(order_date AS TIME)` over a `Date` field, `CAST(amount AS TIME)` over a numeric field or `CAST(is_paid AS TIME)` over a boolean field
- **THEN** each is rejected at plan time naming the source type, on every database

#### Scenario: TRY_CAST of invalid text
- **WHEN** dimensions are `TRY_CAST('bogus' AS TIME)` and `TRY_CAST('25:00:00' AS TIME)` on DuckDB, SQLite, MySQL and ClickHouse
- **THEN** each yields NULL

#### Scenario: Time to text
- **WHEN** dimensions are `CAST(TIME '10:30:00' AS VARCHAR)` and `CAST(TIME '10:30:00.5' AS VARCHAR)` on DuckDB and on SQLite
- **THEN** they yield `'10:30:00'` and `'10:30:00.5'`

#### Scenario: Time to another type
- **WHEN** a dimension is `CAST(TIME '10:30:00' AS TIMESTAMP)` or `CAST(TIME '10:30:00' AS INTEGER)`
- **THEN** each is rejected at plan time with the typed cast error

### Requirement: A string compared with a time of day is a time literal

A string literal in a comparison slot facing a time of day — either side of `=`, `<>`, `<`, `<=`, `>`, `>=` and `IS [NOT] DISTINCT FROM`, every member of `IN`, both bounds of `BETWEEN`, and the second argument of `nullif` — SHALL be read as the `TIME` literal of its text, and a string that names no time of day SHALL be rejected at bind time with the typed time-literal error naming it.

#### Scenario: Every slot
- **WHEN** filters on DuckDB and on SQLite are `shifts.starts_at > '09:00'`, `shifts.starts_at IN ('09:30:00', '17:00')`, `shifts.starts_at BETWEEN '09:00' AND '12:00:00'` and `shifts.starts_at IS DISTINCT FROM '09:30'`
- **THEN** each selects the rows the same filter written with `TIME` literals selects

#### Scenario: Not a time
- **WHEN** a filter is `shifts.starts_at > 'noon'` or `shifts.starts_at IN ('09:00', '2024-01-15')`
- **THEN** each is rejected at bind time naming the string

### Requirement: What a time of day can do

A time of day SHALL be usable as a dimension, as an order key, and in `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN`, `IS [NOT] NULL` and `IS [NOT] DISTINCT FROM` against another time of day. `coalesce`, `ifnull`, `nullif`, `greatest`, `least`, `iif` and `CASE` whose value arguments are all times of day or NULL SHALL yield a time of day. Its default aggregations SHALL be `count`, `count_distinct`, `count_distinct_approx`, `first`, `last`, `min`, `max` and `collect`, with `min`, `max`, `first` and `last` yielding a time of day and `collect` a list of them where the database has lists. Arithmetic on a time of day, a comparison of a time of day with a `Date` or `DateTime`, a numeric aggregation of one, and a conditional mixing a time of day with another type SHALL each be rejected with a typed error naming the operand and its type.

#### Scenario: Group, order and aggregate
- **WHEN** a query on DuckDB and on SQLite over shifts starting 09:30, 09:30 and 17:00 has the dimension `shifts.starts_at`, the metrics `count(shifts.*)`, `min(shifts.starts_at)` and `max(shifts.starts_at)`, ordered by the dimension
- **THEN** it returns 09:30 with count 2 then 17:00 with count 1, and the same query without the dimension returns 09:30 and 17:00

#### Scenario: Conditionals keep the type
- **WHEN** dimensions are `coalesce(shifts.ends_at, TIME '23:59:59')` and `greatest(shifts.starts_at, TIME '10:00')`
- **THEN** each has planned type `Time`

#### Scenario: Refused operations
- **WHEN** a query uses `shifts.starts_at + 1`, `sum(shifts.starts_at)`, `shifts.starts_at > orders.created_at` over a `DateTime` field, or `coalesce(shifts.starts_at, 0)`
- **THEN** each is rejected with a typed error naming the operand and its type

### Requirement: Date functions other than the three parts refuse a time of day

`date_trunc`, `date_add`, `date_diff`, the `interval` spellings and `date_part` with a part other than `hour`, `minute` and `second` SHALL reject a time-of-day operand with the typed date-operand error, which SHALL say that a time of day is accepted only by `hour`, `minute` and `second`.

#### Scenario: Refused date functions
- **WHEN** a dimension is `date_trunc('hour', shifts.starts_at)`, `date_add(shifts.starts_at, 1, 'hour')`, `date_diff('minute', shifts.starts_at, shifts.ends_at)` or `date_part('year', shifts.starts_at)`
- **THEN** each is rejected with the typed error naming the operand

### Requirement: Every Tier-1 database holds a time of day

A time of day SHALL be held as the database's own time-of-day type at microsecond precision on DuckDB, PostgreSQL, MySQL, SQL Server, Trino, Snowflake and BigQuery; as ISO text `HH:MM:SS.ffffff` on SQLite, where a stored value is read by the rule SQLite's date and timestamp text is read by; and as the database's own time-of-day type with six fractional digits on ClickHouse. Each construct of this capability SHALL yield the same value on every one of them. MariaDB SHALL behave as MySQL. Any other Tier-2 database SHALL keep the cast it renders before this change for the literal and the cast, and SHALL refuse `current_time()` and the time parts of a time of day at plan time unless it translates them.

#### Scenario: One value everywhere
- **WHEN** the dimensions `TIME '10:30:00.123456'`, `CAST('10:30:00' AS TIME)` and `date_part('hour', TIME '10:30:00')` run on each Tier-1 database
- **THEN** they yield 10:30:00.123456, 10:30:00 and 10 on every one

#### Scenario: SQLite text
- **WHEN** a `Time` field on SQLite reads a text column holding `09:30:00` and `17:00:00.250`
- **THEN** the dimension returns the times of day 09:30:00 and 17:00:00.25, and `min` of the field is 09:30:00

### Requirement: ClickHouse answers from server 25.6

On ClickHouse a query that computes or reads a time of day SHALL run on a server of version 25.12 or later as written; on a server from 25.6 up to 25.12 it SHALL run with the server's time-type setting enabled for that statement, sent by SLayer whatever else the statement contains; and on an older server, or one whose version cannot be read, it SHALL be refused before any SQL is executed with a typed error naming the construct and the version it needs. Where the server or the user's profile does not let the setting be changed, the query SHALL be refused with a typed error naming the setting. A query that touches no time of day SHALL be unaffected.

#### Scenario: Default-on server
- **WHEN** the dimension `CAST('10:30:00' AS TIME)` runs on ClickHouse 25.12
- **THEN** it yields 10:30:00 and the statement carries no time-type setting

#### Scenario: Setting sent
- **WHEN** the same dimension runs on ClickHouse 25.8, in a query with no join
- **THEN** it yields 10:30:00 and the statement enables the time-type setting

#### Scenario: Old server
- **WHEN** the same dimension runs on ClickHouse 25.4
- **THEN** the query fails with a typed error naming the cast and version 25.6, and no SQL is executed

#### Scenario: Unknown version
- **WHEN** the same dimension is planned as a dry run without a connection
- **THEN** it fails with the same typed error

#### Scenario: Setting forbidden
- **WHEN** the same dimension runs on ClickHouse 25.8 as a user whose profile makes the time-type setting read-only and off
- **THEN** the query fails with a typed error naming the setting

### Requirement: A time-of-day result is a time of day

Every cell of a result column whose planned type is `Time` SHALL be returned as a time of day or NULL, whatever form the database driver returns it in. A cell that is not a time of day — a value below `00:00:00` or at or above `24:00:00` on a database whose type also holds durations, or text that names no time of day — SHALL fail the query with a typed error naming the column and the value. The JSON and text forms SHALL be `HH:MM:SS`, followed by the fraction only when it is not zero, written by the one result codec. Arrow and Flight SHALL carry the column as a microsecond time type, the Postgres facade as its `time` type in text and binary form, and the facade catalog SHALL report it as `TIME`; each SHALL read the same type back where it reads types.

#### Scenario: Driver forms agree
- **WHEN** the dimension `TIME '10:30:00'` runs on SQLite, MySQL and ClickHouse, whose drivers return text, a duration and text
- **THEN** each result cell is the time of day 10:30:00 and its JSON form is `"10:30:00"`

#### Scenario: A stored duration fails the query
- **WHEN** a `Time` field on MySQL or ClickHouse reads a column holding `25:00:00`, and a query selects the field
- **THEN** the query fails with a typed error naming the column and `25:00:00`

#### Scenario: Non-time text on SQLite
- **WHEN** a `Time` field on SQLite reads a text column holding `noon`, and a query selects the field
- **THEN** the query fails with a typed error naming the column and `noon`

#### Scenario: Typed outputs
- **WHEN** a query with a `Time` dimension is served over Flight and over the Postgres facade
- **THEN** the Flight schema types the column as a microsecond time, and the facade describes it with the `time` type and sends 10:30:00 in text and in binary form

### Requirement: Time of day in strict Ossie mode

Under the strict Ossie binding, the `TIME` literal, `CURRENT_TIME`, `CURRENT_TIME()` and a cast to `TIME` SHALL be accepted with the meaning this capability gives them.

#### Scenario: Strict accepts the three constructs
- **WHEN** a strict query on DuckDB has the dimensions `CURRENT_TIME`, `TIME '10:30:00'` and `CAST('10:30:00' AS TIME)`
- **THEN** it runs, and the last two yield 10:30:00

### Requirement: Time-of-day fields are profiled as a range

Sample profiling SHALL profile a `Time` field as the other ordered non-text types are: a `min .. max` range of its values.

#### Scenario: Range
- **WHEN** a `Time` field over values 09:30:00 and 17:00:00 is inspected with samples
- **THEN** its sample text is `09:30:00 .. 17:00:00`

### Requirement: Database time-of-day columns ingest as Time

Ingestion, drift validation and a statement's column-type probe SHALL type a database column of a time-of-day type without a time zone as `Time` on every Tier-1 database: `TIME`, `TIME(p)` and `TIME WITHOUT TIME ZONE`, and ClickHouse `Time` and `Time64(p)`. A time type with a time zone SHALL stay `Opaque`. A stored field of another type over such a column SHALL be reported by validation as a type conflict naming both types, SHALL be left as stored by a load, and SHALL be re-typed to `Time` by a re-ingest with every other attribute unchanged.

#### Scenario: Ingested as Time
- **WHEN** a DuckDB table with a `TIME` column and a `TIME WITH TIME ZONE` column is ingested
- **THEN** the first field is `Time` and the second `Opaque`

#### Scenario: A stored DateTime field over a TIME column
- **WHEN** a stored dataset types a field `DateTime` over a database `TIME` column, the datasets are validated, and the datasource is then re-ingested
- **THEN** validation reports a type conflict naming `DateTime` and `Time`, the stored dataset is unchanged until the re-ingest, and the re-ingest makes the field `Time` keeping its description and label
