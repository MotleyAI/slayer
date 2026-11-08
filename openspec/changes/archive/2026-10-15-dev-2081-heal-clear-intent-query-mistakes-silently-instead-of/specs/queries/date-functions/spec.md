## ADDED Requirements

### Requirement: Date-part aliases mean their date_part
Each of the following spellings SHALL be accepted, case-insensitively, as an exact equivalent of
`date_part` with the listed canonical part — same value, same result key, same SQL — in every
Mode-B position:
- `isodow`, `day_of_week` → `day_of_week`;
- `doy`, `dayofyear`, `day_of_year` → `day_of_year`;
- `isoyear`, `iso_year` → `iso_year`;
- `isoweek`, `iso_week`, `weekofyear` → `week`;
- `dayofmonth`, `day_of_month` → `day`.

Each alias SHALL be accepted as a one-argument function `<alias>(x)` and as the part argument
`date_part('<alias>', x)`. An alias function call with any other argument count, or with keyword
arguments, SHALL be rejected naming the function. The granularity names (`second`, `minute`, `hour`,
`day`, `week`, `week_sunday`, `month`, `quarter`, `year`) SHALL keep their time-bucket meaning as
functions; they are not aliases.

#### Scenario: Alias function as a dimension
- **WHEN** a query on `orders` has the expression dimension `isodow(created_at)` named `dow` and measures `count(*)`
- **THEN** its columns, rows and SQL equal those of the same query with `date_part('day_of_week', created_at)`

#### Scenario: Every alias in every position
- **WHEN** each alias is used as a function and as a `date_part` part, in a dimension, a filter, an aggregation input and an order key
- **THEN** each query returns the same columns, rows and SQL as its canonical `date_part` twin

#### Scenario: Alias part inside date_part
- **WHEN** a filter uses `date_part('ISOWEEK', created_at) = 1`
- **THEN** it binds to the same value as `date_part('week', created_at) = 1`

#### Scenario: Wrong alias arity
- **WHEN** a filter uses `isodow(created_at, 1) = 1`
- **THEN** the query is rejected with an error naming `isodow` and its argument count

#### Scenario: Granularity call keeps its bucket meaning
- **WHEN** a query lists `"week(created_at)"` in `dimensions`
- **THEN** it is the week time bucket, not a week number

### Requirement: extract is the SQL spelling of date_part
`extract(<part> from x)` SHALL mean exactly `date_part('<part>', x)` — same value, same result key,
same SQL — for every canonical part and every date-part alias, the part written bare or as a string
literal, `extract` and `from` matched case-insensitively, `x` any temporal operand (including nested
calls and expressions). A part outside the canonical parts and aliases SHALL be rejected with the
error listing the accepted parts. Text inside string literals SHALL never be read as `extract` or
`from`.

#### Scenario: Extract with an alias part
- **WHEN** a query on `orders` has the expression dimension `extract(isodow from created_at)`
- **THEN** its columns, rows and SQL equal those of `date_part('day_of_week', created_at)`

#### Scenario: Extract over a nested operand
- **WHEN** a measure uses `max(EXTRACT(day FROM coalesce(shipped_at, created_at)))`
- **THEN** it equals `max(date_part('day', coalesce(shipped_at, created_at)))`

#### Scenario: Unknown extract part
- **WHEN** a filter uses `extract(epoch from created_at) > 0`
- **THEN** the query is rejected with an error listing the accepted parts

#### Scenario: Keyword text in a string literal
- **WHEN** a filter uses `status == 'extract(day from x)'`
- **THEN** the literal is compared unchanged

### Requirement: Ambiguous weekday names name both conventions
`dow`, `dayofweek` and `weekday` SHALL NOT heal: as a `date_part` or `extract` part they SHALL be
rejected, and as a function call that resolves to no aggregation (including a model's custom
aggregation of that name) they SHALL be rejected; in both cases the error SHALL state that the name
has conflicting weekday numberings across databases and give `date_part('day_of_week', x)` (ISO,
Monday = 1 … Sunday = 7) and `date_part('day_of_week', x) % 7` (Sunday = 0 … Saturday = 6).

#### Scenario: dow as a function
- **WHEN** a query on `orders` has the expression dimension `dow(created_at)`
- **THEN** the query is rejected with an error giving both `date_part('day_of_week', x)` and `date_part('day_of_week', x) % 7`

#### Scenario: dayofweek as an extract part
- **WHEN** a filter uses `extract(dayofweek from created_at) = 1`
- **THEN** the query is rejected with the same explanatory error

#### Scenario: Custom aggregation named weekday still works
- **WHEN** a model declares a custom aggregation `weekday` and a measure uses `weekday(amount)`
- **THEN** the measure renders the model's `weekday` aggregation

## MODIFIED Requirements

### Requirement: Date function names are reserved
A model custom aggregation or custom granularity named `date_part`, `date_diff`, `date_add`,
`interval`, `current_date`, `now`, `extract`, or any date-part alias (`isodow`, `day_of_week`, `doy`,
`dayofyear`, `day_of_year`, `isoyear`, `iso_year`, `isoweek`, `iso_week`, `weekofyear`, `dayofmonth`,
`day_of_month`) (in any case) SHALL be rejected at model validation.

#### Scenario: Shadowing aggregation rejected
- **WHEN** a model declares a custom aggregation named `date_diff`
- **THEN** model validation fails naming the reserved scalar function

#### Scenario: Alias-named aggregation rejected
- **WHEN** a model declares a custom aggregation named `isodow`
- **THEN** model validation fails naming the reserved date-part alias
