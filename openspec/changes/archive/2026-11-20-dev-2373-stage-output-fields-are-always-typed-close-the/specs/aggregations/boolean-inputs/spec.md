## MODIFIED Requirements

### Requirement: Boolean aggregation result types and formats
The result of an aggregation over a boolean-valued input SHALL be typed and formatted as:
`sum` → INT with INTEGER format; `avg` → DOUBLE with PERCENT format unless the source column
declares a format, which wins; `min` / `max` → BOOLEAN; every other numeric aggregation → DOUBLE,
as over an INT source; the count family → INT; `first` / `last` → the source type; a custom
aggregation → its declared or inferred type (per `aggregations/formula-templates` › A custom
aggregation's result type). Every surface that reports an aggregate's
type — the query plan, stage schemas consumed by a later stage, and the SQL facade's
metric catalogue — SHALL report the same type for the same column and aggregation.

#### Scenario: Sum is an integer measure
- **WHEN** a query selects `sum(flag)`
- **THEN** the measure's type is INT and its response format INTEGER, and no cast to BOOLEAN
  wraps the aggregate

#### Scenario: Avg displays as a percentage
- **WHEN** a query selects `avg(flag)` and `flag` declares no format
- **THEN** the measure's format is PERCENT

#### Scenario: Explicit column format wins
- **WHEN** `flag` declares a FLOAT format and a query selects `avg(flag)`
- **THEN** the measure's format is that FLOAT format

#### Scenario: Facade agrees with the engine
- **WHEN** the SQL facade lists the metrics of a model with a BOOLEAN, an INT, a DOUBLE, a TEXT
  and a TIMESTAMP column
- **THEN** every metric's data type equals the type the query engine assigns the same column and
  aggregation, including custom aggregations (their declared or inferred type)

#### Scenario: A custom aggregation over a boolean takes its formula's type
- **WHEN** a model defines `trues` as `COUNT(CASE WHEN {value} THEN 1 END)` and a query selects
  `trues(flag)`
- **THEN** the measure's type is INT, not BOOLEAN
