## MODIFIED Requirements

### Requirement: Query feature counters
For each query about to execute, a usage report SHALL count structural flags — multistage, inline source model or model extension, time dimensions, filters, computed dimensions, saved-query reference, non-default `to_many_handling`, a cast (`cast`), a `TRY_CAST` (`try_cast`), an aggregation `FILTER (WHERE …)` (`aggregate_filter`) and a list literal (`list_literal`) — and the occurrences of each built-in transform, aggregation and library function name. Names SHALL be read from the parsed expression, so each spelling the grammar folds into one construct counts as that construct (`count(DISTINCT x)` as `count_distinct`, `strpos` as `instr`, `CASE` as `iif`, `EXTRACT` as `date_part`). An aggregation or transform name that is not built in SHALL be counted as `custom`; a function name outside the fixed library vocabulary SHALL NOT be counted. Every expression position of every stage SHALL be read — measures, dimensions, time dimensions, filters and order — as well as the measure formulas of an inline source model or model extension. An expression that does not parse SHALL contribute no names. Features SHALL be counted per attempted query, independently of its success.

#### Scenario: Built-in and custom names
- **WHEN** a query uses `time_shift`, `sum` and a user-defined aggregation
- **THEN** the report counts `time_shift` and `sum` by name and the user-defined one as `custom`

#### Scenario: Multistage flag
- **WHEN** a query list of three stages executes
- **THEN** the `multistage` flag count increments once

#### Scenario: Grammar forms
- **WHEN** a query's measures are `sum(x)::int`, `CAST(avg(y) AS DOUBLE)`, `count(*) FILTER (WHERE z > 0)` and `count(DISTINCT x)`
- **THEN** the report counts aggregations `sum`, `avg`, `count` and `count_distinct`, the flags `cast` (twice) and `aggregate_filter`, and no `custom`

#### Scenario: Library functions by name
- **WHEN** a query uses `lower(name)`, `regexp_like(name, 'a')`, `payload ->> 'k'` and `contains_any(skills, ['go'])`
- **THEN** the report counts the functions `lower`, `regexp_like`, `json_value` and `contains_any`, and the flag `list_literal`

#### Scenario: Every position is read
- **WHEN** a query has `upper(city)` as a computed dimension, `coalesce(shipped_at, ordered_at)` as a time dimension, `length(name) > 3` as a filter and `abs(rate)` as an order key
- **THEN** the report counts `upper`, `coalesce`, `length` and `abs`

#### Scenario: Unknown name
- **WHEN** a query's measure is `array_length(tags)`
- **THEN** the report counts one `custom` aggregation and no function
