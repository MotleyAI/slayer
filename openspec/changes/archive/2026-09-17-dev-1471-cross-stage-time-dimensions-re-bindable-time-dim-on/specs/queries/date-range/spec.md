# queries/date-range Delta

## ADDED Requirements

### Requirement: A date_range on a stage time dimension filters the stage's rows

A two-bound `date_range` on a downstream stage's time dimension SHALL filter that stage's input rows to the inclusive range `[start, end]` on the stage column, before the stage's own aggregation, exactly as a model-scope `date_range` filters a model's rows. The range MUST NOT be silently dropped, and it MUST NOT alter the upstream stage's own computation.

#### Scenario: Stage date_range restricts the outer stage

- **WHEN** an inner stage buckets `created_at` at `month` with a revenue sum over four months, and the outer stage declares a time dimension on `created_at` at `month` with `date_range` covering the middle two months
- **THEN** the outer stage returns only those two months with the inner sums, by executed values on SQLite and DuckDB, and the generated SQL applies the range in the outer stage's WHERE on the stage column

#### Scenario: Stage date_range on a coarser re-bucketing

- **WHEN** the outer stage re-buckets a monthly column at `year` with a `date_range` inside one year
- **THEN** the result is that year's total over the months inside the range only
