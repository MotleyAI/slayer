# queries/transforms Delta

## ADDED Requirements

### Requirement: Time axis on stage datasets

A downstream stage's own time dimension SHALL be the time axis for every time-ordered transform in that stage (`time_shift`, `change`, `change_pct`, `cumsum`, `lag`, `lead`, `first`, `last`, `consecutive_periods`) and for its windowed aggregates, with values identical to the same shapes evaluated over a model-backed dataset holding the same rows. With two stage time dimensions and no `main_time_dimension`, a time-ordered transform SHALL fail with the existing unambiguous-time-dimension remedy; `main_time_dimension` SHALL select the axis. A stage has no model-level default time dimension, so no default is applied.

#### Scenario: time_shift over a stage time dimension

- **WHEN** an outer stage declares a time dimension on `created_at` at `month` over an inner monthly stage and selects `time_shift(rev:sum, -1, 'month')`
- **THEN** each row carries the previous month's inner sum, NULL for the first month, by executed values on SQLite and DuckDB

#### Scenario: change and cumsum over a stage time dimension

- **WHEN** the same outer stage selects `change(rev:sum)` and `cumsum(rev:sum)`
- **THEN** each row carries the month-over-month delta (NULL first) and the running total, by executed values

#### Scenario: last over a stage time dimension

- **WHEN** the same outer stage selects `last(rev:sum)`
- **THEN** every row carries the value of the latest month, by executed values

#### Scenario: Windowed aggregate over a stage time dimension

- **WHEN** the outer stage selects `rev:sum(window='60d')` over the stage time dimension
- **THEN** each row carries the trailing-window sum keyed on the stage bucket, by executed values

#### Scenario: time_shift over a multi-hop flat time dimension

- **WHEN** an inner stage projects `customers.regions.last_activity_at` at `month` with `*:count` named `n`, and the outer stage declares a time dimension on `customers__regions__last_activity_at` at `month` with `time_shift(n:sum, -1, 'month')`
- **THEN** the shifted relation references the inner stage's flat alias and the query executes with correct values

#### Scenario: Two stage time dimensions need main_time_dimension

- **WHEN** the outer stage declares time dimensions on two distinct temporal columns, `created_at` and `shipped_at`, each at `month`, and selects `change(rev:sum)` without `main_time_dimension`
- **THEN** planning fails with the existing error naming the `main_time_dimension` remedy, and setting `main_time_dimension` to `created_at` makes the query execute with that column's bucket as the axis
