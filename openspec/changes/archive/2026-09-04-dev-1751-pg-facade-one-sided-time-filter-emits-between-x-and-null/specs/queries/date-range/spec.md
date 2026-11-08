# queries/date-range

## Purpose

Defines the `TimeDimension.date_range` contract at planning time: what a well-formed
range renders as, and how malformed ranges fail — loudly for inexpressible bounds,
with a warning for wrong-length ranges.

## ADDED Requirements

### Requirement: A two-bound date_range renders an inclusive range filter

A `date_range` of two non-null string bounds SHALL filter rows to the inclusive range
`[start, end]` on the time dimension's underlying raw column.

#### Scenario: Both bounds present

- **WHEN** a query's time dimension has `date_range: ['2024-01-01', '2024-12-31']`
- **THEN** the executed query returns only rows whose raw column value lies between
  the two bounds inclusive

### Requirement: A null date_range bound is a hard error

Planning a query whose time dimension has a two-element `date_range` containing a
null bound SHALL raise a `ValueError` naming the time dimension and the received
range, and suggesting a one-sided filter as the supported spelling. The planner MUST
NOT emit a comparison against NULL. The check MUST apply to every query shape,
including models whose planning does not use a plain single-model scope.

#### Scenario: Missing upper bound

- **WHEN** a query is planned with `date_range: ['2024-01-01', None]`
- **THEN** planning raises a `ValueError` whose message names the time dimension

#### Scenario: Missing lower bound

- **WHEN** a query is planned with `date_range: [None, '2024-12-31']`
- **THEN** planning raises a `ValueError`

#### Scenario: Both bounds missing

- **WHEN** a query is planned with `date_range: [None, None]`
- **THEN** planning raises a `ValueError`

#### Scenario: Multi-stage models fail loudly too

- **WHEN** a query against a multi-stage (`source_queries`) model is planned with a
  time dimension whose `date_range` contains a null bound
- **THEN** planning raises a `ValueError` rather than silently ignoring the range

### Requirement: A wrong-length date_range warns and emits no filter

A `date_range` that is present but not two elements (`[]`, one element, three or more)
SHALL be ignored — no date filter is emitted — and SHALL surface a
`MALFORMED_DATE_RANGE` normalization warning. This preserves the ratified DEV-1745
silent-no-op behavior while keeping the drop visible.

#### Scenario: Single-element range is ignored with a warning

- **WHEN** a query is planned with `date_range: ['2024-01-01']`
- **THEN** no date filter is applied and a `MALFORMED_DATE_RANGE` warning is emitted
