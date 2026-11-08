## MODIFIED Requirements

### Requirement: Malformed date_range shapes are rejected at construction

Constructing a query SHALL fail with a typed error naming the time dimension and the received value when its `date_range` is an empty list, has three or more elements, is `[null, null]`, or has an element that is not a time point by syntax (neither an instant, a period literal nor a relative token; a relative token's unit may be any name, since it may be a granularity of the query's datasource). Checks that depend on the column's type or on the datasource (such as a sub-day bound on a DATE column, or a relative unit the datasource does not define) SHALL fail at planning with the typed time-literal error.

#### Scenario: Empty and over-long ranges

- **WHEN** a query is constructed with `date_range: []` or `date_range: ['2024-01-01', '2024-02-01', '2024-03-01']`
- **THEN** construction fails with the typed error naming the time dimension

#### Scenario: Both bounds missing

- **WHEN** a query is constructed with `date_range: [None, None]`
- **THEN** construction fails with the typed error

#### Scenario: Unparseable bound

- **WHEN** a query is constructed with `date_range: ['2025/01/01', None]`
- **THEN** construction fails with the typed error listing the accepted time-point forms

#### Scenario: Unknown relative unit

- **WHEN** a query against a datasource that defines no granularity `fortnight` has `date_range: ['last fortnight', None]`
- **THEN** planning fails with the typed time-literal error listing the accepted time-point forms

#### Scenario: Sub-day bound on a DATE column

- **WHEN** a query on a DATE time dimension has `date_range: ["2025-01-01 10:00:00", null]`
- **THEN** planning fails with the typed time-literal error
