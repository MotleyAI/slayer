## MODIFIED Requirements

### Requirement: A refined run equals the hand-written merged final stage
Run-by-name SHALL accept a refinement carrying any of `dimensions`, `time_dimensions`, `measures`,
`filters`, `order`, `limit`, `offset`, `main_time_dimension`, `whole_periods_only`,
`distinct_dimension_values` and `to_many_handling`, with the same input forms and validation as the
same fields of a query. The refinement SHALL merge into the saved query's final stage only — earlier
stages are never changed — and the result SHALL be exactly the values, result keys, warnings and
errors of that merged stage written by hand in place of the saved final stage. A refinement carrying
`source_model`, `name`, `version`, `variables` or any unknown field SHALL be rejected. An empty
refinement SHALL behave exactly like no refinement.

Scenario data for this capability (executed on SQLite and DuckDB): table `orders(id, customer_id, region,
status, amount, ordered_at)` with rows (1, c1, US, paid, 100, 2025-01-05), (2, c1, US, refunded, 40,
2025-01-20), (3, c2, EU, paid, 70, 2025-02-03), (4, c3, EU, paid, 35, 2025-02-17), (5, c2, US, paid,
50, 2025-03-09); model `orders` over it with default time dimension `ordered_at`. Saved query
`monthly_revenue` = `{"source_model": "orders", "time_dimensions": [{"dimension": "ordered_at",
"granularity": "month"}], "measures": [{"formula": "sum(amount)", "name": "revenue"}], "filters":
["status = 'paid'"]}`, which runs by name to 2025-01 → 100, 2025-02 → 105, 2025-03 → 50 under keys
`orders.ordered_at`, `orders.revenue`.

#### Scenario: Extra dimension
- **WHEN** `monthly_revenue` runs with refinement `{"dimensions": ["region"]}`
- **THEN** the rows are (US, 2025-01, 100), (EU, 2025-02, 105), (US, 2025-03, 50)

#### Scenario: Extra measure
- **WHEN** `monthly_revenue` runs with refinement `{"measures": ["count(*)"]}`
- **THEN** the result adds `orders._count` = 1, 2, 1 for 2025-01, 2025-02, 2025-03

#### Scenario: Extra filter ANDs with the saved filters
- **WHEN** `monthly_revenue` runs with refinement `{"filters": ["amount >= 50"]}`
- **THEN** the rows are 2025-01 → 100, 2025-02 → 70, 2025-03 → 50

#### Scenario: Window added to the saved time dimension
- **WHEN** `monthly_revenue` runs with refinement `{"time_dimensions": [{"dimension": "ordered_at", "granularity": "month", "date_range": ["2025-02-01", "2025-03-31"]}]}`
- **THEN** the rows are 2025-02 → 105, 2025-03 → 50

#### Scenario: Order and limit
- **WHEN** `monthly_revenue` runs with refinement `{"order": [{"column": "revenue", "direction": "desc"}], "limit": 1}`
- **THEN** the single row is 2025-02 → 105

#### Scenario: Second granularity renames the saved key
- **WHEN** `monthly_revenue` runs with refinement `{"time_dimensions": ["date_trunc('year', ordered_at)"]}`
- **THEN** the result keys are `orders.ordered_at.month`, `orders.ordered_at.year` and `orders.revenue`, as in the hand-written query with both granularities

#### Scenario: Functional granularity in dimensions
- **WHEN** a refinement's `dimensions` contains `date_trunc('year', ordered_at)`
- **THEN** it merges as that time dimension, exactly as a query's `dimensions` entry does

#### Scenario: Empty refinement
- **WHEN** `monthly_revenue` runs with refinement `{}`
- **THEN** the result equals the plain run by name, with the same SQL

#### Scenario: Forbidden refinement fields
- **WHEN** a refinement carries `source_model`, `name`, `version`, `variables`, or an unknown key
- **THEN** it is rejected as invalid input before anything executes

### Requirement: Merge rules per field
Dimensions and measures SHALL be the saved entries followed by the refinement entries not already
present. A dimension column reference SHALL be identified by its reference with a leading root
qualifier dropped (`region` ≡ `orders.region`), a computed dimension by its name, a measure by its name
(an unnamed measure by its formula), a formula being compared by its parsed meaning with
any leading root qualifier dropped (`sum(amount)` ≡ `sum(orders.amount)`), never by its text, so text
inside a string literal or quoted identifier is compared exactly. Two entries with the same identity
SHALL collapse into one when they are equal in every field. A refinement measure whose formula is a
bare name, or that name qualified by the root, equal to a saved measure entry's name — named with that
same name, unnamed, or the string shorthand — SHALL restate that saved entry: it collapses into the
saved entry when every field it explicitly sets (`label`, `type`, `description`, `meta`; an explicit
`null` included) equals the saved entry's value, and conflicts otherwise. Time dimensions SHALL be
keyed by (dimension, granularity), the dimension compared as a formula is: a new key is appended; for
an existing key each of `date_range` and `label` takes the side that sets it, keeps equal values, and
conflicts when both sides set different values, `date_range` being compared after input
normalization. Filters SHALL be the saved filters followed by the refinement's, exact duplicate
strings dropped. `order`, `limit`, `offset`, `main_time_dimension`, `whole_periods_only`,
`distinct_dimension_values` and `to_many_handling` SHALL replace the saved value when the refinement
supplies them, an explicit empty `order` or `null` clearing the saved value, and SHALL leave it
unchanged otherwise.

#### Scenario: Identical duplicates collapse
- **WHEN** `monthly_revenue` runs with refinement `{"dimensions": ["orders.region"], "measures": [{"formula": "sum(amount)", "name": "revenue"}]}`
- **THEN** the result equals the extra-dimension scenario's, with no name-collision error

#### Scenario: A root-qualified formula is the saved formula
- **WHEN** `monthly_revenue` runs with refinement measures `[{"formula": "sum(orders.amount)", "name": "revenue"}]`, or with a refinement time dimension `date_trunc('month', orders.ordered_at)`
- **THEN** each collapses into the saved entry, and the result equals the plain run by name

#### Scenario: Literals are never conflated
- **WHEN** a saved query's unnamed measure is `sum(iif(status = 'paid', amount, 0))` and a refinement adds the unnamed measure `sum(iif(status = 'orders.paid', amount, 0))`
- **THEN** the refinement measure is appended as a distinct entry

#### Scenario: Restating a saved measure by name
- **WHEN** `monthly_revenue` runs with refinement measures `[{"formula": "revenue", "name": "revenue"}, "count(*)"]`, `[{"formula": "revenue"}, "count(*)"]`, `["revenue", "count(*)"]` or `["orders.revenue", "count(*)"]`
- **THEN** each result equals that of refinement measures `["count(*)"]`

#### Scenario: Restatement with differing metadata conflicts
- **WHEN** a refinement restates `revenue` as `{"formula": "revenue", "label": "Rev"}` and the saved entry's label differs (including a saved label with an explicit `null` restatement)
- **THEN** a refinement conflict error names `measures` and `revenue`

#### Scenario: Restatement under another name is not a restatement
- **WHEN** a refinement measure is `{"formula": "revenue", "name": "r2"}`
- **THEN** it is appended as a new entry, as before

#### Scenario: Same date range on the saved window
- **WHEN** a saved query whose month time dimension carries `date_range` `["2025-01-01", "2025-02-28"]` runs with a refinement giving that time dimension the same range, spelled as a list or with equal normalized values
- **THEN** it runs unchanged, with no error

#### Scenario: Filter narrowing a saved window
- **WHEN** that saved query (which also orders by `revenue` desc with `limit` 2) runs with refinement `{"filters": ["ordered_at >= '2025-02-01'"]}`
- **THEN** the single row is 2025-02 → 105

#### Scenario: Clearing a saved limit
- **WHEN** that saved query runs with refinement `{"limit": null}`
- **THEN** both window months return, in order 2025-02 → 105, 2025-01 → 100

#### Scenario: Scalar settings replace only when supplied
- **WHEN** a refinement supplies `to_many_handling` (or another replaceable setting)
- **THEN** the merged stage uses the refinement's value
- **WHEN** a refinement omits it
- **THEN** the saved value stands

#### Scenario: Duplicate filters dropped
- **WHEN** a refinement repeats a saved filter string exactly
- **THEN** the merged stage carries that filter once
