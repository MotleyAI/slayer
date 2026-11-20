## MODIFIED Requirements

### Requirement: Multi-stage saved queries refine their final stage
For a multi-stage saved query the refinement SHALL merge into the final stage, reading the earlier
stages exactly as the saved final stage does; a reference the final stage cannot resolve SHALL fail
with the ordinary resolution error.

#### Scenario: Refining the final stage
- **WHEN** the saved query `[{"name": "per_customer", "population": "orders", "dimensions": ["customer_id", "region"], "metrics": [{"metric": "sum(amount)", "alias": "revenue"}], "filter": ["status = 'paid'"]}, {"population": "per_customer", "metrics": [{"metric": "avg(revenue)", "alias": "avg_revenue"}]}]` runs by name
- **THEN** `avg_revenue` is 63.75
- **WHEN** it runs with refinement `{"dimensions": ["region"]}`
- **THEN** the rows are (US, 75.0), (EU, 52.5)
- **WHEN** it runs with refinement `{"filter": ["region = 'EU'"]}`
- **THEN** the value is 52.5

#### Scenario: A column aggregated away
- **WHEN** it runs with refinement `{"dimensions": ["status"]}`
- **THEN** the ordinary unknown-reference error for `status` is raised

### Requirement: Merge rules per field
Dimensions and measures SHALL be the saved entries followed by the refinement entries not already
present. A dimension column reference SHALL be identified by its reference in its converted,
qualified form (`region` ≡ `orders.region` in a stage naming population `orders`, `queries/references`), a computed dimension by its name, a measure by its name
(an unnamed measure by its formula), a formula being compared by its parsed meaning in
its converted, qualified form (`sum(amount)` ≡ `sum(orders.amount)`), never by its text, so text
inside a string literal or quoted identifier is compared exactly. Two entries with the same identity
SHALL collapse into one when they are equal in every field. A refinement measure whose formula is a
bare name, or that name qualified by the population, equal to a saved measure entry's name — named with that
same name, unnamed, or the string shorthand — SHALL restate that saved entry: it collapses into the
saved entry when every field it explicitly sets (`label`, `datatype`, `description`, `meta`; an explicit
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
- **WHEN** `monthly_revenue` runs with refinement `{"dimensions": ["orders.region"], "metrics": [{"metric": "sum(amount)", "alias": "revenue"}]}`
- **THEN** the result equals the extra-dimension scenario's, with no name-collision error

#### Scenario: A root-qualified formula is the saved formula
- **WHEN** `monthly_revenue` runs with refinement measures `[{"metric": "sum(orders.amount)", "alias": "revenue"}]`, or with a refinement time dimension `date_trunc('month', orders.ordered_at)`
- **THEN** each collapses into the saved entry, and the result equals the plain run by name

#### Scenario: Literals are never conflated
- **WHEN** a saved query's unnamed measure is `sum(iif(status = 'paid', amount, 0))` and a refinement adds the unnamed measure `sum(iif(status = 'orders.paid', amount, 0))`
- **THEN** the refinement measure is appended as a distinct entry

#### Scenario: Restating a saved measure by name
- **WHEN** `monthly_revenue` runs with refinement measures `[{"metric": "revenue", "alias": "revenue"}, "count(*)"]`, `[{"metric": "revenue"}, "count(*)"]`, `["revenue", "count(*)"]` or `["orders.revenue", "count(*)"]`
- **THEN** each result equals that of refinement measures `["count(*)"]`

#### Scenario: Restatement with differing metadata conflicts
- **WHEN** a refinement restates `revenue` as `{"metric": "revenue", "label": "Rev"}` and the saved entry's label differs (including a saved label with an explicit `null` restatement)
- **THEN** a refinement conflict error names `metrics` and `revenue`

#### Scenario: Restatement under another name is not a restatement
- **WHEN** a refinement measure is `{"metric": "revenue", "alias": "r2"}`
- **THEN** it is appended as a new entry, as before

#### Scenario: Same date range on the saved window
- **WHEN** a saved query whose month time dimension carries `date_range` `["2025-01-01", "2025-02-28"]` runs with a refinement giving that time dimension the same range, spelled as a list or with equal normalized values
- **THEN** it runs unchanged, with no error

#### Scenario: Filter narrowing a saved window
- **WHEN** that saved query (which also orders by `revenue` desc with `limit` 2) runs with refinement `{"filter": ["ordered_at >= '2025-02-01'"]}`
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
