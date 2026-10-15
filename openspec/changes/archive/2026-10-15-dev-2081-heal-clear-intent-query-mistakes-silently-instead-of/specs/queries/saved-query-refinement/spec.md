## MODIFIED Requirements

### Requirement: Merge rules per field
Dimensions and measures SHALL be the saved entries followed by the refinement entries not already
present. A dimension column reference SHALL be identified by its reference with the source-model
prefix stripped (`region` ≡ `orders.region`), a computed dimension by its name, a measure by its name
and formula (an unnamed measure by its formula). Two entries with the same identity SHALL collapse
into one when they are equal in every field. A refinement measure whose formula is a bare name equal
to a saved measure entry's name — named with that same name, unnamed, or the string shorthand — SHALL
restate that saved entry: it collapses into the saved entry when every field it explicitly sets
(`label`, `type`, `description`, `meta`; an explicit `null` included) equals the saved entry's value,
and conflicts otherwise. Time dimensions SHALL be keyed by (column,
granularity): a new key is appended; for an existing key each of `date_range` and `label` takes the
side that sets it, keeps equal values, and conflicts when both sides set different values,
`date_range` being compared after input normalization. Filters SHALL be the saved filters followed
by the refinement's, exact duplicate strings dropped. `order`, `limit`, `offset`,
`main_time_dimension`, `whole_periods_only`, `distinct_dimension_values` and `to_many_handling` SHALL
replace the saved value when the refinement supplies them, an explicit empty `order` or `null`
clearing the saved value, and SHALL leave it unchanged otherwise.

#### Scenario: Identical duplicates collapse
- **WHEN** `monthly_revenue` runs with refinement `{"dimensions": ["orders.region"], "measures": [{"formula": "sum(amount)", "name": "revenue"}]}`
- **THEN** the result equals the extra-dimension scenario's, with no name-collision error

#### Scenario: Restating a saved measure by name
- **WHEN** `monthly_revenue` runs with refinement measures `[{"formula": "revenue", "name": "revenue"}, "count(*)"]`, `[{"formula": "revenue"}, "count(*)"]` or `["revenue", "count(*)"]`
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
