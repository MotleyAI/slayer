## ADDED Requirements

### Requirement: List columns are profiled over their elements

A list column SHALL be profiled over its elements, through the same profiling path, caching, failure bounding and logging as any other column. Its `sampled_values` and `distinct_count` SHALL describe the distinct non-NULL elements by frequency for `TEXT` / `BOOLEAN` elements, and its range SHALL be the elements' min and max for numeric and temporal elements. A whole list SHALL never be sampled, and a list with `UNKNOWN` elements SHALL never be profiled.

#### Scenario: Text elements
- **WHEN** `inspect_model` profiles `contractors.skills` over rows holding `['go', 'sql', 'go']`, `['sql', NULL]`, `[]`, NULL and `['go']`
- **THEN** `sampled_values` is `go` then `sql`, and `distinct_count` is 2

#### Scenario: Numeric elements
- **WHEN** a column `scores ARRAY<INT>` holding `[3, 9]` and `[1]` is profiled
- **THEN** its sampled range is 1 to 9

#### Scenario: Opaque elements
- **WHEN** a list column with `element_type: UNKNOWN` is read by `inspect_model`
- **THEN** no profiling query is executed for it
