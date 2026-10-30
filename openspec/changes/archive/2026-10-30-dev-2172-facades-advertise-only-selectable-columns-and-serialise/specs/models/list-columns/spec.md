## RENAMED Requirements

- FROM: `### Requirement: List columns are not dimensions, comparisons or general aggregation inputs`
- TO: `### Requirement: List columns are not group keys, comparisons or general aggregation inputs`

## MODIFIED Requirements

### Requirement: List columns are not group keys, comparisons or general aggregation inputs

A list column SHALL NOT be a group key: a query that groups (it has measures, or `distinct_dimension_values` is true) SHALL reject a list-valued dimension, whether it names the column or is an expression. A raw-rows query (`distinct_dimension_values: false`) SHALL project a list-valued dimension once per record. A list column SHALL NOT be an operand of `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN` or `IS DISTINCT FROM`, and SHALL NOT be an `order` key. `IS NULL` / `IS NOT NULL` SHALL be accepted. The only aggregation over a list-valued source SHALL be `count`, which counts non-NULL lists; every other built-in aggregation, `collect` and every custom aggregation over a list-valued source SHALL be rejected — whether the source is a column or an expression. `allowed_aggregations` on a list column naming anything but `count` SHALL fail at save time. Each rejection SHALL be a typed error naming its remedy.

#### Scenario: List as a dimension
- **WHEN** a grouping query on `contractors` has the dimension `skills`
- **THEN** it fails with a typed error naming `skills.value`

#### Scenario: List projected per record
- **WHEN** a query on `contractors` has the dimensions `id` and `skills` and `distinct_dimension_values: false`
- **THEN** it returns one row per contractor with that contractor's list, and NULL for a NULL list

#### Scenario: List-valued expression projected per record
- **WHEN** a raw-rows query has a computed dimension whose expression is the list column `skills`
- **THEN** it returns each contractor's list, and the same expression in a grouping query fails naming `skills.value`

#### Scenario: Ordering by a list
- **WHEN** a raw-rows query on `contractors` orders by `skills`
- **THEN** it fails with a typed error naming the list type

#### Scenario: Comparing a list
- **WHEN** a filter is `skills = 'go'` or `skills IN ('go', 'sql')`
- **THEN** it fails with a typed error naming `contains` / `contains_any`

#### Scenario: Distinct count of a list
- **WHEN** a measure is `count_distinct(skills)`
- **THEN** it fails with a typed error naming `count_distinct(skills.value)`

#### Scenario: Counting lists
- **WHEN** a measure is `count(skills)` over rows holding `['go']`, `[]` and NULL
- **THEN** it returns 2

#### Scenario: Custom aggregation over a list
- **WHEN** a model's custom aggregation `my_agg` is applied as `my_agg(skills)`
- **THEN** the query fails with a typed error naming the list type

#### Scenario: Aggregating a collected list
- **WHEN** a measure is `min(collect(team))`
- **THEN** it fails with a typed error naming the list type

#### Scenario: Save-time allowed aggregations
- **WHEN** a model is saved with a list column whose `allowed_aggregations` is `[sum]`
- **THEN** the save fails with a typed error naming the column

#### Scenario: NULL test
- **WHEN** a filter is `skills IS NULL`
- **THEN** it keeps exactly the rows whose list is NULL
