# models/identifier-columns Specification

## Purpose
Defines which model columns are identifiers — a model's sole primary-key column — and what that status restricts: the aggregations allowed over the column and its exclusion from sampling, type probing and profiling.

## Requirements

### Requirement: Only a sole primary key is an identifier
A column SHALL be an identifier exactly when it is its model's only primary-key column. An identifier SHALL be aggregatable only with `count`, `count_distinct`, `count_distinct_approx`, `min` and `max`, regardless of its type or `allowed_aggregations`, and SHALL be excluded from inspect sample grouping and sample measures, type probing, and value profiling. A member of a composite primary key is not an identifier: it SHALL be aggregatable per its type defaults (narrowed by `allowed_aggregations`) and sampled, probed and profiled like any other column. A composite primary key is unique only as a whole — none of its members is unique on its own.

#### Scenario: A sole primary key allows count-family and min/max only
- WHEN a model's only primary-key column is `id` (type number)
- THEN `count(id)`, `count_distinct(id)`, `min(id)` and `max(id)` are accepted and `sum(id)` is refused with an aggregation-not-allowed error

#### Scenario: A composite primary-key member aggregates by its type
- WHEN a model declares `order_id` and `line_no` (both number) as primary-key columns
- THEN `max(line_no)` and `sum(line_no)` are accepted

#### Scenario: allowed_aggregations on a composite member is checked against type defaults
- WHEN a composite primary-key member of type number declares `allowed_aggregations: ["sum"]`
- THEN the model is valid

#### Scenario: Composite members are profiled, a sole primary key is not
- WHEN a model with a composite primary key is inspected or profiled, and so is a model with a sole primary key
- THEN the composite members receive sample values and take part in type probing, while the sole primary-key column does not
