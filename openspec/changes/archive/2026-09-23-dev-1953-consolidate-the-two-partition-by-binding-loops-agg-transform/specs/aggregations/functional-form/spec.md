## ADDED Requirements

### Requirement: Repeated keyword arguments are rejected
A call in a Mode-B expression — an aggregation in functional or colon spelling, or a
transform — SHALL reject a keyword argument that appears more than once with a
parse-time error naming the call and the keyword; the parser never keeps the last
occurrence and never concatenates the values.

#### Scenario: Repeated partition_by on a transform
- **WHEN** a measure names `rank(sum(amount), partition_by=region, partition_by=city)`
- **THEN** parsing fails with an error naming `rank` and `partition_by`

#### Scenario: Repeated keyword on an aggregation
- **WHEN** a measure names `sum(amount, partition_by=region, partition_by=city)` or `amount:sum(partition_by=region, partition_by=city)`
- **THEN** parsing fails with an error naming the aggregation and `partition_by`
