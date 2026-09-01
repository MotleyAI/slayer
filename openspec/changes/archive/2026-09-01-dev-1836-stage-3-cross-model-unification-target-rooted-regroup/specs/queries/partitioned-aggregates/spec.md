# queries/partitioned-aggregates Delta

## ADDED Requirements

### Requirement: Partition keys are attributable from the aggregate's root
Every explicit `partition_by=` key SHALL be attributable from the aggregate's root — expressible over join hops that are provably many-to-one. An unattributable partition key is a hard error in both lenient and strict mode, naming the key, the failing hop, and the remedy; the producer MUST never join through an unproven or fanning hop to express a declared grain.

#### Scenario: Joined partition key over a provably safe hop works
- WHEN a local aggregate declares `partition_by=` naming a dimension reached over a provably many-to-one join
- THEN the producer computes at that grain with correct executed values

#### Scenario: Partition key over an unproven hop errors
- WHEN an aggregate declares `partition_by=` naming a dimension reachable only across a join with unproven arity
- THEN the query fails with a clear error naming the key and the remedy, never silently double-counting inside the producer

## MODIFIED Requirements

### Requirement: Attachment preserves cardinality structurally
Attaching a partitioned aggregate MUST never change the query's row count or any other column's values. The planner SHALL verify structurally that the attachment joins on the producer's complete unique key, and that a keyless attachment is provably single-row. The same verification SHALL apply to every nested attachment inside a producer — including attachments nested inside target-rooted (cross-model) producers — so no attach at any depth can multiply rows.

#### Scenario: Adding a partitioned measure is cardinality-neutral
- WHEN any supported query runs with and without an additional partitioned-aggregate measure
- THEN both runs return the same rows and identical values in all shared columns

#### Scenario: Empty partition set attaches the overall total
- WHEN a measure declares `partition_by=[]`
- THEN every row carries the overall total and the row count is unchanged

#### Scenario: Nested attachments inside a target-rooted producer are cardinality-checked
- WHEN a cross-model producer internally attaches a nested producer (e.g. a computed dimension it groups by)
- THEN the nested attach joins on the nested producer's complete unique key and the outer producer's row count is unchanged by it
