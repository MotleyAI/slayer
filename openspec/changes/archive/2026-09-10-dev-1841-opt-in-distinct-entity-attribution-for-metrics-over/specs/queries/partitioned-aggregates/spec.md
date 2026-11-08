# queries/partitioned-aggregates delta

## MODIFIED Requirements

### Requirement: Partition keys are attributable from the aggregate's root
Under `to_many_handling: "broadcast"` and `"error"`, every explicit `partition_by=`
key SHALL be attributable from the aggregate's root — expressible over join hops that
are provably many-to-one. An unattributable partition key is a hard error in those
modes, naming the key, the failing hop, and the remedy; the producer MUST never join
through an unproven or fanning hop to express a declared grain. Under `"associate"`,
an explicit partition key not attributable from the root is legal: the aggregate
attributes at the declared grain by distinct-entity association (per
`queries/attribution-modes`), without warning.

#### Scenario: Joined partition key over a provably safe hop works
- **WHEN** a local aggregate declares `partition_by=` naming a dimension reached over a
  provably many-to-one join
- **THEN** the producer computes at that grain with correct executed values

#### Scenario: Partition key over an unproven hop errors
- **WHEN** an aggregate declares `partition_by=` naming a dimension reachable only
  across a join with unproven arity, under `"broadcast"` or `"error"` mode
- **THEN** the query fails with a clear error naming the key and the remedy, never
  silently double-counting inside the producer

#### Scenario: Unattributable partition key attributes under associate
- **WHEN** the same aggregate runs under `to_many_handling: "associate"`
- **THEN** it computes at the declared grain over distinct associated entities with
  correct executed values and no warning
