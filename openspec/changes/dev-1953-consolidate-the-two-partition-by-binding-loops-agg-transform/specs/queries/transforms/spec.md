## ADDED Requirements

### Requirement: Rank-family partition keys are operand-grain members
A rank-family transform (`rank`, `dense_rank`, `percent_rank`, `ntile`) partitions its
operand's cells, so every key in its own `partition_by=` SHALL be a member of the
transform's operand grain, in every position. The operand grain is the union over the
transform's input: an aggregate contributes its explicit `partition_by=` keys, else the
query grain (its dimensions and time buckets), plus the query's active time bucket when
it is windowed; a nested transform contributes its own operand grain, minus its time
axis when it is `first` or `last`; a composite contributes the union of its operands,
with a projected row-level leaf contributing itself; an input with neither aggregate nor
leaf is the query grain. A non-member key SHALL fail at plan time with an error naming
the transform, the key, the operand grain and the remedy (add the key to the inner
aggregate's `partition_by=`, or partition by a member). A key that is no query dimension
at all keeps the existing "not a query dimension" error, and an ungrained inner aggregate
in dimension position keeps the existing grain-self-containment error; both take
precedence over the membership error.

#### Scenario: Non-member query dimension as a measure
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=product)`
- **THEN** planning fails with an error naming `rank`, `product`, the operand grain `city, region` and the `partition_by=` remedy — it never executes by widening the grain

#### Scenario: Non-member query dimension in a filter
- **WHEN** the same query filters `rank(sum(amount, partition_by=[city, region]), partition_by=product) <= 2`
- **THEN** planning fails with the same error

#### Scenario: Non-member key in dimension position, plain column
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount, partition_by=[city, product]), partition_by=region)`
- **THEN** planning fails with the same error naming `region` and the grain `city, product`, never with an internal producer-slot error

#### Scenario: Non-member key in dimension position, computed-dimension name
- **WHEN** a query declares `ureg` = `upper(region)` and the dimension `rank(sum(amount, partition_by=[city, region]), partition_by=ureg)`
- **THEN** planning fails with the same error naming the key and the grain `city, region`, never with an internal error

#### Scenario: Member key executes
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=region)`
- **THEN** each row carries its (city, region) total's rank within the region: East Zeta 1, Delta 2, Epsilon 2; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap the NULL city 1, Kappa 2; Void Xi 1

#### Scenario: Ungrained inner keeps the query-dimension rule
- **WHEN** a query over the banded dimension alone selects `rank(sum(amount), partition_by=region)`
- **THEN** planning fails with the existing "partition_by column 'region' is not a query dimension" error listing the available dimensions; over `[region, band]` the same measure executes because the ungrained inner is grained at the query grain and `region` is a member

#### Scenario: Windowed inner admits the active bucket
- **WHEN** a monthly query selects `rank(sum(amount, window='1y', partition_by=customers.regions.name), partition_by=ordered_at)`
- **THEN** the partition key passes the operand-grain rule as the query's month bucket, a member contributed by the windowed inner

#### Scenario: Nested collapsing transform drops its axis
- **WHEN** a monthly query selects `rank(last(sum(amount, partition_by=[customers.regions.name, ordered_at])), partition_by=ordered_at)`
- **THEN** planning fails with the operand-grain error naming `ordered_at` and the grain `customers.regions.name`; with `partition_by=customers.regions.name` the key passes the rule

#### Scenario: Residue error precedes the membership rule
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount), partition_by=region)`
- **THEN** planning fails with the existing grain-self-containment error ("must declare partition_by= explicitly"), not the operand-grain error
