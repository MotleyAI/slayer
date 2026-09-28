## ADDED Requirements

### Requirement: Expressions over a computed dimension's whole aggregate evaluate as the dimension's value
When a computed dimension's whole expression is a partitioned aggregate, a re-aggregation or a transform, that expression — alone or inside arithmetic or a scalar call — in measure or order position SHALL evaluate as the dimension's value per result cell and execute on every supported dialect, never with an internal placeholder, render or partition-key error. The shared aggregate SHALL be computed once and attached once.

Oracles use the DEV-1847 `sales` fixture with `P` = `amount:sum(partition_by=region)` (North 90, South 140, East 180, Gap 20, Void NULL), `R` = `avg(sum(amount, partition_by=[city, region]), partition_by=region)` (North 45, South 70, East 60, Gap 10, Void NULL) and `tot` = `amount:sum`.

#### Scenario: Arithmetic over a partitioned-aggregate dimension as a measure
- **WHEN** a query over dimensions `[region, rd]`, with `rd` = `P`, declares the measures `P + 1` and `P * 2 + amount:sum`
- **THEN** `P + 1` is North 91, South 141, East 181, Gap 21, Void NULL and `P * 2 + amount:sum` is North 270, South 420, East 540, Gap 60, Void NULL

#### Scenario: Arithmetic over a re-aggregation dimension as a measure
- **WHEN** a query over dimensions `[region, rd]`, with `rd` = `R`, declares the measures `R + 1` and `R * 2 + amount:sum`
- **THEN** `R + 1` is North 46, South 71, East 61, Gap 11, Void NULL and `R * 2 + amount:sum` is North 180, South 280, East 300, Gap 40, Void NULL

#### Scenario: Arithmetic over an aggregate dimension as an order target
- **WHEN** the `rd` = `P` query orders by `P + 1` descending, and the `rd` = `R` query orders by `R + 1` descending
- **THEN** the first arrives East, South, North, Gap, Void and the second South, East, North, Gap, Void

#### Scenario: Arithmetic over a transform dimension as an order target
- **WHEN** a query over dimensions `[region, rk]`, with `rk` = `rank(P)`, orders by `rank(P) + 1` descending
- **THEN** rows arrive in descending `rk` order

#### Scenario: A finer-grained aggregate dimension read as a measure
- **WHEN** a query over dimensions `[region, x]`, with `x` = `amount:sum(partition_by=[city, region])`, declares the measures `amount:sum(partition_by=[city, region])` and `amount:sum(partition_by=[city, region]) + 1`
- **THEN** on every row the first equals `x` and the second equals `x` plus 1
