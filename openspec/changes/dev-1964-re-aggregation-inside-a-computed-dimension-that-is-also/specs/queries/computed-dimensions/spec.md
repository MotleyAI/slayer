## ADDED Requirements

### Requirement: An aggregate expression shared by a computed dimension and another position evaluates per position
When the same explicitly grained aggregate expression — a partitioned aggregate, a re-aggregation, a transform over them, or a cross-model re-aggregation — appears inside a computed dimension AND in another position of the same query (measure, measure-typed filter conjunct, order target), each occurrence SHALL evaluate as that position defines it: inside the dimension at row scope, broadcast onto the rows its grain determines; elsewhere at query grain (Axiom 13). The shared expression SHALL be computed once (one producer) and the query SHALL execute with correct values or fail with a typed query error — never an internal placeholder, materialisation, hidden-slot, name-collision or join-back error. Filtering or ordering by the computed dimension's NAME uses its banded output; filtering or ordering by the underlying expression uses the expression's value.

Oracles below use the DEV-1847 `sales` fixture with `R` = `avg(sum(amount, partition_by=[city, region]), partition_by=region)` (North 45, South 70, East 60, Gap 10, Void NULL), `rlevel` = `CASE WHEN R > 50 THEN 'hi' ELSE 'lo' END`, `tlevel` = `CASE WHEN rank(R) > 1 THEN 'top' ELSE 'rest' END`, and `tot` = `amount:sum`.

#### Scenario: Re-aggregation in a dimension and a measure-typed filter
- WHEN a query over dimensions `[region, rlevel]` selects `tot` and filters on `R < amount:sum`
- THEN the rows are East/hi 180, Gap/lo 20, North/lo 90, South/hi 140 (Void's NULL fails the predicate), and the filter `R > amount:sum` returns zero rows without error

#### Scenario: Re-aggregation in a dimension and an order target
- WHEN the same query is ordered by `R` descending
- THEN rows arrive South, East, North, Gap, then Void

#### Scenario: Re-aggregation in a dimension and a measure
- WHEN the same query also selects `R` as a measure
- THEN `R` per row equals the per-region values above and `rlevel` agrees with it

#### Scenario: Transform over a re-aggregation in a dimension
- WHEN a query over dimensions `[region, tlevel]` selects `tot`
- THEN South is `rest` and East, North, Gap and Void are `top`

#### Scenario: Transform over a re-aggregation in a dimension and elsewhere
- WHEN the `tlevel` query also selects `rank(R)` as a measure, or filters on `rank(R) > 1` or `rank(R) < 4`, or orders by `rank(R)` ascending
- THEN the rank values are South 1, East 2, North 3, Gap 4, Void 5 on every supported dialect; `rank(R) > 1` keeps East, North, Gap, Void; `rank(R) < 4` keeps South, East, North; and the ascending order is South, East, North, Gap, Void

#### Scenario: Two dimensions sharing a re-aggregation
- WHEN a query declares both `tlevel` and `rlevel` as dimensions and selects `tot` and `R`
- THEN the rows are South (rest, hi), East (top, hi), North (top, lo), Gap (top, lo), Void (top, lo) with `tot` unchanged, and no internal name reaches the user

#### Scenario: Ordering by a transform shared with a dimension
- WHEN a computed dimension is `CASE WHEN rank(amount:sum(partition_by=region)) > 1 THEN 'top' ELSE 'rest' END` and the query orders by `rank(amount:sum(partition_by=region))` ascending
- THEN rows arrive East, South, North, Gap, Void, and ordering by the dimension's name instead sorts by its banded value

#### Scenario: Windowed transform over a re-aggregation in a dimension with a filter
- WHEN a monthly query declares the dimension `cumsum(min(<attached operand>, partition_by=region))` and filters on that dimension
- THEN it fails at plan time with the `TimeAxisError` naming `cumsum`, exactly as the same transform over `amount:sum(partition_by=region)` does

#### Scenario: Cross-model re-aggregation in a dimension and a measure-typed filter
- WHEN a `corders` query over dimensions `[customers.regions.name, cl]`, with `cl` banding `avg(sum(amount, partition_by=customer_id), partition_by=customers.regions.name)` at 40, filters on that re-aggregation `< amount:sum`
- THEN the only row is North/lo with `amount:sum` 70
