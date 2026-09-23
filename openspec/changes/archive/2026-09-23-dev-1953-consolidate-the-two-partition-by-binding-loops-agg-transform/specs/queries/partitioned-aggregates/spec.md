## ADDED Requirements

### Requirement: Transform partition keys bind like aggregate partition keys
A rank-family transform's own `partition_by=` SHALL bind exactly as an aggregation's
`partition_by=` does: each element — singly or in a list — is a column reference, a
dotted joined-model reference, or the name of a computed dimension declared in the
query, which resolves to that dimension's value; the same binding applies in the
measure, aggregation-parameter, filter, order-target and computed-dimension positions.
A name that is no declared computed dimension and no column SHALL fail as an unknown
reference; an element that is neither a column reference nor a computed-dimension name
SHALL fail with one error naming the construct — `aggregation` or `transform '<op>'` —
and the offending kind. An attach-carrying computed dimension (one whose expression
contains an aggregate) binds the same way; its filter and order positions execute, while
its measure and aggregation-parameter positions fail closed with a planner error, never
an unknown-reference error (target behaviour, DEV-1960: they execute like the
aggregation twin).

Values below are on the sales graph with `ureg` declared as the computed dimension
`upper(region)` and `spend_band` banding `sum(amount, partition_by=[city, region]) > 45`.

#### Scenario: Measure position
- **WHEN** a query over `[ureg, city]` selects `rank(sum(amount), partition_by=ureg)`
- **THEN** each city is ranked by its total within its upper-cased region — EAST: Zeta 1, Delta 2, Epsilon 2; NORTH: Beta 1, Alpha 2; SOUTH: Gamma 1, Alpha 2; GAP: the NULL city 1, Kappa 2; VOID: Xi 1 — and the bound partition grain equals that of `sum(amount, partition_by=ureg)`

#### Scenario: Aggregation-parameter position
- **WHEN** a query over `[ureg]` selects `weighted_avg(amount, weight=rank(sum(amount, partition_by=[ureg, city]), partition_by=ureg))`
- **THEN** each region's rows are weighted by their city's rank within the region: EAST 56, NORTH 120/7, SOUTH 36, GAP 7, VOID NULL

#### Scenario: Filter position
- **WHEN** a query over `[ureg, city]` filters `rank(sum(amount), partition_by=ureg) <= 1`
- **THEN** exactly the rank-1 rows survive: EAST/Zeta, NORTH/Beta, SOUTH/Gamma, GAP/NULL, VOID/Xi

#### Scenario: Order position
- **WHEN** a query over `[ureg, city]` orders by `rank(sum(amount), partition_by=ureg)` ascending
- **THEN** the five rank-1 rows precede every rank-2 row

#### Scenario: Computed-dimension position with a member key
- **WHEN** a query declares `ureg` and a second computed dimension `rank(sum(amount, partition_by=[city, ureg]), partition_by=ureg)` named `r`, selecting `sum(amount)`
- **THEN** rows group by `(ureg, r)`: EAST r=1 80 and r=2 100, NORTH r=1 60 and r=2 30, SOUTH r=1 100 and r=2 40, GAP r=1 12 and r=2 8, VOID r=1 NULL

#### Scenario: Mixed list of a column and a computed dimension
- **WHEN** a measure names `rank(sum(amount), partition_by=[ureg, product])`
- **THEN** the bound partition grain is `{upper(region), product}`, identical to the grain `sum(amount, partition_by=[ureg, product])` binds

#### Scenario: Non-column element names the construct
- **WHEN** a measure names `rank(sum(amount), partition_by=sum(amount))` or `sum(amount, partition_by=sum(amount))`
- **THEN** binding fails with "transform 'rank' partition_by must resolve to a column reference; got AggregateKey." or "aggregation partition_by must resolve to a column reference; got AggregateKey." respectively

#### Scenario: Undeclared name stays unknown
- **WHEN** a query over `[city]` (no `ureg` dimension) selects `rank(sum(amount), partition_by=ureg)`
- **THEN** binding fails with the unknown-reference error naming `ureg`

#### Scenario: Attach-carrying computed dimension in filter and order positions
- **WHEN** a query over `[spend_band, city]` filters `rank(sum(amount), partition_by=spend_band) <= 1`
- **THEN** exactly the rows hi/Gamma (100) and lo/Alpha (70) survive, and the same expression as an ascending order target sorts those two rows first

#### Scenario: Attach-carrying computed dimension in measure and parameter positions fails closed
- **WHEN** a query over `[spend_band, city]` selects `rank(sum(amount), partition_by=spend_band)`, or a query over `[spend_band]` selects `weighted_avg(amount, weight=rank(sum(amount), partition_by=spend_band))`
- **THEN** the key binds to the dimension's value and planning fails with a planner error, never an unknown-reference error (target behaviour, DEV-1960: both execute like `sum(amount, partition_by=spend_band)`)
