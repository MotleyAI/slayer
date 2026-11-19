## ADDED Requirements

### Requirement: The home, not the query root, decides where an aggregation is computed

An aggregation over a row-level source whose home (per "Home dataset of a row-level aggregation source") is a dataset below the query root SHALL be computed over that home's rows exactly as when the query is rooted on any other dataset whose join paths reach the same home: the same value per cell it can attribute, the same input-safety outcome, the same refusal when it is refused. The longest common prefix of the source's own leaves (its anchor) SHALL NOT decide where it is computed. An aggregation homed on the query root, a second-order aggregation (homed on its operand dataset) and an aggregation in a query whose root is not a model SHALL keep their own evaluation.

#### Scenario: A root column and a list element home on the list child
- **WHEN** a query on `contractors` measures `sum(rate * length(skills.value))`, and a query on `assignments` measures `sum(contractors.rate * length(contractors.skills.value))`
- **THEN** both return 230 — each element counted once with its contractor's rate — by executed values; grouped by `team` (by `contractors.team` from `assignments`) both return A 130 and B 100

#### Scenario: A list-element definition default reads like from its home
- **WHEN** a custom aggregation `w` declared on `contractors` as `SUM({value} * length({weight}))` defaults `weight` to `skills.value`, and `w(rate)` is measured from `contractors` and `w(contractors.rate)` from `assignments`
- **THEN** both return 230, by executed values

#### Scenario: A root column and a reverse to-many column home on the child dataset
- **WHEN** a query on `contractors` measures `sum(rate * assignments.id)`, and a query on `assignments` measures `sum(contractors.rate * id)`
- **THEN** both return 460, by executed values

#### Scenario: Every producer kind is independent of the query root
- **WHEN** an aggregation homed below the query root on `contractors` is a `FILTER` aggregation, a ranked or `first` / `last` aggregation, a windowed aggregation, an aggregation with an attached constituent, or an explicit `partition_by=` aggregation, and the same aggregation is spelled from `assignments`
- **THEN** each returns the same values from both populations, or both populations refuse it with the same typed error

#### Scenario: Undetermined inputs are refused from every population
- **WHEN** an aggregation's inputs are determined by no single dataset over provably to-one hops
- **THEN** it is refused with the same input-safety error from every population that reaches its inputs

#### Scenario: A home-changing default after a redundant partition still homes below the root
- **WHEN** an aggregation homed below the query root also carries a redundant `partition_by=` that the query's grain already implies and a definition default that moves its home
- **THEN** it is computed at the same home and returns the same value as without the redundant `partition_by=`
