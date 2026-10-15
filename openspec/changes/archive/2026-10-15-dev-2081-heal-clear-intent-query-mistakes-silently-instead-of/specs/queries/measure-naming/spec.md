## ADDED Requirements

### Requirement: Measures may be named after transforms
A measure's name SHALL NOT be rejected for equalling a transform name (`cumsum`, `change`,
`change_pct`, `time_shift`, `first`, `last`, `lag`, `lead`, `consecutive_periods`, `rank`,
`percent_rank`, `dense_rank`, `ntile`) — for query measures, refinement measures, model-extension
measures and saved model measures. A transform name SHALL act as a transform only in call position;
as a bare name it SHALL resolve like any other measure name, and the measure's result key SHALL be the
name given. Importers SHALL import such measures like any other. Custom aggregation names keep their
transform-name reservation.

#### Scenario: Query measure named change
- **WHEN** a monthly query on `orders` measures `{"formula": "change(sum(amount))", "name": "change"}`
- **THEN** the result key is `orders.change`, and its rows and SQL equal those of the same query naming the measure `chg` with `chg` replaced by `change`

#### Scenario: The name is referenceable
- **WHEN** that query also filters `change > 0`, orders by `change`, or an outer stage reads its flat column `change`
- **THEN** each reference resolves to the measure

#### Scenario: Saved measure named rank
- **WHEN** a model declares a measure `rank` with formula `count(*)` and a query measures `rank * 2` and `rank(sum(amount), direction='desc')`
- **THEN** the first reads the saved measure and the second is the rank transform

#### Scenario: Importers keep transform-named measures
- **WHEN** a dbt, Cube or OSI project defines a measure named `rank`
- **THEN** the imported model carries that measure
