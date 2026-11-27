## MODIFIED Requirements

### Requirement: Measures may be named after transforms
A metric's name or alias SHALL NOT be rejected for equalling a transform name (`cumsum`, `change`,
`change_pct`, `time_shift`, `lag`, `lead`, `consecutive_periods`, `rank`,
`percent_rank`, `dense_rank`, `ntile`) or `first` / `last` — for query metrics, refinement metrics, query-extension
metrics and saved metrics. A transform name SHALL act as a transform, and `first` / `last` as an aggregation, only in call position;
as a bare name it SHALL resolve like any other alias, and a qualified saved metric so named like any other
saved metric, and the entry's result key SHALL be its alias. Importers SHALL import such metrics like any
other. Custom aggregation names keep their transform-name reservation.

#### Scenario: Query measure named change
- **WHEN** a monthly query on `orders` measures `{"metric": "change(sum(orders.amount))", "alias": "change"}`
- **THEN** the result key is `change`, and its rows and SQL equal those of the same query aliasing the metric `chg` with `chg` replaced by `change`

#### Scenario: The name is referenceable
- **WHEN** that query also filters `change > 0`, orders by `change`, or an outer stage reads its column `change`
- **THEN** each reference resolves to the metric

#### Scenario: Saved measure named rank
- **WHEN** a dataset `orders` declares a metric `rank` with expression `count(*)` and a query measures `orders.rank * 2` and `rank(sum(orders.amount), direction='desc')`
- **THEN** the first reads the saved metric and the second is the rank transform

#### Scenario: Importers keep transform-named measures
- **WHEN** a dbt, Cube or OSI project defines a measure named `rank`
- **THEN** the imported dataset carries that metric
