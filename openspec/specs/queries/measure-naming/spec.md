# queries/measure-naming Specification

## Purpose
Governs how SLayer names result columns and SQL aliases for query measures the
user did not name: the derived-key convention, its uniformity and validity
across dialects, collision handling, and raw-formula referencing.

## Requirements

### Requirement: Unnamed formula measures derive sanitized identifier keys

The result-column key of an unaliased metric whose expression is not a plain
aggregate reference (an arithmetic composite, a transform, or a mix with
literals) SHALL be the longest common anchored prefix of its references
(`queries/references`), followed by a bare identifier derived by sanitizing the
canonical expression text relative to that prefix under the product-wide
expression-name convention: lowercase, every run of non-alphanumeric characters
collapsed to one `_`, leading/trailing `_` stripped, a leading digit guarded,
names over 48 characters folded to `<head>_<hash8>_<tail>`, and no `__` in the
result. When the references share no anchor, the key SHALL be the sanitized
full expression text with no prefix. The query's population SHALL NOT enter the
key. The SQL projection alias SHALL use the same derived name. In the canonical
expression text a rank-family `direction` SHALL appear as its bare normalised
value (`asc` / `desc`), never as `direction=...`.

#### Scenario: Arithmetic composite

- **WHEN** an unaliased metric `sum(mart.logo_churn) / sum(mart.logo_bop)` is queried
- **THEN** its result key is `mart.logo_churn_sum_logo_bop_sum`, whatever the population

#### Scenario: Transform formula

- **WHEN** an unaliased metric `time_shift(sum(mart.cmrr_eop), -1, 'year')` is queried
- **THEN** its result key is `mart.time_shift_cmrr_eop_sum_1_year`

#### Scenario: Rank direction spelled as its bare value

- **WHEN** the unaliased metrics `rank(sum(o.a), direction='desc')`,
  `rank(sum(o.a), direction='Ascending')` and
  `rank(sum(o.a), partition_by=o.r, direction='asc')` are queried
- **THEN** their result keys are `o.rank_a_sum_desc`, `o.rank_a_sum_asc` and
  `o.rank_a_sum_partition_by_r_asc`, while `ntile(sum(o.a), n=4)` keeps
  `o.ntile_a_sum_n_4`

#### Scenario: Formatting-insensitive derivation

- **WHEN** the same expression is written with different spacing (`sum(mart.logo_churn)/sum(mart.logo_bop)`)
- **THEN** it derives the identical result key

#### Scenario: Long formulas hash-fold

- **WHEN** an unaliased expression's sanitized name exceeds 48 characters
- **THEN** the key folds to the `<head>_<hash8>_<tail>` form deterministically

#### Scenario: References with no common anchor

- **WHEN** an unaliased metric `sum(orders.amount) / count(customers.*)` is queried
- **THEN** its result key is the sanitized full expression text, with no dataset prefix

### Requirement: Derived keys are dialect-uniform and emitted aliases dialect-valid

The derived key of an unnamed formula measure SHALL be identical on every
supported dialect, and generated SQL for every supported dialect SHALL project
such a measure under an alias that is valid for that dialect after the
dialect's emission rewrites.

#### Scenario: BigQuery accepts the alias

- **WHEN** SQL is generated for BigQuery for an unnamed arithmetic or transform measure
- **THEN** every projection alias in the emitted SQL is a valid BigQuery identifier

#### Scenario: One key across all dialects

- **WHEN** the same query with unnamed formula measures is generated for each supported dialect
- **THEN** the derived result keys are identical across all of them

### Requirement: Colliding derived keys fail loudly

Two *different* unnamed formulas in one query node whose derived keys coincide
SHALL raise a clear error naming both formulas and the remedy (set `name`);
identical formulas SHALL merge into one result column.

#### Scenario: Different formulas, same derived key

- **WHEN** unnamed measures `sum(a) / sum(b)` and `sum(a) * sum(b)` appear in one query
- **THEN** the query fails with an error naming both formulas and suggesting a rename

#### Scenario: Identical formulas merge

- **WHEN** the same unnamed formula appears twice in one query
- **THEN** the result contains that column once and no error is raised

#### Scenario: Identical formulas with conflicting metadata

- **WHEN** the same unnamed formula appears twice with a different explicit `datatype` or `label`
- **THEN** the query fails with an error suggesting a rename (set `name`)

### Requirement: Raw-formula references resolve to the derived column

A `filter` or `order` entry referencing an unnamed formula measure by its raw
formula text SHALL resolve to that measure's result column.

#### Scenario: Order by raw formula text

- **WHEN** a query has the unnamed measure `sum(logo_churn) / sum(logo_bop)` and orders by `sum(logo_churn) / sum(logo_bop)`
- **THEN** the query succeeds and orders by that measure's values

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

### Requirement: Aliased and aggregate-rooted metrics key by alias or anchor

An aliased metric SHALL be keyed by its bare alias. An unaliased metric that is a plain (possibly parametric, star, expression-source, or cross-dataset) aggregate reference SHALL be keyed by its anchor and route as written, followed by the aggregate's derived name (`amount_sum`, and `_count` for `*`). An unaliased saved-metric reference SHALL be keyed `<anchor>.<route>.<metric>`. The SQL projection alias SHALL follow the key. None of these keys SHALL depend on the query's population.

#### Scenario: Explicit name wins
- **WHEN** a metric is `{"metric": "sum(mart.logo_churn) / sum(mart.logo_bop)", "alias": "churn_ratio"}`
- **THEN** its result key is `churn_ratio`

#### Scenario: Plain aggregate references key by their anchor
- **WHEN** unaliased metrics `sum(orders.revenue)` and `count(orders.*)` are queried with population `orders` and again with population `customers`
- **THEN** both queries key them `orders.revenue_sum` and `orders._count`

#### Scenario: Saved metric reference keys by its anchor
- **WHEN** a dataset saves a metric `aov` and a query references it unaliased as `<dataset>.aov`
- **THEN** its result key is `<dataset>.aov`
