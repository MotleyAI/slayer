# queries/measure-naming (delta)

## Purpose

Governs how SLayer names result columns and SQL aliases for query measures the
user did not name: the derived-key convention, its uniformity and validity
across dialects, collision handling, and raw-formula referencing.

## ADDED Requirements

### Requirement: Unnamed formula measures derive sanitized identifier keys

The result-column key of an unnamed measure whose formula is not a plain
aggregate reference (an arithmetic composite, a transform, or a mix with
literals) SHALL be derived by sanitizing the canonical formula text into a bare
identifier under the product-wide expression-name convention: lowercase, every
run of non-alphanumeric characters collapsed to one `_`, leading/trailing `_`
stripped, a leading digit guarded, names over 48 characters folded to
`<head>_<hash8>_<tail>`, and no `__` in the result. The SQL projection alias
SHALL use the same derived name.

#### Scenario: Arithmetic composite

- **WHEN** an unnamed measure `logo_churn:sum / logo_bop:sum` is queried on model `mart`
- **THEN** its result key is `mart.logo_churn_sum_logo_bop_sum`

#### Scenario: Transform formula

- **WHEN** an unnamed measure `time_shift(cmrr_eop:sum, -1, 'year')` is queried on model `mart`
- **THEN** its result key is `mart.time_shift_cmrr_eop_sum_1_year`

#### Scenario: Formatting-insensitive derivation

- **WHEN** the same formula is written with different spacing (`logo_churn:sum/logo_bop:sum`)
- **THEN** it derives the identical result key

#### Scenario: Long formulas hash-fold

- **WHEN** an unnamed formula's sanitized name exceeds 48 characters
- **THEN** the key folds to the `<head>_<hash8>_<tail>` form deterministically

### Requirement: Named and aggregate-rooted measures keep their keys

Explicitly named measures, saved measures referenced by bare or dotted name,
and every measure whose formula is a plain (possibly parametric, star,
expression-source, or cross-model) aggregate reference SHALL keep result keys
and SQL aliases byte-identical to their existing convention.

#### Scenario: Explicit name wins

- **WHEN** a measure is `{"formula": "logo_churn:sum / logo_bop:sum", "name": "churn_ratio"}` on model `mart`
- **THEN** its result key is `mart.churn_ratio`

#### Scenario: Plain aggregate references unchanged

- **WHEN** unnamed measures `revenue:sum` and `*:count` are queried on model `orders`
- **THEN** their result keys are `orders.revenue_sum` and `orders._count`

#### Scenario: Saved measure reference unchanged

- **WHEN** a model saves measure `aov` and a query references it as the unnamed formula `aov`
- **THEN** its result key is `<model>.aov`

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

- **WHEN** unnamed measures `a:sum / b:sum` and `a:sum * b:sum` appear in one query
- **THEN** the query fails with an error naming both formulas and suggesting a rename

#### Scenario: Identical formulas merge

- **WHEN** the same unnamed formula appears twice in one query
- **THEN** the result contains that column once and no error is raised

#### Scenario: Identical formulas with conflicting metadata

- **WHEN** the same unnamed formula appears twice with a different explicit `type` or `label`
- **THEN** the query fails with an error suggesting a rename (set `name`)

### Requirement: Raw-formula references resolve to the derived column

A `filters` or `order` entry referencing an unnamed formula measure by its raw
formula text SHALL resolve to that measure's result column.

#### Scenario: Order by raw formula text

- **WHEN** a query has the unnamed measure `logo_churn:sum / logo_bop:sum` and orders by `logo_churn:sum / logo_bop:sum`
- **THEN** the query succeeds and orders by that measure's values
