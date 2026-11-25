## ADDED Requirements

### Requirement: An sql-mode base field is matched by the column it reads

Schema drift and live typing of an sql-mode dataset SHALL match each field against the result columns by its name, and a base field also by the physical column it reads, quoting removed, exactly as a table-backed dataset's base field is matched against its live table. A field matching neither SHALL be dropped by drift when it is a base field, and left untyped by live typing.

#### Scenario: A quoted base field is not dropped
- **WHEN** an sql-mode dataset whose query returns the column `amount` declares the field `total` with `expression: '"amount"'`, and a metric `sum(total)`
- **THEN** drift removes neither the field nor the metric

#### Scenario: A quoted base field is typed from its column
- **WHEN** the same dataset leaves `total` untyped and the result column `amount` is numeric
- **THEN** live typing reports `total` as numeric

#### Scenario: A quoted base field whose column vanished is dropped
- **WHEN** the dataset's query no longer returns `amount`
- **THEN** drift removes `total` and the metric reading it, with a reason naming `total`
