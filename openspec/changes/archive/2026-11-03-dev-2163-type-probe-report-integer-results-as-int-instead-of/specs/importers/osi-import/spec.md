## MODIFIED Requirements

### Requirement: Field datatypes

A field's `datatype` SHALL map to SLayer types as: `String`→TEXT, `Integer`→INT, `Decimal` and `Float`→DOUBLE, `Boolean`→BOOLEAN, `Date`→DATE, `DateTime` and `DateTimeTz`→TIMESTAMP, `Time` and `Opaque`→UNKNOWN. A column's type SHALL be the type the database reports for it; when the database cannot report one, the mapped `datatype` SHALL be used, then the type of a source column the field shares its name with, and DOUBLE only when none exists. When both exist and disagree, the report SHALL hold an informational entry. The declared value SHALL be kept in the column's metadata. A column whose database type is exact-numeric or opaque SHALL keep the database's type name.

#### Scenario: Declared type when the database cannot report one
- **WHEN** a derived field `UPPER(name)` declares `datatype: String` and the database type probe fails
- **THEN** the column is TEXT

#### Scenario: Database type wins
- **WHEN** a field over an INTEGER source column declares `datatype: String`
- **THEN** the column is INT and the report holds an informational entry naming the field

#### Scenario: Time of day
- **WHEN** a field declares `datatype: Time` and the database reports no type
- **THEN** the column is UNKNOWN

#### Scenario: Integer expression
- **WHEN** a derived field `LENGTH(name)` over a DuckDB text column declares `datatype: Integer`, and a dataset-scoped metric aggregates a function the formula language does not offer over an integer operand
- **THEN** the field's column and the metric's hidden operand column are INT, and the report holds no datatype entry for the field

#### Scenario: Exact decimal keeps its database type
- **WHEN** a query-sourced dataset selects a DuckDB `DECIMAL(18,2)` column
- **THEN** the model's column is DOUBLE and records `DECIMAL(18,2)` as its database type
