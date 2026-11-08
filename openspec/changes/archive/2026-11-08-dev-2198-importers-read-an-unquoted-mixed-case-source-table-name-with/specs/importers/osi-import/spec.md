## MODIFIED Requirements

### Requirement: Selected expressions are rendered for the target datasource

The selected expression SHALL be read with the conventions of its own dialect, where `OSSIE_SQL_2026` and `ANSI_SQL` read Ossie's portable function set (including `IFF`, `ZEROIFNULL`, `DATEADD`, `STARTSWITH` and `||`), and its unquoted identifiers SHALL be read with the target datasource's identifier folding. Every raw-SQL definition the import stores (a column's SQL, a column's filter) SHALL be rendered in the target datasource's SQL dialect. An expression whose dialect is the target datasource's own SHALL be stored verbatim, apart from the identifier folding of "Imported SQL reads with the source's identifier folding" (`importers/meaning-preservation`). An expression that cannot be parsed SHALL be reported for its entity.

#### Scenario: Portable function rendered for Postgres
- **WHEN** a field is `sql: "IFF(amount > 0, amount, 0)"` and the target is a Postgres datasource
- **THEN** the stored column SQL is valid Postgres and querying the column returns `amount` for positive amounts and `0` otherwise

#### Scenario: Concatenation on MySQL
- **WHEN** a field is `sql: "first_name || ' ' || last_name"` and the target is a MySQL datasource
- **THEN** the stored column SQL concatenates the strings on MySQL

#### Scenario: Target-native kept verbatim
- **WHEN** the selected entry is `SNOWFLAKE` with expression `amount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the stored column SQL is exactly `amount::NUMBER(10,2)`

#### Scenario: Target-native mixed-case identifier folded
- **WHEN** the selected entry is `SNOWFLAKE` with expression `OrderAmount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the stored column SQL is exactly `orderamount::NUMBER(10,2)`

#### Scenario: Portable expression folded by the target
- **WHEN** a field is `sql: "IFF(OrderAmount > 0, OrderAmount, 0)"` and the target is a Postgres datasource
- **THEN** the stored column SQL reads the physical column `orderamount`

#### Scenario: Unparseable expression
- **WHEN** a field's selected expression is `amount +* 2`
- **THEN** that field is reported and the dataset's other fields import
