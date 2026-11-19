## MODIFIED Requirements

### Requirement: Physical references read the physical column

Every column reference an importer takes from the source's own SQL (a reference to a column of the source table, as opposed to a reference to a source-defined member) SHALL read that physical column in the imported model, whatever fields the model declares, including when a different field of the model holds the physical column's name (a derived, filtered or renamed field); the shared name SHALL keep its own definition. Resolution SHALL NOT depend on the order in which the source declares its members. References inside nested scopes (subqueries, CTEs, `UNION` branches) SHALL be left as written, and the rest of the source's SQL text SHALL be kept verbatim, apart from the identifier folding of "Imported SQL reads with the source's identifier folding". A reference to a physical column that is absent, ambiguous or of undecidable case SHALL keep its existing verdicts in this capability.

#### Scenario: A renamed dimension holding a physical name
- **WHEN** a Cube cube over `orders` declares dimension `status` with `sql: {CUBE}.state` and dimension `status_upper` with `sql: UPPER({CUBE}.status)`
- **THEN** grouping by `status` returns `open`, `paid` and `x`, and `status_upper` returns `PAID`, `PAID` and `OPEN` for ids 1, 2 and 3

#### Scenario: The same collision in dbt
- **WHEN** a dbt semantic model over `orders` declares dimension `status` with `expr: state` and measure `paid_cnt` with `agg: sum` and `expr: CASE WHEN status = 'paid' THEN 1 ELSE 0 END`
- **THEN** `paid_cnt` imports and returns 2

#### Scenario: The same collision in OSI
- **WHEN** an OSI dataset over `orders` declares field `status: state` and field `status_upper: UPPER(status)`
- **THEN** `status_upper` imports and returns `PAID`, `PAID` and `OPEN` for ids 1, 2 and 3

#### Scenario: Declaration order does not matter
- **WHEN** the Cube cube above declares `status_upper` before `status`
- **THEN** both import with the same values

#### Scenario: Nested scopes are left alone
- **WHEN** a Cube dimension `min_amount` is `(SELECT MIN(amount) FROM orders)` and the cube also declares dimension `amount` with `sql: {CUBE}.amount * 2`
- **THEN** `min_amount` imports with the subquery kept as written and returns 5 on every row

## REMOVED Requirements

### Requirement: Self-named transforms are dropped with a report
**Reason**: A field's expression now reads its own-name source column directly, so a self-named transform imports with the source's meaning.
**Migration**: None needed; such members now import (see "Self-named transforms import with the source's meaning").

## ADDED Requirements

### Requirement: Self-named transforms import with the source's meaning

A source dimension or field whose definition transforms the physical column of its own name SHALL import in every importer, reading that physical column, with no report.

#### Scenario: Cube self-named dimension
- **WHEN** a Cube cube over `orders` declares dimension `status` with `sql: UPPER({CUBE}.status)`
- **THEN** grouping by `status` returns `PAID` for 2 rows and `OPEN` for 1

#### Scenario: dbt self-named dimension
- **WHEN** a dbt semantic model over `orders` declares dimension `status` with `expr: UPPER(status)`
- **THEN** grouping by `status` returns `PAID` for 2 rows and `OPEN` for 1

#### Scenario: OSI self-named field
- **WHEN** an OSI dataset over `orders` declares field `status: UPPER(status)`
- **THEN** grouping by `status` returns `PAID` for 2 rows and `OPEN` for 1

### Requirement: One hidden reader per physical column

When an importer needs a field to read a physical column and no unfiltered field reads it bare (a physical reader, `models/field-expressions` › A physical reader is a bare source column without a filter), it SHALL add one hidden field reading that column, and every surface of the import (dimensions, measures, filters, relationship keys, other cubes' references) SHALL reuse it. Physical columns SHALL be told apart by their exact spelling under the source's quoting and case rules. The hidden field SHALL be named `<column>_raw`, else `<column>_raw_2`, `<column>_raw_3` and so on, taking the first name free of the dataset's other fields, independent of the order members are declared or visited. Importing the same source twice SHALL produce the same datasets.

#### Scenario: A taken name gets the next suffix
- **WHEN** a Cube cube over `orders` declares dimension `status_raw` with `sql: {CUBE}.state`, dimension `status` with `sql: {CUBE}.state`, and a join keyed on `{CUBE}.status`
- **THEN** the join is keyed on a hidden field `status_raw_2` reading the physical `status`, and grouping by `status_raw` returns `open`, `paid` and `x`

#### Scenario: One helper serves every surface
- **WHEN** a cube's join key and one of another cube's dimensions both read the physical `status` of `orders`, whose dimension `status` is `UPPER({CUBE}.status)`
- **THEN** `orders` gains exactly one hidden field reading `status`, used by both

#### Scenario: Re-import is stable
- **WHEN** the same Cube project is imported twice
- **THEN** both imports produce identical datasets
