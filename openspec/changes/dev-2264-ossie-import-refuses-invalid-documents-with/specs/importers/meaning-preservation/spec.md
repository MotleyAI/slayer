## MODIFIED Requirements

### Requirement: Imported SQL reads with the source's identifier folding

Each importer SHALL read every unquoted identifier of the source's physical SQL — table names, source queries, member, field, entity and join expressions, FILTER_PARAMS bodies — with the identifier folding of the datasource's database, and store it in a spelling SLayer reads as the same name. On a dialect that folds unquoted identifiers (Postgres, Redshift, Snowflake, Oracle) an unquoted identifier holding an upper-case letter SHALL be stored lowercased (on Postgres its ASCII letters only); on every other dialect it SHALL be stored as written. Quoted identifiers, logical references (Cube member and cube references, dbt Jinja, OSI metric references), string literals, comments and the rest of the text SHALL be stored byte-identical. Names local to the SQL (CTE names, table and column aliases) SHALL be folded alike at their declaration and at every use. A dbt member without an `expr` SHALL read the physical column its name folds to and keep its name.

#### Scenario: Unquoted mixed-case table on Postgres
- **WHEN** a Postgres datasource holds `"Sales"."OrderItems"` with an integer `qty` and the row `(4)`, and `sales.orderitems` with a text `qty` and the row `('t')`, and a Cube cube's `sql_table` is `Sales.OrderItems` with dimension `qty`
- **THEN** `qty` is typed `TEXT` and grouping by `qty` returns `t`

#### Scenario: Quoted mixed-case table on Postgres
- **WHEN** the same cube's `sql_table` is `"Sales"."OrderItems"`
- **THEN** `qty` is typed `INT` and grouping by `qty` returns `4`

#### Scenario: The same two tables through dbt and OSI
- **WHEN** a dbt semantic model's SQL is `SELECT qty FROM Sales.OrderItems`, or an OSI dataset's `source` is `Sales.OrderItems`, over the same datasource
- **THEN** `qty` is typed `TEXT` and returns `t`; with `"Sales"."OrderItems"` written instead it is typed `INT` (the dbt SQL source: the numeric type its type category gives) and returns `4`

#### Scenario: Mixed-case column on Postgres
- **WHEN** on Postgres a Cube dimension `amount` has `sql: {CUBE}.OrderAmount` and a measure `total` sums `{CUBE}.OrderAmount`
- **THEN** both read the physical column `orderamount`

#### Scenario: Snowflake
- **WHEN** on a Snowflake datasource a cube's `sql_table` is `Sales.OrderItems` with dimension `amount` over `{CUBE}.OrderAmount`
- **THEN** the SQL SLayer generates for a query on `amount` reads the table `sales.orderitems` and the column `orderamount`, both unquoted

#### Scenario: A dialect that does not fold
- **WHEN** the cube above is imported over DuckDB
- **THEN** its `sql_table` and column SQL are stored as written

#### Scenario: Quoted names, literals, comments and logical references kept
- **WHEN** on Postgres a cube dimension's SQL is `CONCAT({CUBE}."Status", 'Paid', {CUBE}.Region) -- Note`
- **THEN** the stored SQL keeps `"Status"`, `'Paid'` and `-- Note` as written and reads the physical column `region`

#### Scenario: Names local to a source query
- **WHEN** on Postgres a cube's `sql` is `WITH Recent AS (SELECT Qty AS Total FROM Sales.OrderItems) SELECT Total FROM Recent ORDER BY Total`
- **THEN** the model reads `sales.orderitems` and exposes the column `total`

#### Scenario: dbt member without an expr
- **WHEN** on Postgres a dbt dimension `OrderDate` declares no `expr`
- **THEN** the imported column is named `OrderDate` and reads the physical column `orderdate`

#### Scenario: OSI reference to another dataset's field
- **WHEN** on Postgres an OSI field expression of dataset `orders` is `UPPER(Customers.Region)`, where `Customers` is another dataset of the same document with field `Region`
- **THEN** the import is refused, naming the field and the reference: a field expression reads only its own dataset's source columns (`importers/osi-import` › Invalid documents are refused with every violation)
