## ADDED Requirements

### Requirement: Imported SQL reads with the source's identifier folding

Each importer SHALL read every unquoted identifier of the source's physical SQL — table names, source queries, member, field, entity and join expressions, FILTER_PARAMS bodies — with the identifier folding of the datasource's database, and store it in a spelling SLayer reads as the same name. On a dialect that folds unquoted identifiers (Postgres, Redshift, Snowflake, Oracle) an unquoted identifier holding an upper-case letter SHALL be stored lowercased (on Postgres its ASCII letters only); on every other dialect it SHALL be stored as written. Quoted identifiers, logical references (Cube member and cube references, dbt Jinja, OSI metric references and references to another dataset's fields), string literals, comments and the rest of the text SHALL be stored byte-identical. Names local to the SQL (CTE names, table and column aliases) SHALL be folded alike at their declaration and at every use. A dbt member without an `expr` SHALL read the physical column its name folds to and keep its name.

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
- **WHEN** on Postgres an OSI field expression of dataset `orders` is `UPPER(Customers.Region)`, where `Customers` is another dataset with field `Region`
- **THEN** the reference resolves to field `Region` of `Customers`

### Requirement: An import whose identifier case cannot be determined is refused

When a Cube or dbt import has no datasource dialect (its datasource is not configured) and its physical SQL holds an unquoted identifier with an upper-case letter, or a dbt relation name part the quoting policy leaves unquoted or undetermined, the import SHALL be refused before any model is produced or saved, with an error listing those identifiers and saying to configure the datasource (it need not be reachable) or to quote the names. An import whose physical identifiers are all lowercase or quoted SHALL import without a dialect as before.

#### Scenario: Offline mixed-case Cube
- **WHEN** `slayer import-cube` runs with a datasource that is not configured and a cube whose `sql_table` is `Sales.OrderItems`
- **THEN** the command fails without a traceback, names `Sales` and `OrderItems`, and saves no model

#### Scenario: Offline lowercase Cube
- **WHEN** the same import runs with `sql_table: sales.orderitems` and lowercase member SQL
- **THEN** the models import as before

#### Scenario: Configured but unreachable datasource
- **WHEN** the datasource is a configured Postgres datasource that cannot be reached and the cube's `sql_table` is `Sales.OrderItems`
- **THEN** the cube imports with `sql_table` `sales.orderitems`, and the report holds the informational entry saying types were not checked

### Requirement: An unparseable fragment that may need folding is dropped

On a dialect that folds unquoted identifiers, a physical SQL fragment the importer cannot parse that holds an unquoted word with an upper-case letter SHALL drop the construct holding it — the member, or the whole model when it is the model's source query or table name — with a report naming the fragment and saying its identifiers could not be read with the source's meaning.

#### Scenario: Unparseable mixed-case dimension
- **WHEN** on Postgres a cube dimension `doubled` has `sql: {CUBE}.Amount +* 2`
- **THEN** `doubled` is dropped with a report naming its SQL, and the rest of the cube imports

## MODIFIED Requirements

### Requirement: Physical references read the physical column

Every column reference an importer takes from the source's own SQL (a reference to a column of the source table, as opposed to a reference to a source-defined member) SHALL read that physical column in the imported model, whatever logical columns the model declares. When a different column of the model holds the physical column's name (a derived, filtered or renamed column), the construct holding the reference SHALL be dropped with a report naming the reference, and the shared name SHALL keep its own definition. Resolution SHALL NOT depend on the order in which the source declares its members. References inside nested scopes (subqueries, CTEs, `UNION` branches) SHALL be left as written, and the rest of the source's SQL text SHALL be kept verbatim, apart from the identifier folding of "Imported SQL reads with the source's identifier folding".

#### Scenario: A renamed dimension holding a physical name
- **WHEN** a Cube cube over `orders` declares dimension `status` with `sql: {CUBE}.state` and dimension `status_upper` with `sql: UPPER({CUBE}.status)`
- **THEN** `status_upper` is dropped and the report names `status`; grouping by `status` returns `open`, `paid` and `x`

#### Scenario: The same collision in dbt
- **WHEN** a dbt semantic model over `orders` declares dimension `status` with `expr: state` and measure `paid_cnt` with `agg: sum` and `expr: CASE WHEN status = 'paid' THEN 1 ELSE 0 END`
- **THEN** `paid_cnt` is dropped and the report names `status`

#### Scenario: The same collision in OSI
- **WHEN** an OSI dataset over `orders` declares field `status: state` and field `status_upper: UPPER(status)`
- **THEN** `status_upper` is dropped and the report names `status`

#### Scenario: Declaration order does not matter
- **WHEN** the Cube cube above declares `status_upper` before `status`
- **THEN** the same construct is dropped

#### Scenario: Nested scopes are left alone
- **WHEN** a Cube dimension `min_amount` is `(SELECT MIN(amount) FROM orders)` and the cube also declares dimension `amount` with `sql: {CUBE}.amount * 2`
- **THEN** `min_amount` imports with the subquery kept as written and returns 5 on every row

### Requirement: Identifier matching follows the source dialect

A quoted physical identifier SHALL match a column exactly; an unquoted one SHALL match using the datasource dialect's identifier folding. A reference that matches more than one column SHALL drop the construct holding it, with a report naming the reference; this includes a reference that matches one live column by its folded spelling and another by its exact spelling. Emitted SQL SHALL keep the source's quoting.

#### Scenario: Case-only ambiguity
- **WHEN** a dbt model declares dimensions `s_lower` (`expr: status`) and `s_upper` (`expr: '"Status"'`) over two distinct physical columns, a dimension `s_len` with `expr: LENGTH(Status)`, and the datasource dialect folds unquoted identifiers case-insensitively
- **THEN** `s_len` is dropped and the report names `Status` as ambiguous

#### Scenario: Folded and exact live spellings on Snowflake
- **WHEN** a Snowflake table has columns `FOO` and `"foo"`, and a cube dimension reads `{CUBE}.foo`
- **THEN** the dimension is dropped and the report names `foo` as ambiguous

### Requirement: Column types come from the database first

When the import's datasource is configured and reachable, each imported column's type SHALL be the type the database reports: a base column's type from the live table (a list column as a list with its element type), an expression column's type from a type probe (a list result with element type `UNKNOWN`). A source's table name SHALL name the table the source means — quoted, unquoted mixed-case (read per "Imported SQL reads with the source's identifier folding"), reserved-word, schema- and catalog-qualified names alike — and a source's own SQL SHALL be a read-only query. When the database cannot report a type — including a source SQL or expression the importer cannot read as SQL, or a source SQL that is not a read-only query — the importer SHALL use, in order: the source's declared type; the numeric type a `sum` or `avg` implies (`sum_boolean` → integer); `TEXT` for a column only counted. A column with no type from any of these that a `min`, `max` or other order-dependent aggregation reads SHALL drop that measure with a report telling the user to re-import against a reachable datasource or declare the type. A declared type the database contradicts SHALL be reported for information.

#### Scenario: List measure source typed from the database
- **WHEN** a Cube measure `tag_cnt` is `count` over `{CUBE}.tags` and the datasource is reachable
- **THEN** the column `tag_cnt` reads is a list of text, and `validate_models` proposes no deletion for the model

#### Scenario: Offline count source
- **WHEN** the same cube is imported with the datasource not configured
- **THEN** `tag_cnt` imports reading a `TEXT` column and the report holds one informational entry saying types were not checked against the database, naming the reason

#### Scenario: Offline min over an untyped source
- **WHEN** a Cube measure `latest` is `max` over `{CUBE}.created_at`, no dimension reads `created_at`, and the datasource is not configured
- **THEN** `latest` is dropped and the report says to re-import against a reachable datasource or declare the type

#### Scenario: Declared type contradicted by the database
- **WHEN** a dbt column declares `data_type: text` and the live column is an integer
- **THEN** the column is typed `INT` and the report holds an informational entry naming the column

#### Scenario: Mixed-case schema-qualified table
- **WHEN** a cube's `sql_table` is `"Sales"."Order Items"`, a live table with an integer column `qty` and a timestamp column `placed_at`, and the datasource is reachable
- **THEN** `qty` is typed `INT` and `placed_at` `TIMESTAMP` from the live table, and the report holds no entry saying types were not checked

#### Scenario: Reserved-word table name
- **WHEN** a dbt model or cube reads a live table named `order` (bare in the source) with an integer column `amount`, and the datasource is reachable
- **THEN** `amount` is typed `INT` from the live table

#### Scenario: Catalog-qualified table
- **WHEN** a cube's `sql_table` is `analytics.sales.orders` on a datasource that reaches catalog `analytics`, and `orders` has an integer column `qty`
- **THEN** `qty` is typed `INT` from the live table

#### Scenario: Quoted name containing a dot
- **WHEN** a cube's `sql_table` is `"sales.v2".orders`, naming table `orders` in the schema `sales.v2`
- **THEN** the columns are typed from that table, not from a schema `sales` or a table `v2`

#### Scenario: Expression probe over a quoted source
- **WHEN** a dbt dimension's `expr` is `qty * 2` over a model whose source is a mixed-case or reserved-word table, or SQL selecting from one, and the datasource is reachable
- **THEN** the dimension is typed from the probe — `INT` where the driver names the type, else the numeric type its type category gives — never from the offline fallback

#### Scenario: Source SQL the importer cannot read
- **WHEN** a dbt model's SQL, with refs inlined, is text the importer cannot read as a query, or is not a read-only query
- **THEN** the import completes, its columns fall back per this requirement, and the report holds an informational entry saying types were not checked against the database, naming the reason
