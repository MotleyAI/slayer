## MODIFIED Requirements

### Requirement: Column types come from the database first

When the import's datasource is configured and reachable, each imported column's type SHALL be the type the database reports: a base column's type from the live table (a list column as a list with its element type), an expression column's type from a type probe (a list result with element type `UNKNOWN`). A source's table name SHALL be read with the source dialect's identifier rules — quoted, mixed-case, reserved-word, schema- and catalog-qualified names all name the table the source means — and a source's own SQL SHALL be a read-only query. When the database cannot report a type — including a source SQL or expression the importer cannot read as SQL, or a source SQL that is not a read-only query — the importer SHALL use, in order: the source's declared type; the numeric type a `sum` or `avg` implies (`sum_boolean` → integer); `TEXT` for a column only counted. A column with no type from any of these that a `min`, `max` or other order-dependent aggregation reads SHALL drop that measure with a report telling the user to re-import against a reachable datasource or declare the type. A declared type the database contradicts SHALL be reported for information.

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
- **THEN** the dimension is typed from the probe as the integer type the database reports

#### Scenario: Source SQL the importer cannot read
- **WHEN** a dbt model's SQL, with refs inlined, is text the importer cannot read as a query, or is not a read-only query
- **THEN** the import completes, its columns fall back per this requirement, and the report holds an informational entry saying types were not checked against the database, naming the reason

#### Scenario: Expression the importer cannot read
- **WHEN** a cube dimension's `sql` is text the importer cannot read as a SQL expression and declares `type: number`
- **THEN** the import completes and the dimension is typed from its declared type
