## Purpose

Defines a field's and a metric's single definition key, Ossie's `expression` object, which of its forms runs on a datasource, and how names inside a definition resolve: a bare name reads a source column, a dotted path reads a field.

Scenarios use the table `orders(id, status, state, amount, tags)` of `importers/meaning-preservation`, with rows `(1, 'paid', 'open', 10, ['a', 'b'])`, `(2, 'paid', 'paid', 20, ['b'])` and `(3, 'open', 'x', 5, NULL)`, `amount` an integer column, read by a dataset `orders`.

## ADDED Requirements

### Requirement: A field is defined by one expression

A field SHALL be defined by one key, `expression`: an object with a portable `sql` string, a `dialects` list of `{dialect, expression}` entries, or both, and no other key. A plain string SHALL be accepted as shorthand for `{sql: <string>}`. A dataset SHALL be stored and returned with each `expression` exactly as written, shorthand or object, with every entry kept. A field with no `expression` SHALL read the source column of its own name.

#### Scenario: Shorthand and object are stored as written
- **WHEN** dataset `orders` declares field `double` with `expression: "amount * 2"` and field `triple` with `expression: {sql: "amount * 3", dialects: [{dialect: SNOWFLAKE, expression: "amount * 3"}]}`, and is saved and loaded
- **THEN** `double`'s expression is the string `amount * 2`, `triple`'s is the same object with its `SNOWFLAKE` entry, and querying them returns 20, 40, 10 and 30, 60, 15

#### Scenario: A field without an expression
- **WHEN** dataset `orders` declares field `amount` with no `expression`
- **THEN** querying `amount` returns 10, 20 and 5

#### Scenario: An object with neither form is refused
- **WHEN** a field is declared with `expression: {}`
- **THEN** it is refused with a typed error naming the field

### Requirement: The datasource's own dialect entry runs first

On a datasource, a field SHALL run the `dialects` entry whose dialect is the datasource's own; without one, it SHALL run its portable `sql`, or else an `OSSIE_SQL_2026` entry. A datasource's own dialect is named by its Ossie token (the datasource type's SQL dialect name, upper-cased), and dialect tokens SHALL be compared case-insensitively. Entries for other dialects SHALL be kept and SHALL never run. A field with two entries for the datasource's dialect, or with no form it can run there, SHALL be refused with a typed error naming the field and the dialects it carries.

#### Scenario: The datasource-dialect entry wins
- **WHEN** field `n` is `{sql: "amount * 2", dialects: [{dialect: DUCKDB, expression: "amount * 3"}]}`
- **THEN** on DuckDB querying `n` returns 30, 60 and 15, and on SQLite it returns 20, 40 and 10

#### Scenario: An Ossie dialect entry stands in for sql
- **WHEN** field `n`'s only entry is `{dialect: OSSIE_SQL_2026, expression: "amount * 2"}`
- **THEN** on SQLite and DuckDB querying `n` returns 20, 40 and 10

#### Scenario: A token matches whatever its case
- **WHEN** field `n`'s only entry is `{dialect: duckdb, expression: "amount + 1"}`
- **THEN** on DuckDB querying `n` returns 11, 21 and 6

#### Scenario: Two entries for one dialect are refused
- **WHEN** field `n` carries the entries `DUCKDB` and `duckdb` and the dataset is saved on a DuckDB datasource
- **THEN** the save is refused with a typed error naming `n` and `DUCKDB`

#### Scenario: No runnable form is refused
- **WHEN** field `n`'s only entry is `{dialect: SNOWFLAKE, expression: "amount::NUMBER"}` and the dataset is saved on a DuckDB datasource
- **THEN** the save is refused with a typed error naming `n` and `SNOWFLAKE`

### Requirement: Portable sql has Ossie's meaning; a dialect entry is the database's own SQL

A field's portable `sql` (or `OSSIE_SQL_2026` entry) SHALL be read as Mode B and SHALL mean the same on every dialect, with Ossie's numerics: `/` is true division and two-argument `ROUND` breaks ties away from zero. A datasource-dialect entry SHALL reach the database verbatim, apart from name resolution, and SHALL parse as exactly one SQL expression: a statement or several SHALL be refused when the field is constructed, with a typed error naming the field. A variable in a dialect entry SHALL bind only as a literal, as in every Mode-A surface.

#### Scenario: True division in portable sql
- **WHEN** field `q` is `expression: "amount / 4"`
- **THEN** on SQLite and DuckDB querying `q` returns 2.5, 5 and 1.25

#### Scenario: The database's division in a dialect entry
- **WHEN** field `q` is `{dialects: [{dialect: SQLITE, expression: "amount / 4"}]}`
- **THEN** on SQLite querying `q` returns 2, 5 and 1

#### Scenario: A statement is refused
- **WHEN** a field's dialect entry is `SELECT pw FROM secrets`
- **THEN** constructing the field fails with a typed error naming it

### Requirement: Inside a row definition a bare name is a source column and a path is a field

In a field's `expression`, a field's `filter` and a dataset's `filters`, a bare name, and `source.<name>`, SHALL read the column of that name of the dataset's own source, never a field. A dotted path from the declaring dataset SHALL read a field: `<dataset>.<field>` reads the dataset's own field (the dataset's name never drops out), and `<hop>.<field>` reads a field across a relationship, each hop spelled by its relationship's name, else its target dataset's name. When the dataset declares a field named `source`, the `source.` qualifier SHALL name that field. Inside a dialect entry, a dotted name SHALL resolve in this order: an alias of the text's own subquery or CTE, the declaring dataset's name, a hop; a dotted name matching none of these SHALL be left verbatim. A field whose definition reads its own source column of the same name SHALL be accepted; a definition that reads its own field by path SHALL be refused as a cycle.

#### Scenario: A self-named field reads its source column
- **WHEN** field `status` is `expression: "UPPER(status)"`
- **THEN** the dataset saves, and grouping by `status` returns `PAID` twice and `OPEN` once

#### Scenario: A source column whose name another field has taken
- **WHEN** field `status` is `expression: state` and field `status_upper` is `expression: "UPPER(status)"`
- **THEN** grouping by `status` returns `open`, `paid` and `x`, and `status_upper` returns `PAID`, `PAID` and `OPEN`

#### Scenario: A valid Ossie dataset keeps Ossie's meaning
- **WHEN** a dataset's source has columns `o_totalprice` (100, 200) and `amount` (1, 2), and it declares fields `amount: {sql: o_totalprice}` and `total: {sql: "amount * 2"}`
- **THEN** `amount` returns 100 and 200, and `total` returns 2 and 4

#### Scenario: A path reads the field
- **WHEN** field `status` is `expression: state` and field `shout` is `expression: "orders.status || '!'"`
- **THEN** `shout` returns `open!`, `paid!` and `x!`

#### Scenario: The source qualifier
- **WHEN** field `status` is `expression: state` and field `raw` is `expression: source.status`
- **THEN** `raw` returns `paid`, `paid` and `open`

#### Scenario: A field across a relationship
- **WHEN** `orders` relates many-to-one to `customers` through relationship `customers`, and field `cust_name` is `expression: customers.name`
- **THEN** `cust_name` returns each order's customer name

#### Scenario: A field named source takes the qualifier
- **WHEN** dataset `orders` declares a field `source`, and field `raw` is `expression: source.status`
- **THEN** the save is refused with a typed error naming `source`

#### Scenario: A field reading itself by path is a cycle
- **WHEN** field `status` is `expression: "orders.status || ''"`
- **THEN** the save is refused with the circular-definition error naming `status`

#### Scenario: A subquery in a dialect entry is left as written
- **WHEN** field `amount` is `expression: "amount * 2"` and field `min_amount` is `{dialects: [{dialect: DUCKDB, expression: "(SELECT MIN(o.amount) FROM orders o)"}]}`
- **THEN** on DuckDB `min_amount` returns 5 on every row

#### Scenario: An unknown qualifier is left verbatim
- **WHEN** a DuckDB dialect entry reads `main.orders.amount` inside a scalar subquery over `main.orders`
- **THEN** the qualified name reaches the database as written

#### Scenario: A filter reads source columns
- **WHEN** field `status` is `expression: state` and field `paid_amount` is `expression: amount` with `filter: "status = 'paid'"`
- **THEN** `paid_amount` returns 10, 20 and NULL

### Requirement: Paths in definitions keep their arity rules

A dotted path in a field's `expression` or `filter`, a dataset's `filters` or a metric's `expression` SHALL be judged by `models/column-definitions` exactly as a reference in a derived definition is: fanning hops, revisits and ambiguous hops are refused at save, unproven hops warn. A bare name SHALL never be such a reference. Save-time and query-time analysis SHALL read the same dependencies from both forms of a definition, portable and dialect entry, filters included.

#### Scenario: A fanning path is refused
- **WHEN** `customers` relates one-to-many to `orders` and a field of `customers` is `expression: orders.amount`
- **THEN** the save is refused with the fanning-hop error naming the field, the reference and the hop

#### Scenario: A cycle through both forms
- **WHEN** field `a` is `expression: "orders.b + 1"` and field `b` is `{dialects: [{dialect: DUCKDB, expression: "orders.a * 2"}]}`
- **THEN** saving the dataset on DuckDB is refused with the circular-definition error, and a stored copy of it is refused at query time with the same error

### Requirement: A physical reader is a bare source column without a filter

A field SHALL be a physical reader of a source column when the form it runs is that column's bare name, quoted or not, and it carries no `filter`. Relationship keys, schema drift and live type refinement SHALL treat exactly the physical readers as base fields and SHALL compare each by the source column it reads.

#### Scenario: A renamed reader is a base field
- **WHEN** field `customer_id` is `expression: cust_fk`
- **THEN** a relationship keyed on `customer_id` joins on the source column `cust_fk`, and drift checks `cust_fk` against the live table

#### Scenario: A filtered reader is not a base field
- **WHEN** field `paid_amount` is `expression: amount` with `filter: "status = 'paid'"`
- **THEN** a relationship keyed on `paid_amount` is refused with the join-key error

### Requirement: Source columns in portable sql are typed

In a field's portable `sql`, a source column SHALL take the field's declared `datatype` when the field reads it bare, else the source column's live type when known, else UNKNOWN. A derived field with a declared `datatype` other than TEXT or UNKNOWN SHALL be cast to it as today.

#### Scenario: Typed from the live column
- **WHEN** field `q` is `expression: "amount / 4"` with no `datatype` over the live integer column `amount`
- **THEN** the dataset saves and `q` is numeric, returning 2.5, 5 and 1.25

### Requirement: A metric is defined by one expression

A metric's `expression` SHALL be a string or the same object as a field's, stored as written. Only its portable `sql`, or else an `OSSIE_SQL_2026` entry, SHALL be evaluated; an entry for the datasource's own dialect SHALL be refused at save with a typed error naming the metric and saying to use `sql`, and entries for other dialects SHALL be kept and never run. In a metric's expression a bare name SHALL be a field, or a saved metric; `source.<name>` SHALL read the dataset's source column, unless the dataset declares a field named `source`; dotted paths SHALL keep their meaning.

#### Scenario: The source qualifier past a renamed field
- **WHEN** field `status` is `expression: state`, metric `paid_rows` is `count(*) FILTER (WHERE source.status = 'paid')` and metric `paid_state` is `count(*) FILTER (WHERE status = 'paid')`
- **THEN** `paid_rows` returns 2 and `paid_state` returns 1

#### Scenario: A metric's own-dialect entry is refused
- **WHEN** metric `p` is `{sql: "sum(amount)", dialects: [{dialect: DUCKDB, expression: "sum(amount)"}]}` and the dataset is saved on DuckDB
- **THEN** the save is refused with a typed error naming `p`

#### Scenario: Another dialect's entry is kept
- **WHEN** metric `p` is `{sql: "sum(amount)", dialects: [{dialect: SNOWFLAKE, expression: "SUM(amount)"}]}`
- **THEN** on DuckDB `p` returns 35, and the stored metric keeps the `SNOWFLAKE` entry

### Requirement: Dialect-dependent checks run where the dialect is known

The checks that need the datasource's dialect (which form runs, two entries for one dialect, no runnable form, a metric's own-dialect entry) SHALL run wherever the dataset's datasource is known: an engine save, a storage save whose store holds the datasource, and every query. Where it is not known, the save SHALL proceed and the query SHALL refuse with the same typed error. Every other check on an `expression` SHALL run on every save.

#### Scenario: Saved without its datasource, refused when queried
- **WHEN** a store holding no datasource config saves dataset `orders` whose field `n` has only a `DUCKDB` entry, and the dataset is later queried on a SQLite datasource
- **THEN** the save succeeds and the query is refused with a typed error naming `n` and `DUCKDB`

### Requirement: A field's sql converts to its expression

A field given with `sql` SHALL be converted to its `expression`, with exactly one `FutureWarning` and one `deprecated_input` warning per input document naming `sql`, `expression` and SLayer 3.0. A bare or double-quoted identifier SHALL become the shorthand string of that identifier. Any other text SHALL become the one `dialects` entry for the dataset's datasource's own dialect, verbatim except that each bare name that today reads a sibling field is written as that field's path from the dataset; dotted names SHALL stay as written. Values SHALL be unchanged. A field given both `sql` and `expression` SHALL be refused with a typed error naming both.

#### Scenario: A bare column
- **WHEN** a field is submitted as `{name: total, sql: amount}`
- **THEN** it is stored as `expression: amount` and the save reports one `deprecated_input` warning naming `sql`

#### Scenario: A sibling reference is qualified
- **WHEN** dataset `orders` on a DuckDB datasource is submitted with fields `doubled` (`sql: "amount * 2"`) and `ratio` (`sql: "doubled / 4"`)
- **THEN** `ratio` is stored as `{dialects: [{dialect: DUCKDB, expression: "orders.doubled / 4"}]}` and returns the values the `sql` spelling returned

#### Scenario: Both keys are refused
- **WHEN** a field carries `sql: amount` and `expression: amount`
- **THEN** it is refused with a typed error naming `sql` and `expression`

### Requirement: A field's sql waits for its dialect

When a field's `sql` needs a dialect entry and the datasource's dialect is not known (a field constructed in Python, or a stored dataset whose store holds no datasource config), the field SHALL keep `sql` as written, SHALL be returned with `sql`, and SHALL be converted wherever the dialect becomes known: at a save, at a query and at a load whose store holds the datasource. A save with no dialect known SHALL store it as written. The deprecation warning SHALL be emitted where the `sql` is supplied.

#### Scenario: Constructed in Python
- **WHEN** a field is constructed in Python with `sql="amount * 2"` and its dataset is saved through the engine on a DuckDB datasource
- **THEN** one `FutureWarning` is emitted at construction, the stored field is `{dialects: [{dialect: DUCKDB, expression: "amount * 2"}]}`, and querying it returns 20, 40 and 10

#### Scenario: Saved where no dialect is known
- **WHEN** the same dataset is saved to a store that holds no datasource config
- **THEN** the stored field keeps `sql: "amount * 2"`, and a later query on a DuckDB datasource returns 20, 40 and 10
