## ADDED Requirements

### Requirement: A document source reads by Ossie's identifier rule
A stored document dataset's table-form `source` SHALL be read with the datasource dialect's identifier rule, as Ossie defines it: a quoted part denotes exactly its text, and an unquoted part is a regular identifier that denotes the name the database folds it to, whatever its case. Parts SHALL be separated only by a dot outside quotes.

#### Scenario: An unquoted mixed-case part folds
- **WHEN** on Postgres a document dataset has `source: public.Orders`, and the schema `public` holds the tables `orders` and `"Orders"`
- **THEN** a query on the dataset reads the table `orders`

#### Scenario: A quoted part is exact
- **WHEN** on Postgres a document dataset has `source: public."Orders"`
- **THEN** a query on the dataset reads the table `Orders`

### Requirement: Every stored source names the table SLayer reads
Every writer of a dataset's `source` (a save, the stored-format upgrade, ingestion and the importers) SHALL quote each part that SLayer's table-name rule reads as exactly its text (a quoted part, or an unquoted part holding an upper-case letter), escaping embedded quotes, and SHALL write every other part as it is, so that the source read by "A document source reads by Ossie's identifier rule" names the table the `sql_table` named.

#### Scenario: A mixed-case table name is quoted
- **WHEN** a dataset with `sql_table: ArtifactsCore` is stored
- **THEN** its stored `source` is `"ArtifactsCore"` and a query on Postgres reads the table `ArtifactsCore`

#### Scenario: Qualified names keep every part
- **WHEN** datasets with `sql_table` `"sales.v2".orders`, `sales."order items"`, `"Sales"."we""ird"` and `public."select"` are stored, reloaded, saved unchanged and exported
- **THEN** each stored `source` names the same table as its `sql_table`, and each step reproduces it
