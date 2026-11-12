# models/table-names Specification

## Purpose
How a model's stored `sql_table` names its table: the identifier rule SLayer reads it with, and the spelling every writer — ingestion and the importers — stores, so a stored name always reads back as its live table.

## Requirements

### Requirement: A stored table name reads with SLayer's identifier rule

Each part of a stored `sql_table` SHALL denote: a quoted part, exactly its text; an unquoted part holding an upper-case letter, exactly its text; an unquoted part without one, the name the database folds it to. Parts SHALL be separated only by a dot outside quotes. Every reader of a stored `sql_table` — query generation, ingestion, schema-drift validation, import typing — SHALL read it by this rule.

#### Scenario: Unquoted mixed-case name
- **WHEN** on Postgres a model's `sql_table` is `ArtifactsCore`
- **THEN** a query on the model reads the table named exactly `ArtifactsCore`

#### Scenario: A schema whose name holds a dot
- **WHEN** a model's `sql_table` is `"sales.v2".orders` and that table exists with the model's columns
- **THEN** a query on the model returns that table's rows, and `validate_models` reports no drift for it

### Requirement: Every writer stores a table name that reads back as its live table

When ingestion or an importer stores the `sql_table` of a live table, each part SHALL be the live name, quoted whenever it is not a plain identifier — it holds a space, a dot or a quote, starts with a digit, or is a reserved word — so that reading it back by "A stored table name reads with SLayer's identifier rule" names the same live table.

#### Scenario: Table name with a space
- **WHEN** a Postgres schema `sales` holds a table `order items` and the datasource is ingested
- **THEN** the model's `sql_table` is `sales."order items"` and a query on the model returns the table's rows

#### Scenario: Schema name with a dot
- **WHEN** a table `orders` in a schema `sales.v2` is ingested
- **THEN** the model's `sql_table` is `"sales.v2".orders`, a query returns its rows, and re-ingesting changes nothing

#### Scenario: Mixed-case live name
- **WHEN** a Postgres table `"OrderItems"` is ingested
- **THEN** a query on its model reads the table named exactly `OrderItems`
