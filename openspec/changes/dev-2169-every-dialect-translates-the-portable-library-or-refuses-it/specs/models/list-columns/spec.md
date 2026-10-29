## MODIFIED Requirements

### Requirement: Backends without native lists reject list constructs

On SQLite, MySQL, MariaDB, SQL Server, Redshift, Databricks, Spark and Oracle, every list construct — a list function over a list, an array literal, element access, a list child and `collect` — SHALL fail at plan time with a typed error naming the construct and the database.

#### Scenario: List function on SQLite
- **WHEN** a model on a SQLite datasource declares a list column and a query filters `contains(skills, 'go')`
- **THEN** the query fails at plan time naming `contains` and SQLite

#### Scenario: collect on MySQL
- **WHEN** a query on a MySQL datasource has the measure `collect(team)`
- **THEN** it fails at plan time naming `collect` and MySQL

#### Scenario: Element access on Databricks
- **WHEN** a model on a Databricks datasource declares a list column `skills` and a query has the dimension `skills.value`
- **THEN** the query fails at plan time naming the element access and Databricks
