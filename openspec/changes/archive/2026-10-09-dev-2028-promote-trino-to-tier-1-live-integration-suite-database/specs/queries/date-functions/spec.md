## MODIFIED Requirements

### Requirement: Date functions agree across dialects
Every date function, part and unit SHALL produce the same result on every Tier-1 dialect
(SQLite, Postgres, DuckDB, MySQL, ClickHouse, SQL Server, Snowflake, BigQuery, Trino), independent of
session settings such as the first day of the week. On SQLite, where a DATE / TIMESTAMP column can
hold text that is not a valid date, a date function over such a value SHALL return NULL. Presto
and Athena SHALL render every date function the way Trino does.

#### Scenario: Weekday independent of session settings
- **WHEN** `date_part('day_of_week', order_date)` runs on SQL Server with any `DATEFIRST` setting
- **THEN** Monday is 1 and Sunday is 7

#### Scenario: SQLite malformed stored date
- **WHEN** `date_add(order_date, 1, 'month')` runs on SQLite for a row whose `order_date` text is `'not a date'`
- **THEN** the result for that row is NULL and the query succeeds

#### Scenario: Trino sub-day offset of a DATE
- **WHEN** `date_add(order_date, 2, 'hour')` runs on Trino for a row whose `order_date` is 2024-03-01
- **THEN** the result is the TIMESTAMP 2024-03-01 02:00:00

#### Scenario: Trino ISO calendar parts
- **WHEN** `date_part('iso_year', d)` and `date_part('day_of_week', d)` run on Trino for
  2024-12-30 (a Monday in ISO week 1 of 2025)
- **THEN** the results are 2025 and 1

#### Scenario: Presto and Athena render like Trino
- **WHEN** a query using `date_add`, `date_diff` and `date_part` is rendered for a `presto` or
  `athena` datasource
- **THEN** its interval counts, date-part fields, day and second gaps, and DATE-to-TIMESTAMP
  promotion are those rendered for Trino
