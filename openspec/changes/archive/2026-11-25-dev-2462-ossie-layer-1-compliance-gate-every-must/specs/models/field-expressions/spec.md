## ADDED Requirements

### Requirement: A source column keeps its identity through a relationship
A source column read through a relationship — as a dimension, in a filter, or as a metric's input — SHALL render with the same identity, quoting included, as when its own dataset is the query's root, on every Tier-1 dialect. A column whose name needs quoting (an upper-case letter, a reserved word, or any character outside a plain identifier) SHALL therefore read the same live column on a case-folding database (Postgres, Snowflake) whichever dataset roots the query.

#### Scenario: A mixed-case column of a joined dataset on Postgres
- **WHEN** dataset `invoice` relates to `customer`, whose field `externalId` reads the live column `"externalId"`, and a query rooted at `invoice` groups by `customer.externalId`, filters on it and sums a metric over it, on Postgres
- **THEN** the query executes and returns the same rows as on DuckDB, every reference rendering the column quoted

#### Scenario: Every Tier-1 dialect agrees
- **WHEN** the same query runs on each Tier-1 database
- **THEN** it returns the same rows on every one
