## MODIFIED Requirements

### Requirement: Every advertised facade column is selectable

Every column a facade catalog advertises — in `information_schema.columns`, `pg_attribute`, Flight `GetTables`
table schemas and `INFORMATION_SCHEMA.DIMENSIONS`, including dotted joined dimensions — SHALL be selectable alone
with a plain `SELECT <column> FROM <table>` and SHALL return its values, with the advertised wire type. A dimension
reached through a fanning join SHALL return one row per joined record. A list column SHALL also be groupable, one
group per element set.

#### Scenario: Every column of a mixed-type model
- **WHEN** a model has a list column, a JSON column, a UUID column, a decimal column, a bytes column, an interval
  column and joins to a to-one and a to-many model, and each advertised column is selected alone over the Postgres
  wire and over Arrow Flight
- **THEN** every selection succeeds and returns the column's values

#### Scenario: BI table preview
- **WHEN** a BI tool previews `contractors`, whose `skills` is a list, with `SELECT id, name, skills FROM contractors`
- **THEN** one row per contractor is returned, with `skills` as its stored list's JSON array text and a NULL list as NULL

#### Scenario: Grouping on a list
- **WHEN** a client runs `SELECT skills, COUNT(*) FROM contractors GROUP BY skills` over contractors whose `skills`
  are `['go', 'sql', 'go']`, `['sql', 'go']` and NULL
- **THEN** two rows are returned: `["go","sql"]` with 2, and NULL with 1

#### Scenario: Ordering a grouped list
- **WHEN** a client runs `SELECT skills, COUNT(*) FROM contractors GROUP BY skills ORDER BY skills`, or orders by the
  ordinal `1`
- **THEN** it fails with an error naming the list type
