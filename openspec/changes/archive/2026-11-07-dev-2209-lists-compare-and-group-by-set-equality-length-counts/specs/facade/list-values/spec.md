## MODIFIED Requirements

### Requirement: Facade catalogs keep models with list columns

A model with list columns SHALL stay in every facade catalog, with each list column typed as the facade's text type and selectable through a plain projection. The catalog SHALL advertise the `count` and `count_distinct` metrics of a list column its `allowed_aggregations` permit, each selectable, and SHALL NOT advertise any other built-in or custom-aggregation metric over a list column.

#### Scenario: Catalog listing
- **WHEN** a BI client lists the tables and columns of a datasource whose `contractors` model has a list column `skills`
- **THEN** `contractors` is listed, and `skills` is typed as text

#### Scenario: No custom aggregation over a list
- **WHEN** `contractors` has a parameterless custom aggregation `my_agg` and a list column `skills`
- **THEN** `INFORMATION_SCHEMA.METRICS` has no `skills_my_agg` entry, and keeps one for each scalar column

#### Scenario: Distinct count of lists as a metric
- **WHEN** a BI client lists `INFORMATION_SCHEMA.METRICS` for `contractors` and runs `SELECT skills_count_distinct FROM contractors`
- **THEN** `skills_count` and `skills_count_distinct` are listed, and the query returns the number of distinct `skills` element sets

## ADDED Requirements

### Requirement: A BI preview of a model with list columns returns its rows

A facade query projecting a list column next to a metric or aggregate SHALL group by the list's element set and return one row per combination of the projected dimensions, each list as its element set's JSON array text and a NULL list as SQL NULL; it SHALL NOT fail for grouping on a list.

#### Scenario: Preview with a saved measure
- **WHEN** a BI client runs `SELECT id, skills, row_count FROM contractors` over contractors whose `id` is unique, Ann's `skills` being `['go', 'sql', 'go']`
- **THEN** one row per contractor is returned with `row_count` 1, Ann's `skills` being the text `["go","sql"]` (whitespace aside)
