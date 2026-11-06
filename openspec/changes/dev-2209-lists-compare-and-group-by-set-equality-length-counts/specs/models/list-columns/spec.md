## RENAMED Requirements

- FROM: `### Requirement: List columns are not group keys, comparisons or general aggregation inputs`
- TO: `### Requirement: A whole list groups and compares as its element set`

## MODIFIED Requirements

### Requirement: A whole list groups and compares as its element set

A whole list's value for equality SHALL be its **element set**: its distinct non-NULL elements, sorted ascending as `collect` sorts them. A NULL list SHALL stay NULL, and a list holding only NULL elements SHALL equal `[]`. A query that groups (it has measures, or `distinct_dimension_values` is true) SHALL accept a list-valued dimension, whether it names the column or is an expression, form its cells by element-set equality and show each cell's element set; NULL lists SHALL form one cell of their own. A raw-rows query (`distinct_dimension_values: false`) SHALL project each record's list as stored, order and repeats kept. A list SHALL be accepted as a grain member wherever a group key is, including `partition_by=`. `=`, `<>`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM` and `IN` between two lists SHALL compare element sets, the lists' element types belonging to one type family as for `contains_any`; `L IN (M1, M2)` SHALL equal `L = M1 OR L = M2`. A list SHALL NOT be compared with a scalar, SHALL NOT be an operand of `<`, `<=`, `>`, `>=` or `BETWEEN`, and SHALL NOT be an `order` key. `IS NULL` / `IS NOT NULL` SHALL be accepted. The aggregations over a list-valued source SHALL be `count`, which counts non-NULL lists, and `count_distinct`, which counts distinct element sets of non-NULL lists; every other built-in aggregation, `collect` and every custom aggregation over a list-valued source SHALL be rejected — whether the source is a column or an expression. `allowed_aggregations` on a list column naming anything but `count` and `count_distinct` SHALL fail at save time. A query that groups by a list-valued dimension, or measures `count_distinct` over a list, SHALL carry a structured warning saying the operation reads whole lists as sets and naming the element alternative `<list>.value`. Each rejection SHALL be a typed error naming its remedy.

The scenarios use `contractors` with `skills ARRAY<TEXT>`: Ann `['go', 'sql', 'go']`, Bob `['sql', NULL]`, Cid `[]`, Dee NULL, Eve `['go']`, Fay `['sql', 'go']`, Gil `[NULL]`.

#### Scenario: List as a dimension
- **WHEN** a query on `contractors` has the dimension `skills` and the measure `count(*)`
- **THEN** it returns `['go', 'sql']` 2 (Ann, Fay), `['sql']` 1, `[]` 2 (Cid, Gil), `['go']` 1 and NULL 1, and the response carries a warning naming `skills.value`

#### Scenario: List projected per record
- **WHEN** a query on `contractors` has the dimensions `id` and `skills` and `distinct_dimension_values: false`
- **THEN** it returns one row per contractor with that contractor's list as stored — Ann's as `['go', 'sql', 'go']` — and NULL for a NULL list

#### Scenario: List-valued expression projected per record
- **WHEN** a raw-rows query has a computed dimension whose expression is the list column `skills`, and a grouping query has the same computed dimension and the measure `count(*)`
- **THEN** the raw-rows query returns each contractor's list as stored, and the grouping query returns the cells of the scenario "List as a dimension"

#### Scenario: A list as a partition key
- **WHEN** a query on `contractors` has the dimensions `id` and `skills` and the measure `count(*, partition_by=skills)`
- **THEN** Ann and Fay each carry 2, Cid and Gil each carry 2, and Bob, Dee and Eve each carry 1

#### Scenario: Ordering by a list
- **WHEN** a raw-rows query on `contractors` orders by `skills`
- **THEN** it fails with a typed error naming the list type

#### Scenario: Comparing two lists
- **WHEN** queries measure `count(*)` with the filters `skills = ['sql', 'go']`, `skills <> ['go']` and `skills IS NOT DISTINCT FROM []` in turn
- **THEN** they return 2 (Ann, Fay), 5 (Ann, Bob, Cid, Fay, Gil) and 2 (Cid, Gil)

#### Scenario: A list in a list of lists
- **WHEN** a query measures `count(*)` with the filter `skills IN (['go'], ['sql'])`
- **THEN** it returns 2 (Bob, Eve), and with `NOT IN` it returns 4 (Ann, Cid, Fay, Gil)

#### Scenario: Comparing a list
- **WHEN** a filter is `skills = 'go'` or `skills IN ('go', 'sql')`
- **THEN** it fails with a typed error naming `contains` / `contains_any`

#### Scenario: Ordering comparisons on lists
- **WHEN** a filter is `skills < ['go']` or `skills BETWEEN ['a'] AND ['z']`
- **THEN** it fails with a typed error naming the list type

#### Scenario: Lists of different element types
- **WHEN** a filter is `skills = [DATE '2025-01-01']`
- **THEN** it fails with a typed error naming `TEXT` and `DATE`

#### Scenario: Distinct count of a list
- **WHEN** a measure is `count_distinct(skills)`
- **THEN** it returns 4 (`['go', 'sql']`, `['sql']`, `[]`, `['go']`), and the response carries a warning naming `count_distinct(skills.value)`

#### Scenario: Counting lists
- **WHEN** a measure is `count(skills)` over rows holding `['go']`, `[]` and NULL
- **THEN** it returns 2

#### Scenario: Custom aggregation over a list
- **WHEN** a model's custom aggregation `my_agg` is applied as `my_agg(skills)`
- **THEN** the query fails with a typed error naming the list type

#### Scenario: Aggregating a collected list
- **WHEN** a measure is `min(collect(team))`
- **THEN** it fails with a typed error naming the list type

#### Scenario: Save-time allowed aggregations
- **WHEN** a model is saved with a list column whose `allowed_aggregations` is `[sum]`, and another with `[count, count_distinct]`
- **THEN** the first save fails with a typed error naming the column, and the second succeeds

#### Scenario: NULL test
- **WHEN** a filter is `skills IS NULL`
- **THEN** it keeps exactly the rows whose list is NULL

### Requirement: The list type carries an element type

A column whose values are lists of scalars SHALL have `type: ARRAY` and an `element_type` naming the scalar type of its elements (`TEXT`, `INT`, `DOUBLE`, `BOOLEAN`, `DATE`, `TIMESTAMP`, or `UNKNOWN` for opaque elements such as nested lists or records). `element_type` SHALL be required when the type is `ARRAY`, SHALL be rejected on any other type, and SHALL never be `ARRAY`. A column with `UNKNOWN` elements supports only NULL tests and `count`; every construct reading its elements or its element set — `length`, grouping, comparison, `IN`, `count_distinct` and `<column>.value` — SHALL fail with a typed error naming the opaque element type.

#### Scenario: Missing element type
- **WHEN** a model is saved with a column `type: ARRAY` and no `element_type`
- **THEN** the save fails with a typed error naming the column and `element_type`

#### Scenario: Element type on a scalar column
- **WHEN** a model is saved with a column `type: TEXT, element_type: INT`
- **THEN** the save fails with a typed error naming the column

#### Scenario: Opaque elements
- **WHEN** a column is `type: ARRAY, element_type: UNKNOWN` and queries use, in turn, the filter `col IS NULL` with the measure `count(col)`, the dimension `length(col)`, `col` as a dimension of a grouping query, and `col.value` as a dimension
- **THEN** the first query runs, and each of the others fails with a typed error naming the opaque element type

### Requirement: Re-ingest re-types TEXT list columns

When a stored column is `TEXT` and its live column is a native list, re-ingest SHALL change it to `type: ARRAY` with the live `element_type` and `db_type`, keeping every other stored attribute (description, label, meta) and keeping `allowed_aggregations` only as far as it names `count` and `count_distinct`. A stored `ARRAY` column's `element_type` SHALL NOT be overwritten, except a Snowflake `UNKNOWN` being re-inferred.

#### Scenario: Re-typing keeps metadata
- **WHEN** a stored model has `skills: TEXT` with a description, the live column is `VARCHAR[]`, and the datasource is re-ingested
- **THEN** `skills` is `type: ARRAY`, `element_type: TEXT`, and keeps its description

#### Scenario: Re-typing keeps only the count aggregation
- **WHEN** a stored `skills: TEXT` has `allowed_aggregations: [count, max]`, its live column is `VARCHAR[]`, and the datasource is re-ingested
- **THEN** `skills` is `type: ARRAY` with `allowed_aggregations: [count]`

#### Scenario: Re-typing keeps a distinct count
- **WHEN** a stored `skills: TEXT` has `allowed_aggregations: [count, count_distinct, min]`, its live column is `VARCHAR[]`, and the datasource is re-ingested
- **THEN** `skills` is `type: ARRAY` with `allowed_aggregations: [count, count_distinct]`

#### Scenario: A user's element type survives
- **WHEN** a stored `ARRAY` column has `element_type: TEXT` set by the user over a live `INTEGER[]` column, and the datasource is re-ingested
- **THEN** its `element_type` stays `TEXT`

### Requirement: Agents see list columns and their operations

`inspect` of a model or column, `search` column hits and `models_summary` SHALL render a list column's type as `ARRAY<element type>`, its element samples when profiled, and the operations it supports: `contains`, `contains_all` / `contains_any`, element access through `<column>.value`, and grouping and comparison as a set. The JSON rendering SHALL carry `element_type`.

#### Scenario: Rendered list column
- **WHEN** `inspect_model` renders `contractors` with a profiled `skills` column
- **THEN** the rendering shows `ARRAY<TEXT>`, the sampled elements, names `contains`, `contains_all` and `skills.value`, and says the list groups and compares as a set

## ADDED Requirements

### Requirement: BigQuery and ClickHouse read a NULL list as empty

On BigQuery, which stores a NULL list as an empty one, and on ClickHouse, which has no NULL list, a list that is NULL — stored, or absent across an outer join — SHALL read as `[]` in grouping, comparison, `length` and distinct counting.

#### Scenario: Absent list on ClickHouse
- **WHEN** on ClickHouse `assignments` joins `contractors` to-one, one assignment's contractor is missing, and a query on `assignments` has the dimension `contractors.skills` and the measure `count(*)`
- **THEN** that assignment is counted in the `[]` cell, together with the assignments of contractors whose list is empty
