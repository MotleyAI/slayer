## MODIFIED Requirements

### Requirement: contains_any and contains_all compare two lists

`contains_any(L, M)` SHALL be TRUE when `L` and `M` share an element and FALSE otherwise. `contains_all(L, M)` SHALL be TRUE when every element of `M` is an element of `L` and FALSE otherwise. Over an empty `M`, `contains_any` SHALL be FALSE and `contains_all` TRUE. Both SHALL be NULL only when `L` or `M` is NULL. `M` SHALL be any list-valued expression: an array literal, a list variable or a list column. Two lists of the same element type SHALL compare whatever the database's physical array types (e.g. Postgres `VARCHAR[]` against `TEXT[]`, `BIGINT[]` against `INTEGER[]`).

#### Scenario: All of several values
- **WHEN** a query measures `count(*)` with the filter `contains_all(skills, ['go', 'sql'])`
- **THEN** it returns 1 (Ann)

#### Scenario: Any of several values
- **WHEN** a query measures `count(*)` with the filter `contains_any(skills, ['go', 'sql'])`
- **THEN** it returns 3 (Ann, Bob and Eve)

#### Scenario: Another list column
- **WHEN** a filter is `contains_any(skills, required)` and Ann's `required` is `['rust', 'sql']`
- **THEN** Ann is kept

#### Scenario: An empty selection
- **WHEN** the filter is `contains_all(skills, {wanted})` with the variable `wanted` set to `[]`
- **THEN** every contractor with a non-NULL `skills` is kept, and `contains_any(skills, {wanted})` keeps none

#### Scenario: Physical array types differ on Postgres
- **WHEN** on Postgres `skills` is a `VARCHAR[]` column and `levels` a `BIGINT[]` column, and filters are `contains_any(skills, ['go'])`, `contains_all(levels, [1, 2])` and `contains_any(skills, required)` with `required` a `TEXT[]` column
- **THEN** each runs and returns the same rows as over `TEXT[]` / `INTEGER[]` columns
