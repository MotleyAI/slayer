# queries/list-functions Specification

## Purpose
Defines the Mode-B list functions — `contains`, `contains_any`, `contains_all` and `length` over a list — together with the array literal, their typing, and which databases support them.

The examples use `contractors(id, team, skills ARRAY<TEXT>, required ARRAY<TEXT>)` with these rows:
- Ann: team A, skills `['go', 'sql', 'go']`
- Bob: team A, skills `['sql', NULL]`
- Cid: team B, skills `[]`
- Dee: team B, skills NULL
- Eve: team B, skills `['go']`

## Requirements

### Requirement: contains tests list membership

When its first argument is a list, `contains(L, x)` SHALL be TRUE when `x` is an element of `L` and FALSE otherwise. It SHALL be NULL only when `L` or `x` is NULL. When the first argument is `TEXT`, `contains` SHALL keep its substring meaning.

#### Scenario: Membership
- **WHEN** a dimension is `contains(skills, 'go')`
- **THEN** it is TRUE for Ann and Eve, FALSE for Bob and Cid, and NULL for Dee

#### Scenario: Filtering by membership
- **WHEN** a query on `contractors` measures `count(*)` with the filter `contains(skills, 'go')`
- **THEN** it returns 2

#### Scenario: Absence
- **WHEN** a query measures `count(*)` with the filter `NOT contains(skills, 'go')`
- **THEN** it returns 2 (Bob and Cid)

#### Scenario: The string meaning is unchanged
- **WHEN** a filter is `contains(team, 'A')` on the `TEXT` column `team`
- **THEN** it keeps Ann and Bob

### Requirement: contains_any and contains_all compare two lists

`contains_any(L, M)` SHALL be TRUE when `L` and `M` share an element and FALSE otherwise. `contains_all(L, M)` SHALL be TRUE when every element of `M` is an element of `L` and FALSE otherwise. Over an empty `M`, `contains_any` SHALL be FALSE and `contains_all` TRUE. Both SHALL be NULL only when `L` or `M` is NULL. `M` SHALL be any list-valued expression: an array literal, a list variable or a list column.

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

### Requirement: length counts a list's elements

When its argument is a list, `length(L)` SHALL return the number of non-NULL elements, 0 for an empty list and NULL for a NULL list. When the argument is `TEXT`, `length` SHALL keep its character-count meaning. A BigQuery NULL list reads as an empty list because BigQuery stores it as one.

#### Scenario: Element counts
- **WHEN** a dimension is `length(skills)`
- **THEN** it is 3 for Ann, 1 for Bob, 0 for Cid, NULL for Dee and 1 for Eve

### Requirement: Array literals and list variables

A bracketed list of literals `['go', 'sql']` SHALL be a list-valued expression, accepted wherever a list is accepted and rejected with a typed error elsewhere (for example as a dimension, or as an operand of `=`). Its elements SHALL be non-NULL literals of one type family. `[]` SHALL be accepted where the other operand gives it an element type, and rejected where nothing does. A `{variable}` holding a list SHALL be accepted wherever an array literal is.

#### Scenario: NULL element in a literal
- **WHEN** a filter is `contains_any(skills, ['go', NULL])`
- **THEN** the query fails at plan time with a typed error naming the NULL element

#### Scenario: Mixed literal types
- **WHEN** a filter is `contains_any(skills, ['go', 1])`
- **THEN** the query fails at plan time with a typed error naming the literal

#### Scenario: A literal as a dimension
- **WHEN** a dimension is `['go', 'sql']`
- **THEN** the query fails with a typed error naming the list type

### Requirement: Arguments match the element type

The value `x` of `contains(L, x)` and the elements of `M` SHALL belong to the type family of `L`'s elements. `TEXT` against a numeric element type SHALL follow the mixed-type coercion rule (a literal is converted at plan time, a column is read as a number). Any other mismatch SHALL be a typed plan-time error naming the function and both types.

#### Scenario: A numeric list and a text literal
- **WHEN** a filter is `contains(scores, '5')` on `scores ARRAY<INT>`
- **THEN** it is evaluated as `contains(scores, 5)`

#### Scenario: Incompatible types
- **WHEN** a filter is `contains(skills, DATE '2025-01-01')`
- **THEN** the query fails with a typed error naming `contains`, `TEXT` and `DATE`

### Requirement: One meaning on every database with native lists

Every list function SHALL return the same value for the same inputs on Postgres, DuckDB, BigQuery, Snowflake, ClickHouse and Trino, and SHALL be usable in filters, dimensions and formulas like any other scalar function. Each function SHALL be verified by execution on DuckDB and on every live backend suite, and by emission on every Tier-1 dialect.

#### Scenario: Every list function is covered on every dialect
- **WHEN** a list function is in the Mode-B library
- **THEN** the library matrix holds cases for it that execute on DuckDB, are refused at plan time on SQLite, and emit on every Tier-1 dialect
