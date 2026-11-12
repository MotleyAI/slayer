## MODIFIED Requirements

### Requirement: length counts a list's elements

When its argument is a list, `length(L)` SHALL return the size of its element set — the number of distinct non-NULL elements — 0 for an empty list and NULL for a NULL list; the count with repeats is `count(L.value)`. When the argument is `TEXT`, `length` SHALL keep its character-count meaning. A BigQuery or ClickHouse NULL list reads as an empty list.

#### Scenario: Element counts
- **WHEN** a dimension is `length(skills)`
- **THEN** it is 2 for Ann, 1 for Bob, 0 for Cid, NULL for Dee and 1 for Eve

#### Scenario: Repeats are counted through the elements
- **WHEN** a query on `contractors` has the dimensions `id` and `length(skills)` and the measure `count(skills.*)`
- **THEN** Ann's row carries 2 and 3
