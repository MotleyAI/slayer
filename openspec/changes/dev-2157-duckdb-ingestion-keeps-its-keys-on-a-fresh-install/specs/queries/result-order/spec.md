## Purpose

Defines the order of result rows when a query declares no `order`, so the same query returns its rows
in the same order on every run and every supported database.

## ADDED Requirements

### Requirement: A query without an order is ordered by its dimensions
When the outermost query of a request — a single query, the final query of a query list, or a saved
query run by name (refined or not) — declares no `order` (absent or empty), keeps
`distinct_dimension_values` true, and projects at least one dimension or time dimension, its result
SHALL be ordered exactly as if its `order` listed each projected dimension and time-dimension column,
ascending, in result-column order: same rows, same row order, and NULLs placed as an explicit
ascending order places them (last on every database except SQL Server, which keeps its native
order). The order SHALL apply before `limit` and `offset`. A
query that declares a non-empty `order` SHALL be ordered by it alone, with no columns appended.

#### Scenario: Monthly revenue by store comes back sorted
- **WHEN** `{"source_model": "orders", "measures": ["sum(order_total)"], "dimensions": ["stores.name", "month(ordered_at)"]}`
  runs
- **THEN** rows are sorted by store name, then by month, both ascending

#### Scenario: Identical to the explicit spelling
- **WHEN** a query with dimensions `status` and `month(created_at)` and no `order` runs, and the same
  query with `order: [{"column": "status", "direction": "asc"}, {"column": "month(created_at)", "direction": "asc"}]`
  runs
- **THEN** both return the same rows in the same order, and a row whose `status` is NULL comes after
  every non-NULL status

#### Scenario: The default order picks the first page
- **WHEN** a query with dimension `status`, no `order`, `limit: 2` and `offset: 1` runs over statuses
  `a`, `b`, `c`, `d`
- **THEN** it returns `b` and `c`

#### Scenario: An explicit order wins
- **WHEN** a query with dimensions `status` and `region` declares `order: [{"column": "count(*)", "direction": "desc"}]`
- **THEN** rows are ordered by the count descending only

#### Scenario: Only the outermost query is ordered
- **WHEN** a query list's first stage groups by `customer_id` with no `order` and the final stage
  groups that stage by `region`
- **THEN** only the final result is ordered (by `region`); the first stage's rows are not sorted

#### Scenario: Raw rows and measure-only queries are not ordered
- **WHEN** a query sets `distinct_dimension_values: false`, or projects measures and no dimension
- **THEN** no default order is applied

#### Scenario: Computed and joined dimensions sort by their projected values
- **WHEN** a query projects a computed dimension and a joined dimension (`customers.name`) and
  declares no `order`
- **THEN** rows sort by the projected values of those two columns, in that order
