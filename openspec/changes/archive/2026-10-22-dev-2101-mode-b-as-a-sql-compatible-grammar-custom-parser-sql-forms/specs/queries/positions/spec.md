## ADDED Requirements

### Requirement: Order items carry an explicit NULL placement
An order item SHALL accept `nulls` set to `first` or `last`. With it, the sorted rows SHALL place NULL values first or last on every Tier-1 dialect, SQL Server included, for every kind of order target — a projected field, a hidden filter- or order-only value, a measure, a transform or an expression. Without it, NULL placement SHALL be unchanged. `nulls` SHALL survive serialisation, saved-query refinement and the stage rewrites that rebuild order items.

#### Scenario: NULLS FIRST on a descending order
- **WHEN** a raw-rows query on `orders` orders by `{"column": "discount", "direction": "desc", "nulls": "first"}`
- **THEN** the rows with a NULL discount come first, then the rest by discount descending, on every Tier-1 dialect

#### Scenario: NULLS LAST on a hidden aggregate
- **WHEN** a query groups by `region` and orders by `{"column": "max(discount)", "direction": "asc", "nulls": "last"}` without projecting `max(discount)`
- **THEN** regions whose `max(discount)` is NULL come last, and the hidden value is not returned

#### Scenario: SQL Server honours the policy over an aliased target
- **WHEN** the same two queries run on SQL Server
- **THEN** they return the same row order as on Postgres

#### Scenario: Refinement keeps the policy
- **WHEN** a saved query ordering with `"nulls": "first"` is run with a refinement adding a filter
- **THEN** the refined result keeps NULLs first

#### Scenario: Invalid policy
- **WHEN** an order item has `"nulls": "middle"`
- **THEN** the query is rejected at construction naming the allowed values
