## ADDED Requirements

### Requirement: Functional granularity order key

An order entry `gran(col)` SHALL sort by the bucketed value of the query's time dimension on `col` at granularity `gran` when that time dimension is projected. When no time dimension on `col` is projected, or the projected granularity differs, the query SHALL fail with an error naming the remedy. A granularity call used inside a filter or another expression is a row-level expression (see `queries/time-points`).

#### Scenario: order by projected bucket

- WHEN a query projects `month(created_at)` (functional or explicit) and orders by `"month(created_at)"`
- THEN rows sort by the month bucket, identically to ordering by the time dimension's column name

#### Scenario: order without matching time dimension

- WHEN a query orders by `"month(created_at)"` but projects no time dimension on `created_at`, or projects it at a different granularity
- THEN the query fails with an error naming the missing or mismatched time dimension and the remedy

#### Scenario: granularity call in a filter is recognised

- WHEN a query filter contains `month(created_at) >= '2024-01-01'`
- THEN the query executes and restricts rows to `created_at >= 2024-01-01`

## REMOVED Requirements

### Requirement: Functional granularity form as an order key

**Reason**: Its final sentence and scenario pinned granularity calls in filters as unrecognised; they are now row-level expressions.
**Migration**: The order-key behaviour is unchanged under "Functional granularity order key"; granularity calls in filters follow `queries/time-points`.
