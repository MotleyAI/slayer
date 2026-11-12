## MODIFIED Requirements

### Requirement: One-sided time filters return correct results end-to-end

A facade-translated query with a one-sided time filter SHALL return exactly the rows
satisfying the filter when executed.

#### Scenario: Open-ended date filter returns the matching rows

- **WHEN** a table holds rows on both sides of `2024-01-01` and the facade-translated
  query `SELECT date_trunc('month', ordered_at), revenue_sum FROM orders WHERE ordered_at >= '2024-01-01'`
  is executed
- **THEN** the result contains exactly the rows with `ordered_at >= '2024-01-01'` and is
  non-empty
