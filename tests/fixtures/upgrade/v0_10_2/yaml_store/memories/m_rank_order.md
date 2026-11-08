---
created_at: '2026-10-05T13:43:16.059617Z'
entities:
- shop.orders
query:
  dimensions:
  - label: null
    model: null
    name: status
  distinct_dimension_values: true
  filters: null
  limit: 2
  main_time_dimension: null
  measures:
  - description: null
    formula: amount:sum
    label: null
    meta: null
    name: rev
    type: null
  name: null
  offset: null
  order:
  - column:
      label: null
      model: null
      name: _funcstyle_pending
    direction: asc
    raw_formula: rank(amount:sum)
  source_model: orders
  time_dimensions: null
  to_many_handling: broadcast
  variables: null
  version: 4
  whole_periods_only: false
version: 2
---
Statuses ranked by revenue.