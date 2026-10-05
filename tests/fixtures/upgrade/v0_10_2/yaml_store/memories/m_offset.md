---
created_at: '2026-10-05T13:43:16.062262Z'
entities:
- shop.orders
query:
  dimensions: null
  distinct_dimension_values: true
  filters: null
  limit: null
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
  order: null
  source_model: orders
  time_dimensions:
  - date_range:
    - '2024-01-01T00:00:00+02:00'
    - '2024-06-30T00:00:00+02:00'
    dimension:
      label: null
      model: null
      name: ordered_at
    granularity: month
    label: null
  to_many_handling: broadcast
  variables: null
  version: 4
  whole_periods_only: false
version: 2
---
Monthly revenue for the first half.