---
created_at: '2026-10-05T13:43:16.057898Z'
entities:
- shop.orders
query:
  dimensions:
  - label: null
    model: null
    name: status
  distinct_dimension_values: true
  filters: null
  limit: null
  main_time_dimension: null
  measures:
  - description: null
    formula: total:sum
    label: null
    meta: null
    name: total_sum
    type: null
  name: null
  offset: null
  order: null
  source_model: status_rank
  time_dimensions: null
  to_many_handling: broadcast
  variables: null
  version: 4
  whole_periods_only: false
version: 2
---
Top statuses by revenue.