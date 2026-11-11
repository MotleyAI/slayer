---
created_at: '2026-11-10T15:15:16.117824Z'
entities:
- shop.contractors
query:
  dimensions:
  - label: null
    model: null
    name: name
  distinct_dimension_values: true
  filters: null
  limit: null
  main_time_dimension: null
  measures:
  - description: null
    formula: collect(assignments.id)
    label: null
    meta: null
    name: assignment_ids
    type: null
  name: null
  offset: null
  order: null
  source_model: contractors
  time_dimensions: null
  to_many_handling: broadcast
  variables: null
  version: 6
  whole_periods_only: false
version: 4
---
Assignment ids per contractor.