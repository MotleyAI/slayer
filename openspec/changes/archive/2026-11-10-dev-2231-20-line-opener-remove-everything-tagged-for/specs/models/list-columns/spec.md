## MODIFIED Requirements

### Requirement: Agents see list columns and their operations

`inspect` of a model or column, `search` column hits and `models_summary` SHALL render a list column's type as `ARRAY<element type>`, its element samples when profiled, and the operations it supports: `contains`, `contains_all` / `contains_any`, element access through `<column>.value`, and grouping and comparison as a set. The JSON rendering SHALL carry `element_type`.

#### Scenario: Rendered list column
- **WHEN** `inspect` of the model `contractors` renders it at `compact=False` with a profiled `skills` column
- **THEN** the rendering shows `ARRAY<TEXT>`, the sampled elements, names `contains`, `contains_all` and `skills.value`, and says the list groups and compares as a set
