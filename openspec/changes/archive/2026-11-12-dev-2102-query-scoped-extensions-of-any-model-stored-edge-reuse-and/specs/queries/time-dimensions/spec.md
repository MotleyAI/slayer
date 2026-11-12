## MODIFIED Requirements

### Requirement: Sibling-stage stand-ins carry the bucket

When a stage of a multi-stage query binds against a sibling stage through a model-shaped scope — a stage reading a named sibling that it extends, or a join or dotted reference to a sibling — the sibling's bucketed columns SHALL carry their granularity into that scope, so the re-bucketing rule applies exactly as it does for the flat stage-schema binding.

#### Scenario: ModelExtension over a bucketed sibling rejects finer

- **WHEN** a named stage buckets `created_at` at `month`, and a later stage whose source is that sibling, which it also extends, declares a time dimension on `created_at` at `day`
- **THEN** planning fails with the typed re-bucketing error, and the same shape at `year` executes with correct values

#### Scenario: Stage join to a bucketed sibling rejects finer

- **WHEN** a named stage buckets `created_at` at `month`, and a later stage joins it and declares a time dimension on the dotted sibling column at `day`
- **THEN** planning fails with the typed re-bucketing error, and the same shape at `month` binds
