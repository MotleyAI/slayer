## MODIFIED Requirements

### Requirement: A stored join never targets the spine
Saving a model SHALL be refused with a typed error naming the model and the join when a join's `to` equals `time_spine` case-insensitively and no stored model of its datasource is named exactly as written; the error SHALL state that the spine joins each dataset through its time axis, so a declared join to it is never needed. On load, a stored model below schema version 25 SHALL have each such join removed, with a structured warning naming the model and the join, and SHALL be persisted at the current version only by a save or `slayer storage upgrade`.

#### Scenario: A join to the spine is refused in any spelling
- **WHEN** a model `orders` is saved with a join to `time_spine` on `created_at = timestamp`, and again with the join target `TIME_SPINE`
- **THEN** each save fails with the typed error naming `orders` and the join, and nothing is stored

#### Scenario: A stored join to the spine is dropped on load
- **WHEN** a store written before schema version 25 holds a model `ev` without a time axis whose join targets `time_spine`, and a query groups by `time_spine.timestamp` month over Jan–Mar and sums `ev.amount`
- **THEN** `ev` loads without that join, with the warning naming `ev` and the join, the stored document is unchanged, and the query returns the same rows as the same store without that join
