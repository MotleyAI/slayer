## ADDED Requirements

### Requirement: A datasource save waits for its pending granularity rename

A datasource save SHALL be refused while the stored datasource of that name still defines a custom granularity
whose name a library name reserves. The error SHALL name the stored granularity and point to `slayer storage upgrade`.
Nothing SHALL be written. This SHALL hold on both the YAML and the SQLite store, through every save path.

#### Scenario: Saving a loaded datasource whose rename is pending

- **WHEN** a stored datasource defines the granularity `left`, a stored dataset buckets by it, and the datasource
  as loaded (with `left_granularity`) is saved
- **THEN** the save is refused naming `left` and `slayer storage upgrade`, the stored datasource still defines
  `left`, and after reopening the store the dataset still loads and buckets by `left_granularity`

#### Scenario: Saving after the upgrade

- **WHEN** `slayer storage upgrade` has stored the rename and the datasource is then saved
- **THEN** the save succeeds
