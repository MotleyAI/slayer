## REMOVED Requirements

### Requirement: A 0.9.x-style save_datasource override is adapted
**Reason**: Scheduled for removal in SLayer 2.0; the 0.9.x hook contract is no longer adapted.
**Migration**: Implement `_save_datasource_impl` (the durable write) instead of overriding `save_datasource`.

### Requirement: _list_memories_rows results are shape-checked
**Reason**: Scheduled for removal in SLayer 2.0; the 0.9.x plain-list shape is no longer accepted, so no result needs adapting.
**Migration**: Return `(memories, failed_rows)` from `_list_memories_rows`.

## ADDED Requirements

### Requirement: A backend persists datasources through its write hook

`save_datasource` SHALL validate the datasource's custom granularities and then call the backend's `_save_datasource_impl`. A `StorageBackend` subclass SHALL be instantiable only when a class before `StorageBackend` in its MRO implements `_save_datasource_impl`. A `save_datasource` override SHALL run as written and SHALL NOT be adapted, and defining a backend SHALL emit no `FutureWarning`.

#### Scenario: A new-style backend is untouched
- **WHEN** a `StorageBackend` subclass implementing `_save_datasource_impl` is defined
- **THEN** no `FutureWarning` is emitted and `save_datasource` runs the base template

#### Scenario: A save_datasource override on a bundled-backend subclass is untouched
- **WHEN** a subclass of a bundled backend overrides `save_datasource` and calls `super().save_datasource()`
- **THEN** no `FutureWarning` is emitted and the override runs as written, reaching the bundled backend's write through the base template

#### Scenario: A 0.9.x-style backend does not instantiate
- **WHEN** a `StorageBackend` subclass implements the 0.9.12 hooks, including `save_datasource`, without `_save_datasource_impl`, and is instantiated
- **THEN** instantiation raises a `TypeError` naming the abstract `_save_datasource_impl`

### Requirement: _list_memories_rows returns memories and failed rows

A backend's `_list_memories_rows` SHALL return `(memories, failed_rows)`; `load_memories` SHALL return that pair, and `list_memories` SHALL return the memories and skip and report the failed rows.

#### Scenario: A new-style backend keeps per-document failure reporting
- **WHEN** a backend whose `_list_memories_rows` returns `(rows, failed_rows)` holds one loadable and one unloadable memory
- **THEN** no `FutureWarning` is emitted, `load_memories` returns the loadable memory with the unloadable one as a failed row, and `list_memories` skips and reports it
