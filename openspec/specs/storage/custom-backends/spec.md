# storage/custom-backends Specification

## Purpose
Defines the contract a third-party `StorageBackend` subclass relies on, including how subclasses written against the 0.9.x hook contract keep working, with a deprecation warning, until 2.0.

## Requirements

### Requirement: A 0.9.x-style save_datasource override is adapted

A `StorageBackend` subclass that defines `save_datasource` itself, with no genuine `_save_datasource_impl` on any class before `StorageBackend` in its MRO, SHALL be treated as 0.9.x-style. Such a class SHALL instantiate, and every `save_datasource` call on it SHALL validate the datasource's custom granularities before running the class's own `save_datasource` body. This SHALL also hold for a 0.9.x-style class whose parent is itself 0.9.x-style, whether or not its override calls `super().save_datasource()`. A `FutureWarning` naming the class, the `save_datasource` hook, its replacement `_save_datasource_impl` and the removal version 2.0 SHALL be emitted once, when the class is defined. A class with a genuine `_save_datasource_impl` in its MRO before `StorageBackend` (including a subclass of a bundled backend that overrides `save_datasource`) SHALL be left unchanged and emit no warning.

#### Scenario: A 0.9.x-style backend instantiates with a warning

- **WHEN** a `StorageBackend` subclass implementing the 0.9.12 abstract hooks (including `save_datasource`, without `_save_datasource_impl`) is defined and instantiated
- **THEN** one `FutureWarning` naming the class, `save_datasource`, `_save_datasource_impl` and 2.0 is emitted at definition, the class has no abstract methods left, and it instantiates

#### Scenario: A 0.9.x-style save validates granularities, then runs the old body

- **WHEN** `save_datasource` is called on that backend with a datasource whose custom granularities are valid
- **THEN** the granularity validation runs and the backend's own `save_datasource` body persists the datasource

#### Scenario: An invalid granularity is rejected before the old body runs

- **WHEN** `save_datasource` is called on that backend with a datasource whose custom granularities violate the save-time rules
- **THEN** the save raises the validation error and the backend's own `save_datasource` body does not run

#### Scenario: A 0.9.x-style child of a 0.9.x-style backend still validates

- **WHEN** a subclass of that backend overrides `save_datasource` again, once calling `super().save_datasource()` and once not, and an invalid datasource is saved through it
- **THEN** in both variants the save raises the validation error before the child's body persists anything, a valid datasource saves without recursion, and the child's definition emits its own `FutureWarning`

#### Scenario: A new-style backend is untouched

- **WHEN** a `StorageBackend` subclass implementing `_save_datasource_impl` is defined
- **THEN** no `FutureWarning` is emitted and `save_datasource` runs the base template

#### Scenario: A save_datasource override on a bundled-backend subclass is untouched

- **WHEN** a subclass of a bundled backend overrides `save_datasource` and calls `super().save_datasource()`
- **THEN** no `FutureWarning` is emitted and the override runs as written, reaching the bundled backend's write through the base template

### Requirement: _list_memories_rows results are shape-checked

Every `_list_memories_rows` result SHALL be checked before any caller sees it. A tuple of exactly two lists SHALL pass through unchanged. A list SHALL be treated as the 0.9.x shape: it SHALL be returned as `(rows, [])`, and a `FutureWarning` naming the class that defines the override, the `_list_memories_rows` hook, the `(memories, failed_rows)` shape and the removal version 2.0 SHALL be issued through the normal warning filters at most once per defining class: an `always` filter SHALL NOT repeat it, and a call whose warning a filter raises as an exception SHALL NOT count as that one warning. Any other result SHALL raise a `TypeError` naming the defining class and the expected `(memories, failed_rows)` shape. For an adapted 0.9.x-style backend, a memory that fails to load SHALL make the call raise, as in 0.9.x. A 0.9.x-style subclass of a bundled backend that consumes `super()._list_memories_rows()` as a list is not covered: the bundled result reaches it as `(memories, failed_rows)`.

#### Scenario: A list-returning backend reads every memory count correctly

- **WHEN** a backend whose `_list_memories_rows` returns a list holds 0, 1, 2 or 3 memories, and `list_memories` and `load_memories` are called
- **THEN** `list_memories` returns exactly the stored memories and `load_memories` returns them with no failed rows, for every count including 2

#### Scenario: The 0.9.x shape warns once per class

- **WHEN** `_list_memories_rows` of that backend is reached several times, under a warning filter of `always`
- **THEN** exactly one `FutureWarning` naming the class, `_list_memories_rows`, `(memories, failed_rows)` and 2.0 is emitted

#### Scenario: A wrong return shape raises naming the class

- **WHEN** a backend's `_list_memories_rows` returns something that is neither a list nor a tuple of exactly two lists (for example a dict, a generator, a 3-tuple, or a 2-tuple whose items are not both lists)
- **THEN** `list_memories` raises a `TypeError` naming the defining class and the `(memories, failed_rows)` shape

#### Scenario: A new-style backend keeps per-document failure reporting

- **WHEN** a backend whose `_list_memories_rows` returns `(rows, failed_rows)` holds one loadable and one unloadable memory
- **THEN** no `FutureWarning` is emitted, `load_memories` returns the loadable memory with the unloadable one as a failed row, and `list_memories` skips and reports it
