## MODIFIED Requirements

### Requirement: Moved module paths fail with their new location

Importing a module path that SLayer 2.0 moved SHALL raise an `ImportError` whose message names the old path, the new path and SLayer 2.0. Where the old path is now a package, importing a public name of the moved module through it SHALL raise the same kind of `ImportError`, naming the name's new module. Importing a new path, or any name a package at an old path still provides, SHALL succeed and SHALL NOT warn.

The four old paths that `compatibility/integrator-1x-calls` keeps are the exception: `slayer.core.formula`, `slayer.engine.syntax`, `slayer.engine.schema_drift` and `slayer.sql.column_expansion`.
- Importing such a module SHALL succeed without a warning.
- Accessing one of the names that capability lists SHALL work with its deprecation warning.
- Importing any other name from it SHALL raise the `ImportError` naming its new location.

#### Scenario: An old module path names its new location

- **WHEN** `slayer.sql.client` is imported
- **THEN** an `ImportError` is raised whose message names `slayer.sql.client`, `slayer.database.client` and SLayer 2.0

#### Scenario: An old name through a path that is now a package

- **WHEN** `infer_population` is imported from `slayer.engine.population`
- **THEN** an `ImportError` is raised whose message names `slayer.engine.population.inference`

#### Scenario: New paths are unaffected

- **WHEN** `SqlClient` is imported from `slayer.database.client`, and the `inference` submodule from `slayer.engine.population`
- **THEN** both imports succeed and no warning is emitted

#### Scenario: Every moved path is covered

- **WHEN** every module path moved in 2.0 is imported, and every public name of a module whose old path is now a package is imported through that path
- **THEN** each raises an `ImportError` naming its new location, except the four kept paths, which import

#### Scenario: A kept path serves only its listed names

- **WHEN** `slayer.engine.schema_drift` is imported, `diff_sql_table_model` is imported from it, and then `RetypeFinding` is imported from it
- **THEN** the module import emits no warning, `diff_sql_table_model` works with exactly one deprecation warning naming `slayer.live_schema.schema_drift.diff_sql_table_dataset`, and the last import raises an `ImportError` naming `slayer.live_schema.schema_drift`
