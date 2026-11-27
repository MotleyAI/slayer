## ADDED Requirements

### Requirement: The demo builds on every allowed sqlglot
Building the demo database SHALL succeed on every sqlglot version the package's dependency range allows, so `slayer datasources create demo`, `slayer mcp --demo` and `slayer serve --demo` work on a fresh install. Loading the generated data SHALL create and drop no view.

#### Scenario: Loading the demo creates no view
- **WHEN** the demo's generated data is loaded
- **THEN** the load succeeds and no statement it executes creates or drops a view
