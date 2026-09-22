## Purpose

Governs how rendered SQL reaches the database: the client hands every statement
to the driver verbatim, so no character sequence in model or query SQL is
reinterpreted on the way to the database.

## ADDED Requirements

### Requirement: Rendered SQL executes verbatim
The client SHALL pass every rendered statement to the database driver unchanged
and with no parameter set: a `:word` sequence SHALL never be read as a bind
parameter, and a `%` SHALL never be read as a format directive. This SHALL hold
on every execution path — asynchronous and synchronous query execution and the
column-type probe.

#### Scenario: Regex group inside a string literal
- **WHEN** a query's rendered SQL contains the string literal
  `'(?i)(?:too complicated|too complex)'`
- **THEN** the query executes on both the asynchronous and the synchronous
  path and returns rows carrying that literal unchanged

#### Scenario: Percent sign in a literal
- **WHEN** rendered SQL contains a `%` inside a literal — a `LIKE '%x%'`
  pattern or a `'%Y-%m'` format string — on a driver whose placeholders use `%`
- **THEN** the database receives a single `%` and the query returns the rows
  the pattern selects

#### Scenario: Type probe on the same SQL
- **WHEN** the column-type probe runs over SQL containing the literal `'(?:x)'`
- **THEN** it infers the column types without a bind-parameter error

#### Scenario: Ad-hoc regex column end to end
- **WHEN** a query extends its model with a column whose SQL contains
  `(?i)(?:too complicated|too complex)` and runs through the query engine
- **THEN** the query succeeds and the column's values reflect that expression

### Requirement: One execution door
The client SHALL execute every statement — rendered queries and its own session
statements (timeouts, read-only transactions) alike — through one verbatim
driver-level path; no statement SHALL pass through a text-compilation layer
that parses placeholders.

#### Scenario: Module guard
- **WHEN** the client module's source is inspected
- **THEN** it constructs no text clause and issues no driver-level execute
  outside the single verbatim helper pair
