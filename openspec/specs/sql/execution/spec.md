# sql/execution Specification

## Purpose
Governs how rendered SQL reaches the database: the client hands every statement
to the driver verbatim, so no character sequence in model or query SQL is
reinterpreted on the way to the database.

## Requirements

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

### Requirement: Engines have one disposing owner
Every database engine SHALL be disposed by exactly one owner. The shared engine factory SHALL
dispose every engine it evicts or drops — resetting its cache SHALL always dispose the engines it
held. A query engine SHALL dispose the private in-memory engines of its clients when it is closed;
closing SHALL be idempotent and SHALL leave the query engine usable. The per-call teardown of
loop-bound asynchronous engines SHALL NOT dispose synchronous engines. A query engine that becomes
unreachable without being closed SHALL still have its private in-memory engines disposed when it
is collected.

#### Scenario: Cache reset disposes every held engine
- **WHEN** the engine factory cache is reset while it holds engines
- **THEN** each held engine is disposed exactly once and the cache is empty

#### Scenario: Closing a query engine disposes only its private engines
- **WHEN** a query engine that executed against an in-memory SQLite datasource and a file-backed
  datasource is closed
- **THEN** the in-memory client's private engine is disposed, the factory-owned file engine is
  not, and a second close is a no-op

#### Scenario: A closed query engine is reusable
- **WHEN** a closed query engine executes a query again
- **THEN** the query succeeds, and an in-memory SQLite datasource starts from an empty database

#### Scenario: Per-call async teardown spares synchronous engines
- **WHEN** a query engine's per-call teardown runs after a synchronous execution
- **THEN** the client's synchronous engine is the same object afterwards and its data is intact

#### Scenario: An unreferenced query engine leaks nothing
- **WHEN** a query engine holding a private in-memory engine becomes unreachable without `close()`
  and the garbage collector runs on CPython 3.13 or newer
- **THEN** no `unclosed database` ResourceWarning is emitted

### Requirement: In-memory SQLite is one database per owner and per datasource
Within one owner — a query engine's client, or the shared engine factory — statements over an
in-memory SQLite datasource SHALL see one database regardless of the thread that executes them,
and closing that database from any thread SHALL succeed. In the shared engine factory, two
in-memory SQLite datasources with different names SHALL never share a database.

#### Scenario: Cross-thread coherence through the factory
- **WHEN** a table is created and populated through the factory's engine for an in-memory
  datasource on one thread and a statement over that datasource then runs on a worker thread
- **THEN** the worker-thread statement sees the table and its rows

#### Scenario: Per-datasource isolation in the factory
- **WHEN** two in-memory SQLite datasources with different names are used through the factory in
  one process
- **THEN** a table created through one is not visible through the other

#### Scenario: Reset closes an in-memory engine used from several threads
- **WHEN** an in-memory factory engine was used from more than one thread and the cache is reset
- **THEN** no connection-close error is logged and no `unclosed database` warning is emitted
