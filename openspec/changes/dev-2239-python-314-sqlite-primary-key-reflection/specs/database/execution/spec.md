## ADDED Requirements

### Requirement: A database connection serves one thread at a time
Every database connection SLayer opens SHALL be used by one thread at a time. Concurrent calls over an
in-memory SQLite datasource SHALL wait for its one connection rather than share it, and SHALL still see
one database; a nested request for that connection from the thread already holding it SHALL fail at
once with an error naming the nesting rather than wait. Every engine SLayer builds, for every
datasource type and for its async engines, SHALL hand each connection to one caller at a time.

#### Scenario: Concurrent calls over an in-memory SQLite datasource
- **WHEN** many statements over one in-memory SQLite datasource are awaited concurrently, each slow
  enough that they would overlap
- **THEN** no two threads use its connection at once, and each statement returns its own rows

#### Scenario: Concurrent calls over a file-backed SQLite datasource
- **WHEN** many statements over one file-backed SQLite datasource are awaited concurrently, each slow
  enough that they would overlap
- **THEN** no connection is used by two threads at once, and each statement returns its own rows

#### Scenario: A nested in-memory checkout fails at once
- **WHEN** a thread holding the in-memory SQLite datasource's connection asks its engine for a
  connection again
- **THEN** the request fails at once with an error naming the nested use

#### Scenario: Every engine hands a connection to one caller at a time
- **WHEN** SLayer builds the synchronous engine, and the async engine where the type has one, for each
  Tier 1 database it supports
- **THEN** each engine's pool hands each connection to one caller at a time
