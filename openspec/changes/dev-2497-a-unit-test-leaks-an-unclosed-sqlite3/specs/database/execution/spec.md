## MODIFIED Requirements

### Requirement: Engines have one disposing owner
Every database engine SHALL be disposed by exactly one owner. The shared engine factory SHALL
dispose every engine it evicts or drops — resetting its cache SHALL always dispose the engines it
held. A query engine SHALL dispose the private in-memory engines of its clients when it is closed;
closing SHALL be idempotent and SHALL leave the query engine usable. The per-call teardown of
loop-bound asynchronous engines SHALL NOT dispose synchronous engines. A query engine that becomes
unreachable without being closed SHALL still have its private in-memory engines disposed when it
is collected.

Disposing a synchronous engine SHALL close every connection that engine opened: an idle
connection at once, a connection in use when it is returned, and a connection the engine opens
after its disposal when that connection is returned. A connection in use SHALL keep working
until it is returned; disposal SHALL neither wait for it nor interrupt it. An engine used after
its disposal SHALL still execute statements.

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

#### Scenario: A connection in use across a cache reset is closed when returned
- **WHEN** a connection of a factory engine is in use, the factory cache is reset, the connection
  then executes a statement and is returned
- **THEN** the statement succeeds and the database connection is closed once it is returned

#### Scenario: A connection in use across an invalidation is closed when returned
- **WHEN** a connection of a factory engine is in use, that datasource's engine is invalidated,
  and the connection is then returned
- **THEN** the database connection is closed once it is returned

#### Scenario: A connection in use across an eviction is closed when returned
- **WHEN** a connection of a factory engine is in use, the engine is evicted because the cache
  is over its limit, and the connection is then returned
- **THEN** the database connection is closed once it is returned

#### Scenario: A private in-memory engine's connection in use across close is closed when returned
- **WHEN** a connection of a client's private in-memory engine is in use, the client is closed,
  and the connection is then returned
- **THEN** the database connection is closed once it is returned

#### Scenario: A blocking call abandoned by cancellation leaves no open connection
- **WHEN** a blocking statement is cancelled while its worker still holds a connection, the
  datasource's engine is invalidated before the worker finishes, and the worker then finishes
- **THEN** no connection of that engine is open

#### Scenario: An engine used after its disposal leaves no open connection
- **WHEN** a client that obtained its engine from the factory executes a statement after the
  factory cache was reset
- **THEN** the statement succeeds and no connection of that engine is open afterwards

#### Scenario: A server session is closed on a late return
- **WHEN** a connection of a synchronous Postgres engine is in use, the engine is disposed, and
  the connection is then returned
- **THEN** the server has no session left from that engine
