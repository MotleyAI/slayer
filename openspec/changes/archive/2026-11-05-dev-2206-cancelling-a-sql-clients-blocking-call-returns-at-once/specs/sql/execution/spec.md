## ADDED Requirements

### Requirement: Cancelling a client's blocking database call returns at once

When the default SQL client runs a blocking database call (a statement, a metadata read or a ping)
over a datasource's own connection settings, cancelling the task that awaits it SHALL raise the
cancellation in that task at once, without waiting for the call to finish, and the event loop SHALL
keep running meanwhile. The abandoned call SHALL finish on its own, and its later result or error
SHALL NOT reach the caller or the event loop's exception handler, also when the loop has closed
before the call finishes. Cancelling a statement SHALL start no further retry attempt. An
uncancelled call SHALL return its own result or raise its own error, and SHALL leave no worker
thread running.

#### Scenario: A timeout bounds a blocking metadata read
- **WHEN** a client lists the objects of a SQLite datasource whose file another connection holds
  under an exclusive lock, bounded by `asyncio.wait_for` with a 0.2 s timeout
- **THEN** the caller gets `TimeoutError` long before the database's own busy timeout, and a
  concurrent task keeps advancing while the call is pending

#### Scenario: A timeout bounds a blocking statement
- **WHEN** a client executes a statement on a SQLite datasource whose file another connection holds
  under an exclusive lock, bounded by `asyncio.wait_for` with a 0.2 s timeout
- **THEN** the caller gets `TimeoutError` long before the database's own busy timeout, and a
  concurrent task keeps advancing while the call is pending

#### Scenario: A cancelled call raises the cancellation
- **WHEN** the task awaiting a blocking call is cancelled, either while the call runs or before it
  has started
- **THEN** the task raises `CancelledError` at once, never the call's later result or error, and a
  call cancelled before it started never runs

#### Scenario: The abandoned call's outcome is dropped
- **WHEN** a cancelled call later fails or returns, while its event loop still runs or after the
  loop has closed
- **THEN** nothing is reported to the loop's exception handler, and the call's worker thread ends

#### Scenario: Closing the client right after a cancellation
- **WHEN** a client is closed immediately after one of its blocking calls was cancelled, while that
  call still blocks
- **THEN** closing returns without waiting for the call or raising, and the abandoned call ends on
  its own

#### Scenario: Cancelling a retried statement starts no further attempt
- **WHEN** a statement is cancelled while an attempt runs or while it waits to retry
- **THEN** the caller gets the cancellation and no further attempt starts

#### Scenario: An uncancelled call is unchanged
- **WHEN** a blocking call runs to completion without cancellation, returning a result or raising
  an error
- **THEN** the caller gets that result or that same error, and the call's worker thread has ended
  when the call returns
