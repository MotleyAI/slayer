## Context

See proposal.md — Why. Facts the design rests on, all probed on CPython 3.14.10 with
the locked SQLAlchemy:

- **`Engine.dispose()` does not retire an engine.** It closes the idle connections of
  the current pool and swaps in a fresh pool. A connection checked out at that moment
  is returned to the old pool and stays open until collected (probe: one
  `unclosed database` warning; on a sync psycopg2 Postgres engine, one server session
  left in `pg_stat_activity`). A connection opened on the engine afterwards sits in the
  new pool, which nobody disposes (probe: a second warning).
- **A pool `checkin` listener that calls `connection_record.invalidate()` closes the
  connection being returned.** Registered on the **engine** before `dispose()`, it
  applies both to the pool being retired and to every pool the engine creates later
  (probe: late return closed, connection opened after disposal closed on return, the
  engine still answers `SELECT 1`, no warning at collection).
- **Async engines need nothing.** An asyncpg engine closes a late return by itself
  (probe: zero server sessions left, with and without the listener). There is no async
  SQLite driver in the project.
- **Who reaches the leak in production.** `client._run_sync_in_thread` abandons its
  worker on cancellation; `invalidate_engine`, cache eviction and `reset_cache` dispose
  without waiting. A client also keeps its `_sync_engine` reference after the factory
  disposed that engine.
- **The sqlite doors.** SQLAlchemy binds `sqlite3.dbapi2.connect`; `sqlite3.connect`
  is the same callable under a second name. The PR #674 probe wrapped only
  `sqlite3.connect`, so it saw no SQLAlchemy connection.
- **Warning mechanics.** A `sqlite3.Connection` subclass that defines `__repr__` and no
  `__del__` keeps CPython's `unclosed database in %R` warning, with the subclass repr in
  the text, and a weak-reference callback on it fires at death. A subclass that defines
  `__del__` suppresses the built-in warning, so the harness must not define one.
- **Reproduction status.** Local 3.14 runs with a probe on both doors: shard 4 twice
  and shards 1–3 once with 8 workers, no leak. The only `dispose()` with a connection
  checked out was `test_checked_out_connection_finishes_after_reset`, which cleans up.
  `tests/test_demo_stamp.py` (two of three sightings) opens only DuckDB itself.

## Goals / Non-Goals

**Goals:**

- No connection of a disposed synchronous engine stays open, on any dialect.
- A leaked `sqlite3` connection fails the unit run on every Python version and the
  failure text alone names the opening test and code.

**Non-Goals:**

- Proving which test leaked in CI. If the two closed paths were not the cause, the
  next failure names its source.
- Async engines, `release_idle` (the pool stays owned), and the `dispose(close=False)`
  paths (fork child, `client._abandon`).
- Any CI workflow, documentation, arc42 or model change.

## Decisions

### Production

- **Retire on disposal.** The factory's disposal routine registers one engine-level
  `checkin` listener that invalidates the returned connection record, then calls
  `engine.dispose()`. Registration is idempotent per engine. Both steps sit inside the
  existing log-and-continue boundary, so a failure of either is logged and never raised.
- **One routine.** `engine_factory` owns it; `_dispose_quietly` (eviction,
  `invalidate_engine`, `reset_cache`, lost build race) and the client's private
  in-memory engine disposal (`close()` and its `weakref.finalize` backstop) both go
  through it. `client._dispose_engine_quietly` stops calling `engine.dispose()` itself.
- **Nothing waits, nothing is interrupted.** The connection is closed on the thread
  that returns it, which for SQLite is the thread that used it.

### Harness (`tests/_sqlite_connection_law.py`, a pytest plugin listed in `tests/conftest.py` `pytest_plugins`)

- **One shared wrapper on both names.** Capture both originals, install the same
  wrapper on `sqlite3.connect` and `sqlite3.dbapi2.connect`, restore each exact
  original at pytest cleanup; installing twice is a no-op. The wrapper only defaults
  `factory=` to the traced subclass, so a caller-supplied factory is left alone
  (`tests/test_law_one_thread_per_connection.py` passes its own).
- **Record per connection:** opening test node id (or `<outside a test>`), whether
  that test is under `tests/integration/`, thread name, database argument, and the
  opening stack as `(file, line, function)` frames taken without reading source lines.
- **`__repr__` names the opener** (test id and innermost frame inside the repo); no
  `__del__`. `close()` is overridden to mark the record closed.
- **Judgment.** A weak-reference callback moves an unclosed record to a pending list.
  The `pytest_runtest_teardown` wrapper that follows fails with `pytest.fail(...,
  pytrace=False)`, or adds the text as a note when the teardown itself raised (the
  shape of `tests/_async_engine_release_law.py`). A session-end judgment fails the
  session for records still pending after the last teardown.
- **Failure text per connection:** database, opening test and thread, full opening
  stack, and the test and thread running when it died.
- **Report-only follows the opener.** A record opened by an integration item is
  emitted as a warning, never a failure, whichever test is running when it dies; a
  record opened by a unit item fails even if it dies during an integration test.
- **The 3.13+ gate is untouched.** Both `filterwarnings` entries in `pyproject.toml`
  and `test_gate_config_is_pinned` stay as they are; the built-in warning now carries
  the opener through the repr.

### Decisions log

1. **Scope: fix the proven paths and add attribution, rather than hunt further.**
   Options: fix + attribution / attribution only / keep hunting before planning. Taken:
   fix + attribution, with CI output sufficient to name the source next time. `(user)`
2. **A leak fails on every Python version.** Options: every version / 3.13+ only with
   a report in the log below that. Taken: every version; integration stays
   report-only. `(user)`
3. **Align `test_checked_out_connection_finishes_after_reset`:** return with a plain
   `conn.close()` (no `invalidate()`), keep the `SELECT 1` assertion after the reset,
   assert the sqlite connection is closed after the return, rewrite the docstring. `(user)`
4. **A late return is closed on return.** Blocking the disposal or interrupting the
   statement both contradict "cancellation never waits" and the pinned "a checked-out
   connection keeps working". `(default)`
5. **One disposal routine in `engine_factory`, used by the client too.** Two hand-kept
   copies is the shape that let this through. `(default)`
6. **Every dialect's sync engines; async untouched.** Probed on Postgres. `(default)`
7. **No arc42 or model edit.** `python.database` §3.3 and `python.storage` §3.1 already
   require this; the behaviour goes in the spec. `(default)`
8. **No CI workflow change.** The failure text in the job log is the report. `(default)`
9. **Fail the following teardown, not the opener.** The opener has already been
   reported when the connection dies, and a forced collection after each of ~67k tests
   costs on the order of half an hour of CPU per run. `(default)`
10. **A disposed engine is retired: it pools nothing afterwards.** Marking only the
    pool current at disposal leaves "closes every connection it opened" false, because
    the engine keeps working on a fresh pool. One engine-level listener covers the late
    return and every later connection. `(codex)`
11. **Session-end judgment** for connections that die after the last teardown. `(codex)`
12. **Failure versus warning follows the opening test**, with self-tests for both
    cross-boundary directions. `(codex)`
13. **Listener registration stays inside the quiet error boundary**, with a test for a
    registration failure. `(codex)`
14. **One shared wrapper on both door names, exact restore, idempotent install**, with
    a self-test. `(codex)`

## Risks / Trade-offs

- [A holder of a disposed engine now pays a connect per use] → It is correct and leak
  free; before, it silently pooled connections nobody closed. In-memory SQLite on a
  disposed engine starts empty on each use, as it already did after a disposal.
- [3.11 jobs can now go red on the intermittent leak] → Chosen deliberately
  (decision 2): each failure names its source.
- [Untraced connections: a caller-supplied `factory`, or one opened before the plugin
  loads] → The 3.13+ gate still fails on them, without an opener.
- [An existing test may leak in-process on purpose] → None found by reading; if the
  harness flags one, stop and ask before changing it.
- [The two closed paths may not be the CI cause] → Accepted; see Non-Goals.
