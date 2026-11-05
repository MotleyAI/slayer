## 1. Tests (pr-tests stage)

- [x] 1.1 Create `tests/test_sql_client_cancellation.py` (unit; in-process SQLite, no `integration` marker); verify it is collected by `poetry run pytest -m "not integration" tests/test_sql_client_cancellation.py --collect-only`
- [x] 1.2 Metadata-read scenario: SQLite file datasource (factory engine → worker path), a second `sqlite3` connection holds `BEGIN EXCLUSIVE`; `asyncio.wait_for(client.list_objects(None), 0.2)` raises `TimeoutError` in well under the 5 s busy timeout (bound ≈2 s) while a ticker task advances; release the lock in `finally`. Verify it FAILS on the current code (the probe measured 5.01 s)
- [x] 1.3 Statement scenario: same set-up with `client.execute("SELECT * FROM t")`; verify it fails on the current code
- [x] 1.4 Cancelled-call scenario with a deterministic barrier (a blocking callable that sets an "entered" event, waits on a release event, and records `threading.current_thread()`), driven through `_run_sync_in_thread`: `task.cancel()` while running → `CancelledError` at once; cancel before the task starts → `CancelledError` and the callable never runs
- [x] 1.5 Dropped-outcome scenario: after cancellation the released callable returns, and separately raises; a custom loop exception handler records nothing; the captured worker thread is joined and not alive. Repeat with the loop closed (`asyncio.run` finished) before release
- [x] 1.6 Close-after-cancel scenario: lock-blocked `list_objects` cancelled, then `client.close()` returns promptly without raising; release the lock and the abandoned worker ends
- [x] 1.7 Retry scenario via `_execute_with_retry_threaded(run=...)` with a stub `run`: cancel during an attempt and during the backoff sleep (first attempt raises a retryable `OperationalError`); the caller gets `CancelledError` and the attempt count does not grow afterwards
- [x] 1.8 Uncancelled scenario: barrier callable returns a value / raises an error through `_run_sync_in_thread` → same value / same error, and the captured worker thread is not alive when the await returns; plus a real `client.ping()` on SQLite returns `None`

## 2. Implementation (pr-implement stage)

- [x] 2.1 Rewrite `_run_sync_in_thread` in `slayer/sql/client.py`: explicit `ThreadPoolExecutor(max_workers=1, thread_name_prefix="slayer-blocking")`; await `run_in_executor`; in `finally`, `shutdown(wait=False, cancel_futures=True)` if the await was cancelled, else `shutdown(wait=True)`; keep the concise docstring (scoped-executor reason + cancellation never waits). Verify all of section 1 passes
- [x] 2.2 Add one sentence to `docs/configuration/sql-clients.md` "Lifecycle and concurrency": cancelling a `SlayerSQLClient` call (e.g. with `asyncio.wait_for`) returns at once, except on a client built with `sa_engine=`, and a blocking call already running finishes in its worker thread on its own
- [x] 2.3 Run `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` and `poetry run la-arch-check`; all green
