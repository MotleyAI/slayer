## Context

The branch's content equals `release-1.6.x` (1.6.1); its merge-base with `origin/main` is `v1.6.0`,
and `origin/main` equals `v1.7.0`. A dry-run merge (`git merge-tree`) conflicts in four files; the
`_run_sync_in_thread` fix auto-merges. The 1.6.1 design lives in
`openspec/changes/archive/2026-11-05-dev-2206-cancelling-a-sql-clients-blocking-call-returns-at-once/`.

Thread-hop re-audit on `main`:
- `_run_sync_in_thread` is still the only scoped-executor hop. `main`'s new `_blocking` call sites
  (`get_result_column_types_sync`, `_server_profile_sync`, `_metadata` → `_read_on_connection`) and the
  refused-loop fallback (`_async_engine_here()` is `None` → `_blocking`) all route through it.
- `asyncio.to_thread` sites (`storage/*`, `search/graph.py`, `cli.py`) use the default executor and
  never join on cancellation; `async_utils.py` is unchanged since `v1.6.0`.
- SQLite has no async driver on `main`, so the SQLite-lock cancellation tests still take the worker
  path; every internal they hook (`_list_objects_sync`, `_exec_verbatim`, `_execute_sql_sync`,
  `_execute_with_retry_threaded`, `engine_factory.invalidate_engine`) exists on `main`.

Architecture: element `python.sql`; system §3.7 async-first and sql §3.14 one engine owner apply, as
in the 1.6.1 design. No arc42/.c4 edit.

## Goals / Non-Goals

**Goals:**
- `main` gets the fix, its tests and its spec requirement through one merge commit.

**Non-Goals:**
- New behaviour or new tests beyond what 1.6.1 shipped.
- A version bump (`main` releases later as 1.8.0).

## Decisions

1. **Merge `origin/main` into this branch**, message first line `Merge branch 'origin/main' into
   egor/dev-2206-cancelling-a-sql-clients-blocking-call-returns-at-once`; the PR into `main` is then
   merged with a merge commit, keeping `main`'s first-parent history. (user)
2. **`slayer/sql/client.py`: take `main`'s side of the conflict hunk.** `main` deleted
   `_map_type_code` (typed `ResultColumnType` probing replaced it), so 1.6.1's Sonar split of it
   (`_map_type_name` / `_map_python_type` / `_map_int_type_code`) is dropped; the auto-merged
   `_run_sync_in_thread` rewrite stays. (default)
3. **`tests/test_sql_client.py`: take `main`'s side**; 1.6.1's `test_other_python_type_is_string`
   targets the deleted function. (default)
4. **`openspec/specs/sql/execution/spec.md`: union** — `main`'s appended requirements first, then
   "Cancelling a client's blocking database call returns at once", unchanged. (default)
5. **`pyproject.toml`: keep `main`'s `1.7.0`.** Options weighed: bump to 1.7.1 here (1.6.1 precedent)
   vs keep. Kept: `main` already holds other work and will release as 1.8.0. (user)
6. **Doc sentence reworded for `main`'s driver model.** In `docs/configuration/sql-clients.md`
   "Lifecycle and concurrency", the sentence becomes: "Cancelling a `SlayerSQLClient` call that runs
   in a worker thread (every call over a driver without async support, or over psycopg on Windows'
   default event loop), e.g. with `asyncio.wait_for`, returns at once unless the client was built with
   `sa_engine=`; the blocking call itself finishes in its worker thread on its own." The first draft
   "(every call over a driver the client runs synchronously)" also read as covering `sa_engine=` and
   `conn.run_sync` callbacks. (codex, user-approved wording)
7. **OpenSpec: a `skip_specs` change for the forward-merge, deleted (not archived) at `pr-review`.**
   The requirement is already in the corpus; the 1.6.1 change is already archived and must not be
   archived again. (default)
8. **No new tests.** `tests/test_sql_client_cancellation.py` is the suite; it already exists, so
   `pr-tests` has nothing to write. (default)
9. Merge direction "branch from `origin/main` and merge the release branch into it" — rejected
   (codex): the user asked to merge `origin/main` into this branch, and a merge-commit PR keeps
   `main`'s first-parent history.
10. A cancellation test for the Windows/proactor psycopg fallback — rejected (codex): `main`'s
    `test_refused_loop_runs_the_same_driver_synchronously` proves that path runs in a worker
    (`_blocking` → `_run_sync_in_thread`), whose cancellation the existing suite covers.
11. An `aclose()`-after-cancellation test — rejected (codex): not new on `main` (`v1.6.0`'s `aclose`
    already called `engine_factory.release_idle`), and `release_idle` only drains idle connections,
    never waiting on a checked-out one.

## Risks / Trade-offs

- `main`-side tests could pin shapes the fix touches (executor use, thread counts) → the full
  non-integration suite runs on the merge commit, and any failure is fixed there.
