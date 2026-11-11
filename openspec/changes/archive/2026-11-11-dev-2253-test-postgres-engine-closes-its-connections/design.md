## Context

Motivation: proposal.md — Why. Diagnosis, reproduced on Python 3.14.10 (2026-11-11):

- The CI failure (run 42995942850 attempt 1, unit-tests (3.14, 3), gw3) finalized a
  `psycopg.AsyncConnection … database=tests` — pytest-postgresql's client-fixture database, not the
  `test_<uuid>` database `filtered_engine("postgres", …)` creates: an earlier test on the worker leaked it.
- Pairwise repro (`<module>` then the flaky test, one process, `-p no:randomly -p no:xdist`): the flaky test
  fails after `test_cast_postgres`, `test_dev2177_postgres`, `test_dev2186_postgres`,
  `test_sql_mode_retype_postgres`; passes after the 13 other pytest-postgresql modules tried.
- Parallel repro: the 29 pytest-postgresql unit modules with `-n 6 --dist loadscope -p no:randomly` →
  exactly one failure, the flaky test.
- Census (scratch plugin; state-based, so it under-counts — see Decision 3): unit leaks only in those 4
  modules (their fixtures return a `SlayerQueryEngine` and never `aclose()` it); integration (local Postgres
  modules) leaks in `test_dev1737_date_functions_postgres` (117 tests), `test_integration_postgres` (62),
  `test_dev2046_postgres` (14), `test_dev1756_identifier_length_pg` (9), `test_dev2101_postgres` (7),
  `test_postgres_statement_timeout` (3), `test_dev2172_postgres` (3), `test_dev1933_postgres_verbatim` (3),
  `test_dev2015_postgres` (2). MySQL / ClickHouse / SQL Server / Trino suites need Docker: not measurable
  locally.
- Mechanism: a client's async engine is loop-bound and released only by a `dispose()` on its own loop
  (`aclose()`), per `architecture/python.database.arc42.md` §3.3. A dropped engine keeps its pooled psycopg
  connection open in a reference cycle until an arbitrary later GC, in whichever test runs then; only a test
  that makes `ResourceWarning` an error (this one, with its explicit `gc.collect()`) fails.

Symptom: the flaky test. Structural cause: (1) no single owner construct for a test's query engine — ~20
Postgres modules hand-roll `aclose()`/`close()`/`invalidate_engine`, and 4 unit + 9 integration modules
forget it; (2) nothing attributes a leak to its owner — the DEV-1943 warning gate is sqlite-only, and a
GC-timed finalizer warning lands on an unrelated test. Grounding: python.database §3.3 "One engine owner"
(async engines "each released only on its own loop" [enforced: test:tests/test_law_resource_ownership.py]).
Principles that apply: python.database §3.3, §3.1 (one database door — untouched), system §3 async-first
(the helper releases on the engine's own loop).

## Goals / Non-Goals

**Goals:**

- Every async engine a test causes a client to build is disposed on its own loop before that loop closes,
  and a test that breaks this fails itself, naming the datasource and the test that built the engine.
- One owner construct for a test's query engine, used by every test that builds one over a networked
  datasource.

**Non-Goals:**

- Production code or specified behaviour (release semantics: `database/execution`, "The query engine owns
  the clients it builds"); arc42 / `.c4` edits (§3.3 already states the invariant).
- Rolling the law out backend by backend (Decision 9).
- Moving sqlite / DuckDB test engines that already go through `seeded_exec_engine` (they gain `aclose()`
  through it; other sqlite / DuckDB inline engines build no async engine and are out of scope).

## Decisions

1. **Structural fix, not an instance fix** — test-side ownership plus a per-test law, over patching the 3
   modules from the first repro. The class is "test engines not released on their loop" + "a leak surfaces in
   an unrelated test". (default)
2. **The law judges engine lifecycle events, not GC finalization** — over (a) making psycopg's
   `ResourceWarning` a suite-wide error (still fires at GC time in an unrelated test: the flake, generalized)
   and (b) a per-test `gc.collect()` for Postgres tests (slow; `item.funcargs` keeps fixture values alive
   through teardown, so it misses the owner). (default)
3. **Law = "every async engine a client hands out receives a completed `dispose()` on its creating loop
   before that loop closes"**, recorded as events: engine built (strong reference, creating loop, datasource,
   building test's node id), dispose completed (on that loop), abandoned (`dispose(close=False)` of a
   closed loop's engine). Not inferred from teardown state: `_abandon()` empties the pool without closing
   its connections and removes the engine from the client, so "not held, pool empty" proves nothing; and no
   driver-specific pool inspection, so the law is the same for psycopg, aiomysql, ClickHouse, SQL Server and
   Trino. The losing engine of a concurrent first build never connected and is never handed out, so it is not
   recorded. (codex)
4. **Strong references until judged** — records hold the engine until its loop is judged, so a client or
   engine a test creates inline and drops is still caught; a record is cleared once judged. (codex)
5. **Judged in a `pytest_runtest_teardown` hookwrapper, after `yield`** — over an autouse fixture finalizer,
   whose LIFO position is not guaranteed and which runs before module/session-scoped loops close. After
   `yield`, `teardown_exact(nextitem)` has run: function loops are closed, and at module/package/session
   boundaries so are scoped loops. A violation fails the current item's teardown, naming the building test
   when that differs. An engine on a still-open loop (bridge loop, scoped loop mid-scope) is not judged until
   its loop closes. (codex)
6. **Unit AND integration suites**, no relaxation like the sqlite gate's; every integration leak the law
   finds is fixed — the non-Postgres backends through their path-gated workflows, which this PR triggers
   (`tests/_*.py`). Over unit-only like DEV-1943. (user)
7. **Marker `abandons_async_engines`** for a test that exercises closed-loop abandonment: under it the
   law expects the abandoned engines instead of failing; registered in `pyproject.toml`. Applied to
   `tests/test_sql_client_event_loops.py::test_a_closed_loops_engine_is_dropped` (deliberately drops two
   `asyncio.run()` loops' engines) — marker only, its logic unchanged. Any other test needing it is raised to
   the user. (codex; user approved the test edit)
8. **One owner helper** in `tests/_engine_helpers.py`, an async context manager building a
   `SlayerQueryEngine` over a storage (passing through the engine kwargs callers use) that on exit runs
   `await asyncio.shield(engine.aclose())`, then `engine.close()` in a nested `finally`, then invalidates the
   exact `DatasourceConfig` objects saved into storage (never a rebuilt copy, which can miss fields and key a
   different factory engine). `seeded_exec_engine` is rebuilt on it; a Postgres convenience in
   `tests/_pg_helpers.py` composes it with a datasource from a pytest-postgresql client fixture's `info` or
   from `pg_database`. (default: layout; codex: shield / nested finally / exact datasources)
9. **Integration fixtures keep their scope** — each fixture calls the helper at its existing scope and
   `loop_scope`, with the server / database fixture as a dependency so engines close before the database is
   dropped or the container stops. A backend-by-backend rollout of the law was rejected: conflicts with
   Decision 6, and this PR triggers all four backend workflows anyway. (codex, partly; rollout rejected)
10. **Law self-tests** (pytester, in `tests/test_law_resource_ownership.py`, whose file python.database
    §3.3's enforcement tag already names): leaking function-scoped async fixture fails, naming the test; the
    same through the helper passes; function-, module- and session-scoped loops; a leaking async fixture
    finalized after the test body; a client created inline and dropped; an engine built through
    `SlayerQueryEngine(client_factory=…)`; a sync test using `asyncio.run()`; a bridge-loop engine passes;
    abandonment fails without the marker and passes with it. Each must FAIL without the law. An xdist
    worker-isolation case was rejected: each worker is a separate process, so records cannot cross. (codex,
    partly)
11. **No dedicated test of the helper's cancel / raise cleanup paths** — test-only code, exercised by the
    law's self-tests. (rejected (codex))
12. **No production change, `skip_specs: true`, no arc42 / `.c4` edit** — the change is test harness only.
    (default)
13. **`test_postgres_engine_closes_its_connections` stays unchanged** — with no leaking predecessor it is
    stable. Regression evidence: the pairwise repro after each of the 4 unit leakers and the 29-module
    parallel repro go green. (default)

## Risks / Trade-offs

- [The law flags tests in MySQL / ClickHouse / SQL Server / Trino suites only in CI] → fixed through the helper
  in follow-up pushes on this PR; their workflows run on it.
- [The census prototype raised a TypeError in `test_list_set_equality.py::TestStages` under `-n 6 --dist
  loadscope` (passes alone and without the plugin)] → the real law is verified on the full unit suite under
  xdist before the PR.
- [A recorded engine held until its loop closes keeps a closed loop's connections alive longer within a
  scope] → records are cleared as soon as judged; a leaked one is a failure anyway.
- [Instrumentation patches a private client method (`_get_async_engine`) and the async engine's `dispose` /
  the client's `_abandon`] → same pattern as the existing harness laws (`_statement_render_law`,
  `_emitted_reparse_law`); principles bind production, test instrumentation is exempt.
