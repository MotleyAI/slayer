## Context

See proposal.md — Why. The branch is fast-forwarded to `origin/main` `3031da5cb` (DEV-2469 / #658 landed, so DEV-2405 is on
`main`); `origin/1.x` adds only #654 (merge `c7c6e0e21`; commits `13ec93302`, `7c5218f7b`, `aba77e34a`, `623ffb871`,
`a1ee735f5`, `27250117d`, `773a8f3bc`, `61dc3d9ab`, `78b28ff2e`, `e6b7fc7ad`). #654's design and requirements:
`openspec/changes/archive/2026-11-25-dev-2454-…` on `origin/1.x`; its final requirement texts are in `origin/1.x`'s
`openspec/specs` (`e6b7fc7ad` reworded "Demo data is deterministic" after archive).

A trial merge conflicts, for this change, in `ci.yml`, `latest-deps.yml`, `docs/concepts/ingestion.md`,
`docs/getting-started/{cli,mcp}.md`, three `openspec/specs` files, `poetry.lock`, `pyproject.toml`, `slayer/cli.py`,
`slayer/demo/jaffle_shop.py`, `slayer/engine/ingestion.py`, `slayer/mcp/server.py` and the tests `test_demo_cli`,
`test_naming`, `test_release_gate`, `test_startup_ingest`; `pg-facade-e2e.yml`, `docs/reference/cli.md`,
`scripts/release_gate.py`, `slayer/demo/__init__.py` and `tests/test_cli_error_door.py` merge cleanly.

Where `main` differs from `1.x`:
- **MCP SDK**: `main` runs mcp 2.x `MCPServer` (`_import_mcp_sdk`, `CountedMCPServer`, `mcp = ">=2.3,<3"`). Lowlevel
  `Server.run` enters the lifespan once per connection — once for stdio, once per SSE connection (`MCPServer.sse_app`);
  the streamable-HTTP manager enters it once for the app. `MCPServer(lifespan=…)` exists, and `call_tool(name,
  arguments, context)` receives a `Context` with `report_progress`. Preparation exists only under `slayer mcp` (stdio);
  `create_app` (`slayer serve`) builds its embedded MCP server without one.
- **Ingestion**: renderers live in `slayer/live_schema/ingestion.py`; `slayer/engine/ingestion.py` is a `raise_moved`
  stub. `main`'s demo create still prints `Ingested:` lines; `create_mcp_server` runs the startup ingest synchronously.
  MCP `create_datasource` renders through `_render_ingest_result`, MCP's own wire format (DEV-2379 decision 7).
- **Demo**: `main` loads without views (DEV-2455) and builds every statement as sqlglot AST; `ingest_demo_datasets`
  returns datasets.
- **CI**: `ci.yml` installs the rossbowen jafgen fork twice and keys the notebook demo-DB cache on `JAFGEN_SHA`;
  `pg-facade-e2e.yml` installs it once; `latest-deps.yml` upgrades only SQLAlchemy/DuckDB inside the Poetry env on 3.11,
  4 shards, hang watchdog (shard and watchdog options are `tests/conftest.py` plugins). The Ossie corpus tests skip
  without `uv`.
- **Docs**: `docs/getting-started/cli.md` "Verify it works" shows `42` for `count(orders.*)`; 13 of the 19
  `docs/examples` notebooks use the demo, their stored outputs produced on the fork.
- **Vocabulary**: datasets / fields / metrics / relationships; `population` / `metrics` query syntax; MCP
  `ingest_datasource` / `datasets_summary`.

`scripts/forward_merge_specs.py` (CI) requires every requirement the merged `1.x` range adds or modifies to exist on
`main`'s living specs with the same `(title, body hash)`, or be listed in `openspec/forward-merge-ledger.yaml`; a ledger
entry matching no `1.x` requirement fails as stale.

Applicable principles: `system.arc42.md` §3 2 (model truth — `slayer.mcp` → `slayer.demo` stays inside the `surfaces`
bucket), 4 (SQL as sqlglot AST — the demo stamp's write and read; law `tests/test_law_sql_built_as_ast.py`), 5
(async-first — preparation on the server's loop, blocking setup via `anyio.to_thread`), 6 (Pydantic v2 — `StartupOutcome`,
`DemoStamp`, `DemoIngestResult`); `python.live_schema.arc42.md` §3.1 (idempotent, additive ingestion; One ingest door —
demo and startup ingest only through the idempotent pass); `python.database.arc42.md` §3.1 (one database door — startup
ingest through engine-owned clients). No arc42, `.c4` or spec-attachment change.

Structural cause: #654 already fixes its bug classes structurally ("work before the handshake" → one readiness gate;
N ingest renderers → one). The only `main`-specific structural adaptation is the stamp SQL built as AST (principle 4).

## Goals / Non-Goals

**Goals:**
- `main` contains every `1.x` commit; #654's behaviour on `main` in `main`'s layout, SDK and vocabulary.
- Every acceptance item of DEV-2471 backed by a test that fails on `main` before the merge.
- `latest-deps` tests what a fresh install resolves, on both Python versions, sharded, under the watchdog.

**Non-Goals:**
- Lifting the sqlglot / snowflake-sqlalchemy caps (DEV-2468).
- Asynchronous `slayer serve --demo` / `--ingest-on-startup` (#654 non-goal).
- Unifying MCP `create_datasource`'s wire rendering with the printed renderer (rejected (codex), decision 17).
- JDK / Flight SQL JDBC coverage in `latest-deps` (user, decision 14).
- Changing `architecture/*`.

## Decisions

1. **Notebooks**: after the implementation is green, re-execute the 13 demo notebooks under `docs/examples/` on the seeded
   demo and commit their outputs; a missing-column failure (fork-shaped data) → stop and discuss with the user; any other
   environmental failure → stop and ask. Options: A leave the fork outputs / B re-execute. Taken: B. (user)
2. **Sequencing**: fast-forward to `origin/main` once #658 merged (done); port the tests onto `main` first (pr-tests), so
   each fails without the merge; `git merge --no-ff origin/1.x` in pr-implement, first line
   `Merge branch 'origin/1.x' into egor/dev-2471-merge-1x-into-main-reproducible-demo-mcp`; never rebase. At the merge each
   ported test file resolves to its pr-tests version. As DEV-2379. (default)
3. **Conflict rule**: #654's behaviour, `main`'s layout, SDK and vocabulary; `slayer/engine/ingestion.py` stays `main`'s
   stub; `openspec/specs/**` resolve to `main`'s side; the archived DEV-2454 folder comes in as is; this change's delta
   carries #654's requirements in `main`'s wording. (default)
4. **`StartupPreparation` ports unchanged**: counter of lifespan entries (one per stdio / SSE connection, one per app for
   streamable HTTP), last one out cancels, shielded cancel, a cancelled run restarts on the next entry. Plugged in as
   `MCPServer(lifespan=preparation.lifespan)` via `_counted(server_cls, *, tool_error, preparation)`; `call_tool` awaits
   `preparation.wait(…)` inside the usage action before `super().call_tool`; progress via `context.report_progress(progress,
   message=…)`, none without a context, send failures swallowed. (default; wording refined (codex))
5. **Startup ingest leaves `create_mcp_server`'s synchronous path** for the job, on a dedicated `SlayerQueryEngine`;
   `create_app` keeps its synchronous ingest. (default)
6. **The demo stamp is built as sqlglot AST**: `CREATE OR REPLACE MACRO _slayer_demo_stamp() AS '<json>'` as an
   `exp.Create(kind="MACRO")` with an `exp.Literal.string` body; the read as an AST select. (default, principle 4)
7. **One printed ingest renderer** `print_ingest_result(result, *, file=None, data_source=None)` in
   `slayer/live_schema/ingestion.py`, used by the startup ingest, `_ingest_and_print` and demo create; callers keep their
   empty-result message and exit policy. `ingest_demo_datasets` returns `DemoIngestResult(ingest, datasets)`. (default)
8. **Dependency caps from #654**: `sqlglot >=30.17,<30.18`, `snowflake-sqlalchemy >=1.7,<2.0` (comments name DEV-2468),
   `jafgen = "0.4.14"`; `mcp >=2.3,<3`, version `2.0.0` and `main`'s other pins stay; re-lock. (default)
9. **`latest-deps`**: #654's clean venv (`pip install ".[all]" -r <dev requirements>`, `pip check`, `pip freeze`,
   NOSONAR S8541/S8544 reasons) × `main`'s sharding and watchdog: matrix `python-version ["3.11", "3.14"]` × `shard
   [1, 2, 3, 4]`, `fail-fast: false`, `python -m pytest`, hang-report artifacts `hang-reports-latest-deps-<py>-<shard>`.
   (default)
10. **CI fork removal**: drop the fork installs (`ci.yml` ×2, `pg-facade-e2e.yml`) and `JAFGEN_SHA`; the notebook demo-DB
    cache key drops `env.JAFGEN_SHA` (`poetry.lock` pins jafgen, `slayer/demo/**` carries the seed recipe). `main`'s
    `ci.yml` shards, Python matrix and watchdog stay. (default)
11. **CLI guide** keeps `main`'s count query (`{"population": "orders", "metrics": ["count(orders.*)"]}`); the expected
    output becomes the real count. (default)
12. **"Startup preparation never delays the handshake" stays verbatim**; multi-session spans are covered by tests, not a
    new scenario. (default)
13. **Clean-venv-safe tests**: `tests/test_naming.py`'s cold-import subprocess uses `sys.executable`; anything else the
    clean-venv CI run turns up is fixed in this PR. (default)
14. **`latest-deps` sets up `uv`** (`ci.yml`'s pinned `astral-sh/setup-uv`), so the Ossie corpus, portability-grader and
    validity tests run on the fresh resolve; no JDK — the Flight SQL JDBC tests are orphaned until after Ossie L2.
    Options: uv + JDK / uv only / neither. Taken: uv only. (user) (codex)
15. **The dedicated preparation engine is released fully**: `await prep.aclose()` then `prep.close()` in a nested `finally`
    (`aclose` keeps clients cached; `create_app`'s shutdown does both). #654 calls only `aclose`. (codex)
16. **Stamp escaping is tested**: a stamp value containing a quote and a newline writes and reads back intact. (codex)
17. Rejected (codex): routing MCP `create_datasource` through the printed renderer or adding parity tests — MCP's wire
    format is its own contract (DEV-2379 decision 7), and the acceptance names the four printed paths.
18. Rejected (codex): real SSE / streamable-HTTP preparation tests — preparation is only built under stdio `slayer mcp`;
    #654's in-memory session tests (each session enters the lifespan) cover spans, interruption and shutdown.
19. Rejected (codex): a demo-build test at both sqlglot bounds — `>=30.17,<30.18` admits only 30.17.0, the locked version.
20. **Ledger** (decided after the merge by running the guard): permanent "main keeps its own wording" entries for "CLI
    errors are one line on stderr" ("second metric"), "Demo data is deterministic" (`main`'s query syntax), "A stale demo
    database is rebuilt" (dataset); the existing "datasources create --ingest runs the idempotent ingest" entry's hash is
    replaced by `1.x`'s new hash; temporary entries for the requirements whose `1.x` text this delta keeps verbatim
    ("Startup preparation never delays the handshake", "A release uploads only a wheel that runs the README's queries on
    a fresh install") until archive merges them into `main`'s living specs, removed at archive. Rejected (codex): no
    temporary entries — `openspec/specs/**` stay `main`'s until archive, so the guard needs them, as in DEV-2379. (default)

## Risks / Trade-offs

- [Clean auto-merges carry 1.x vocabulary silently] → `git diff origin/main --stat` audit; grep the changed files for
  `source_model`, `measures`, `models_summary`, `ingest_datasource_models`, `SlayerModel`, `slayer.engine.ingestion`.
- [Git resurrects `slayer/engine/ingestion.py`'s 1.x body] → `git diff origin/main -- slayer/engine/ingestion.py` is empty.
- [The clean-venv run surfaces main-only test breakage (tools or packages only Poetry provided)] → fixed in this PR
  (decision 13).
- [Dropping the fork changes demo-shaped data that tests or notebooks pin] → tests aligned with per-file approval;
  notebooks per decision 1.
- [mcp 2.x hook points differ from FastMCP 1.x in the test helpers (progress-failure scenario, in-memory sessions)] →
  ported to mcp 2.x's equivalents in pr-tests.
- [DEV-2462 also edits `ci.yml`] → whichever lands second resolves the small conflict.
- Existing demo databases rebuild once after upgrade (no stamp); the atomic swap keeps a complete file; additive healing
  keeps user dataset edits.
