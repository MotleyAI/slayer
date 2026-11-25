## Context

See proposal.md (Why). Measured on 1.x: a cold `slayer mcp --demo` spends ~12.5s in jafgen, ~9s loading 86 MB
of CSV into a 168 MB DuckDB file, ~5.5s ingesting, ~1s starting up — all before `mcp.run()`, because
`_run_mcp` calls `_prepare_demo` first and `create_mcp_server(ingest_on_startup=True)` runs the startup ingest
synchronously. jafgen 0.4.14 (PyPI, what users get) draws from unseeded `random`, numpy's legacy `np.random`
and `uuid.uuid4()` (`os.urandom`); it seeds Faker itself, and Faker only yields `customers.name`. CI installs
a rossbowen jafgen fork over PyPI jafgen (`ci.yml`, `latest-deps.yml`, `pg-facade-e2e.yml`) — a different
generator than users run. `build_jaffle_shop` reuses any DB whose schema matches, regardless of data or years.

Applicable principles (`architecture/system.arc42.md` §3): #7 async-first (blocking work bridged off the
server loop), #12 Pydantic v2 only, #14 one ingest door (demo and startup ingest only through the idempotent
pass; the renderer change is presentation only), #17 one database door (startup ingest via engine-owned
clients). No model/arc42 changes: the gate lives in `slayer.mcp`, orchestration in `slayer.cli`, both in the
`surfaces` bucket; spec attachments unchanged (`demo`, `mcp` on `python.surfaces`; `cli` cross-cutting).

## Goals / Non-Goals

**Goals:** startup latency of `slayer mcp` independent of preparation work; "same generator, same demo" as a
guarantee for fresh installs and upgraders alike; one ingest renderer; CI that tests what users resolve.

**Non-Goals:**
- Shipping or hosting pre-generated demo data (deferred; option A alone removes the timeout risk).
- Making `slayer serve --demo` / `--ingest-on-startup` asynchronous (HTTP has no handshake timeout).
- Reporting curated demo enrichment in ingest output.
- Pinning Faker (it changes names only; its version is in the stamp instead).
- The forward merge to main and the main-only findings (sqlglot `DROP VIEW`, duplicate deprecation output).

## Decisions

1. **MCP startup = background preparation job behind a readiness gate** `(user)`. Alternative: ship the data
   in the wheel (still machine-dependent, +15–30 MB). A hosted download was considered and deferred `(user)`.
2. **Held calls, progress notifications, demo failure = permanent tool error, server stays up** `(default)`.
3. **The gate also covers `mcp --ingest-on-startup`; `serve` stays synchronous** `(user)` — one mechanism for
   the bug class "work before the handshake".
4. **Gate mechanics** `(codex)`:
   - The job is created in FastMCP 1.x's `lifespan` hook, on the server's own loop, owned by it, cancelled and
     awaited on shutdown. Never created in `_run_mcp` or the synchronous factory; never started lazily.
   - Blocking work (jafgen, DuckDB load, sync demo setup) runs via `anyio.to_thread`. Startup ingest uses a
     dedicated preparation engine created inside the job and closed with `await aclose()` in `finally`; the
     serving engine is never touched from another loop; no `run_sync` on the server loop.
   - Waiting calls await the shared job through `asyncio.shield`, so a cancelled request cannot cancel it.
   - Progress is emitted by the waiting request itself, via its own request context/progress token, in a
     loop that stops on completion or cancellation; a failed notification is swallowed.
   - The job's outcome is an explicit Pydantic state (pending / ready / demo-failed with reason). Only a demo
     failure fails tool calls; startup-ingest outcomes (datasource exceptions, table-level `result.errors`)
     are reported on stderr and never block — demo errors are never folded into startup-ingest failures.
5. **Drop the rossbowen jafgen fork from every workflow** `(user)` — the fork is a different generator; the
   label "not on PyPI" is stale since jafgen 0.4.14 became a core dependency.
6. **Pin `jafgen = "0.4.14"`; Faker unpinned** `(user)`. Faker drives only customer names (probed) `(codex)`.
7. **latest-deps on Python `["3.11", "3.14"]`** `(user)`.
8. **latest-deps resolves in a clean venv** `(codex)` — supersedes the earlier default of an eager upgrade
   inside the Poetry env, whose leftover lock state can mask fresh-install breakage. One `pip install
   ".[all]" <dev tools from [tool.poetry.group.dev.dependencies]>` resolve, no lockfile, then `pip check`,
   `pip freeze`, and the unit suite from the checkout. No `--prefer-binary`/extra skipping on 3.14 —
   rejected (codex): a packaging failure there is a fresh-install failure users hit.
9. **Generator stamp; rebuild on mismatch** `(user)`. Stamp = jafgen version, Faker version `(codex)`, seed-recipe
   version constant, years. Stored as a DuckDB scalar macro `_slayer_demo_stamp()` `(codex)` — probed: it
   persists, reads back, and is absent from `duckdb_tables()` / `information_schema.tables`, so ingestion
   (tables and views only) never sees it, even with `--all-schemas`. The rebuild reuses the existing
   sibling-file + atomic-swap path, then additive model healing.
10. **Years in the stamp; rebuild on a years mismatch only when `--years` was given explicitly** `(user)`.
    `datasources create demo`'s `--years` default becomes "unset" (builds 2 when nothing is reused);
    `mcp/serve --demo` never pass years explicitly.
11. **Seeding bootstrap in the child interpreter** `(default)`: seed `random` and `np.random` with fixed
    constants; replace `uuid.uuid4` with `uuid.UUID(int=<dedicated seeded random.Random>.getrandbits(128),
    version=4)` so UUID draws never perturb jafgen's own `random` stream; `PYTHONHASHSEED=0`; then
    `jafgen.cli.app()`. Probed: byte-identical CSVs across runs.
12. **Remove the unseedable PATH fallback in `_jafgen_cmd`** `(default)`; not importable → the existing
    "jafgen is required…" error.
13. **One shared ingest-result renderer** `(default)`, taking the result object `(codex)`; caller-specific
    empty-result guidance and exit policy stay with callers (`slayer ingest` keeps its stricter exit).
    `ingest_demo_models` returns a Pydantic result carrying both the `IdempotentIngestResult` and the
    enriched stored models `(codex)`, so `ensure_demo_datasource` and the "Demo ready … N model(s)" line keep
    working. Enrichment is not reported.
14. **Validation-error line** `(default)`: built from `exc.errors(include_input=False, include_url=False,
    include_context=False)`; each `loc` part stringified and dot-joined; each message through `_one_line`;
    capped at 10 fields then `…and N more` `(codex)`; no `--debug` hint; the generic non-SLayer format and its
    scenario are kept `(codex)`.
15. **mcp.md lists Linux, macOS and Windows storage paths** `(default)`.
16. **Release gate asserts `initialize` + `tools/list` within 10s on empty storage** `(user)` `(codex)`; the
    first tool call keeps the 300s per-call timeout and must return demo rows.

## Risks / Trade-offs

- [The first tool call on a cold `mcp --demo` takes ~25s] → progress notifications reset per-request
  timeouts in clients that honour them; 25s is under the MCP TypeScript SDK's 60s default either way.
- [Existing demos rebuild once after upgrade (no stamp)] → atomic swap keeps a complete DB at all times;
  additive healing keeps user model edits; the data is synthetic.
- [A Faker upgrade triggers a rebuild that only changes names] → accepted: the stamp means "what this
  environment would build".
- [Dropping the fork may surface tests/notebooks that depended on fork-shaped data] → fixed in this change;
  notebooks re-executed with saved outputs.
- [The 10s handshake check could flake on a very slow runner] → ~1–2s real cost leaves a wide margin.
- [A dev tool's upper bound could hold back a runtime dependency in the clean-venv resolve] → `pip freeze`
  output makes it visible.

## Migration Plan

No user action. Upgraded installs rebuild an unstamped demo on its next setup. Rollback = revert; a demo
built by this change stays usable by older releases (same schema; the macro is ignored).
