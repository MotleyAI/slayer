## Why

#654 (DEV-2454) fixes a fresh install's first run on `1.x`: a deterministic demo, `slayer mcp` answering the handshake before
its startup preparation, one ingest renderer, named validation errors, a release-gate handshake check and a clean-venv
`latest-deps`. `main` has every one of these problems. Merging `1.x` into `main` brings the fix over and keeps `main`
containing every `1.x` commit; `main` differs from `1.x` in the MCP SDK, the ingestion module, CI and vocabulary, so the
merge is also a port.

## What Changes

- Merge `origin/1.x` (with #654) into `main`; never rebase.
- `slayer mcp --demo` / `--ingest-on-startup` answer `initialize` + `tools/list` at once on mcp 2.x `MCPServer`: demo setup
  then startup ingest run as one background preparation job behind a readiness gate; tool calls wait (progress for calls
  with a token); a demo failure becomes every call's tool error; startup-ingest failures go to stderr only. `slayer serve`
  stays synchronous.
- The Jaffle Shop demo is seeded (`jafgen` pinned to `0.4.14`, no unseedable PATH fallback) and carries a generator stamp;
  a missing or differing stamp, or explicitly given differing `--years`, rebuilds it. `datasources create --years`
  defaults to reusing the existing demo.
- One ingest renderer in `slayer/live_schema/ingestion.py` serves demo create, `datasources create --ingest`,
  `slayer ingest` and the startup ingest; demo create no longer prints `Ingested:` lines.
- CLI validation errors name every failing field on one line, never the rejected values.
- The release gate fails when `slayer mcp --demo` does not answer the handshake within `--mcp-handshake-limit` (10s).
- `sqlglot` capped below 30.18 and `snowflake-sqlalchemy` below 2.0 (DEV-2468 lifts both).
- CI: the rossbowen jafgen fork leaves every workflow; `latest-deps` resolves `.[all]` plus the dev tools fresh in a clean
  venv on Python 3.11 and 3.14, keeping `main`'s 4-way sharding and hang watchdog, with `uv` for the Ossie tests.
- Docs in `main`'s vocabulary; the CLI guide shows the real demo count; the 13 demo notebooks are re-executed on the seeded
  demo.
- `openspec/forward-merge-ledger.yaml` lists the #654 requirements `main` words in its own vocabulary.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `cli/commands`: validation errors name their fields; `datasources create demo --ingest` prints like every other ingest.
- `demo/doc-examples`: the release gate's MCP leg asserts a fast handshake.
- `demo/jaffle-shop`: deterministic demo data; the stale-database rule gains the generator stamp and explicit `--years`.
- `mcp/tool-surface`: startup preparation never delays the MCP handshake.

## Impact

- Code: `slayer/mcp/server.py`, `slayer/cli.py`, `slayer/demo/jaffle_shop.py`, `slayer/demo/__init__.py`,
  `slayer/live_schema/ingestion.py`, `scripts/release_gate.py`.
- Dependencies: `pyproject.toml`, `poetry.lock` (`jafgen` 0.4.14, `sqlglot` and `snowflake-sqlalchemy` caps).
- CI: `.github/workflows/ci.yml`, `latest-deps.yml`, `pg-facade-e2e.yml`.
- Docs: `docs/getting-started/cli.md`, `docs/getting-started/mcp.md`, `docs/concepts/ingestion.md`,
  `docs/reference/cli.md`; 13 notebooks under `docs/examples/` re-executed.
- OpenSpec: `openspec/forward-merge-ledger.yaml`; #654's archived DEV-2454 folder joins `main`'s archive.
- Architecture: no model, arc42 or import change.
- Existing demo databases rebuild once after upgrade (no stamp); user dataset edits survive.
