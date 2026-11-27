## Why

A user installing `motley-slayer[all]` 1.9.1 from PyPI hit a string of first-run problems that all hold on 1.x:
the demo data differs on every build, the CLI guide shows a wrong count, `slayer mcp --demo` answers only
after the whole demo build (21–29s, against a reported ~30s client startup timeout), the demo ingest prints
its own output format, a docs page gives only the Linux storage path, CLI validation errors hide the failing
fields, and the latest-deps CI job does not test what a fresh install resolves.

## What Changes

- The Jaffle Shop demo becomes deterministic: jafgen runs through a seeding bootstrap (Python `random`,
  numpy's legacy `np.random`, a seeded `uuid.uuid4`, `PYTHONHASHSEED=0`); `jafgen` is pinned to `0.4.14`;
  the unseedable PATH fallback for the jafgen executable is removed.
- Each built demo database carries a generator stamp (jafgen version, Faker version, seed-recipe version,
  years) in a DuckDB scalar macro; demo setup rebuilds a database whose stamp is missing or differs, and
  rebuilds on a `--years` mismatch only when `--years` was given explicitly.
- `docs/getting-started/cli.md` shows the count the CLI really prints on the demo, checked by a test.
- `slayer mcp` answers the MCP handshake at once: demo setup (`--demo`) and startup ingest
  (`--ingest-on-startup` / `SLAYER_INGEST_ON_STARTUP`) run as one background preparation job, and every tool
  call waits for it (with progress notifications). A failed demo setup becomes a tool error on every call.
  `slayer serve` stays synchronous.
- The release gate asserts that the installed `slayer mcp --demo` completes `initialize` + `tools/list`
  within 10s on empty storage.
- `slayer datasources create demo --ingest` prints its ingest result with the same renderer as every other
  ingest (`Created:` / `Updated:` lines and report sections); one shared renderer serves all ingest callers.
- The CLI renders a pydantic validation error as one line naming every failing field and why.
- `docs/getting-started/mcp.md` gives the storage path for Linux, macOS and Windows.
- CI: the jafgen fork install is removed from every workflow; latest-deps resolves every dependency fresh
  (clean venv, `pip check`, `pip freeze`) on Python 3.11 and 3.14.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `demo/jaffle-shop`: adds deterministic demo data; extends the stale-database rule with the generator stamp
  and explicit `--years`.
- `demo/doc-examples`: the release gate's MCP leg asserts a fast handshake.
- `mcp/tool-surface`: startup preparation never delays the MCP handshake.
- `cli/commands`: validation errors name their fields; `datasources create demo --ingest` prints like every
  other ingest.

## Impact

- Code: `slayer/demo/jaffle_shop.py`, `slayer/cli.py`, `slayer/mcp/server.py`, `slayer/engine/ingestion.py`,
  `scripts/release_gate.py`.
- Dependencies: `jafgen` pinned to `0.4.14` (`pyproject.toml`, `poetry.lock`).
- CI: `.github/workflows/ci.yml`, `latest-deps.yml`, `pg-facade-e2e.yml`.
- Docs: `docs/getting-started/cli.md`, `docs/getting-started/mcp.md`, and the `--ingest-on-startup` wording
  for `slayer mcp`.
- Existing demo databases are rebuilt once after upgrade (their stamp is missing); user model edits survive.
- No architecture model changes.
