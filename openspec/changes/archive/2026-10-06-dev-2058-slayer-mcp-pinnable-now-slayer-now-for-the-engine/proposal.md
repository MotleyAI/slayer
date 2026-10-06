## Why

`slayer mcp` / `slayer serve` resolve relative time points (`'last 3 months'`, `'this quarter'`, the
`whole_periods_only` / time-spine default upper bound) against the real wall clock, and nothing outside
code can pin it — so evals and probe suites cannot get reproducible answers to clock-dependent queries
over a fixed dataset (DEV-2058; the slayer-evals benchmark DEV-2055 xfails its relative-date tasks on it).

## What Changes

- New env var `SLAYER_NOW` (naive ISO-8601 datetime or date) pins "now" for every engine built
  without an explicit `clock=` — MCP, REST, CLI, Flight, Postgres facade and the Python client alike.
- One canonical host clock (`host_clock()` in `slayer/core/time_points.py`) is the engine's default
  clock; precedence: explicit `clock=` > `SLAYER_NOW` > host wall clock. The engine reads the variable
  once, at construction.
- Timezone-aware or unparseable values are rejected with an error naming `SLAYER_NOW`; the `slayer`
  CLI validates it before dispatching any command (so `--demo` / ingestion never run first), and
  `create_mcp_server` / `create_app` build their engine before seeding or ingestion.
- The comparison probe runner pins `SLAYER_NOW` once, so its MCP `steps` probes share the direct
  probes' `NOW`.
- No CLI flag; unset/empty `SLAYER_NOW` keeps today's behaviour.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/time-points`: the engine clock is the host wall clock unless pinned by `SLAYER_NOW`;
  new requirement specifying the pin, its accepted values, precedence and startup failure.

## Impact

- `slayer/core/time_points.py` (new `host_clock`), `slayer/engine/query_engine.py` (default clock),
  `slayer/cli.py` (startup validation), `slayer/mcp/server.py` and `slayer/api/server.py` (engine
  built first; `query` tool docstring), `examples/comparisons/slayer/run_slayer.py`.
- Docs: `docs/concepts/time.md`, `docs/interfaces/cli.md`, `docs/reference/mcp.md`.
- No new dependencies, no new import arrows, no arc42/.c4 change.
