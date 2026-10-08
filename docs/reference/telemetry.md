# Telemetry

SLayer collects **anonymous, aggregated usage statistics** to learn which surfaces, databases and
query features people use. It never sends your queries, data, model / column / datasource names,
memory text, arguments, error messages, file paths or credentials.

## When it is active

Only in processes started by the `slayer` command (`slayer query`, `slayer mcp`, `slayer serve`,
`slayer flight-serve`, `slayer pg-serve`, …, and the Docker image, which runs `slayer serve`).
Using SLayer as a Python library — the query engine, the client, or an app or MCP server you
build in your own code — never records, prints a notice or sends anything.

## Turning it off

The first rule that matches decides:

| # | Rule | Effect |
|---|------|--------|
| 1 | `DO_NOT_TRACK=1` (or `true` / `yes` / `on`) | off |
| 2 | `SLAYER_TELEMETRY=on` / `SLAYER_TELEMETRY=off` | on / off |
| 3 | `CI` is set; SLayer is an editable install; the config directory cannot be written | off |
| 4 | `slayer telemetry enable` / `slayer telemetry disable` | on / off (persisted) |
| 5 | Default | on |

```bash
slayer telemetry status    # on or off, which rule decided it, the install ID
slayer telemetry show      # print exactly the report the next send would contain (sends nothing)
slayer telemetry disable   # turn off for good; also deletes the install ID and unsent data
slayer telemetry enable
```

## The notice

The first time telemetry is on, SLayer prints a one-line notice on **stderr** (never stdout, so
stdio MCP stays clean) and records that it did. It is shown again only if the report format
changes in a major way.

## What a report contains

One JSON document per send; every value is a version, a SLayer-defined token, a boolean, a
bucket, a date, a random ID or a count. Anything SLayer does not recognise is reported as
`other` (or `custom` for user-defined aggregations).

| Field | Content |
|-------|---------|
| `schema` | Report format version. |
| `install_id` | Random UUID created on first use, replaced every 13 months. |
| `batch_id` | Random UUID per send (lets the backend drop duplicates). |
| `period` | First and last **date** covered (no times). |
| `env` | SLayer version, Python `major.minor`, OS family, CPU architecture, whether it runs in a container, installed SLayer extras (read from package metadata). |
| `usage` | Per action — CLI command, MCP tool name, REST route template (e.g. `/models/{name}`), Flight SQL / PostgreSQL query or probe — the count of successes and of failures per error kind (a fixed list such as `value_error`, `schema_drift`, `http_4xx`). |
| `features` | Per query: counts of multistage queries, inline source models, time dimensions, filters, computed dimensions, saved-query runs, non-default `to_many_handling`; counts of each built-in aggregation and transform (`sum`, `time_shift`, …), user-defined ones as `custom`. |
| `dialects` | Queries per SQL dialect; datasources per dialect in buckets `0` / `1` / `2-5` / `6+`. |
| `context` | Per process: storage backend (`yaml` / `sqlite` / `other`), model count in buckets `0` / `1-10` / `11-50` / `51-200` / `200+`, whether the bundled demo database was queried. |
| `mcp_clients` | MCP sessions per client (Claude Code, Claude Desktop, Cursor, VS Code, Windsurf, Codex, Zed, Cline, Goose, Continue; anything else `other`) with its major version. |

## How and when it is sent

Each process keeps its counts in memory and writes them to its own spool file when it ends — no
network at that point. A report is sent at most **once per 24 hours**, and only while SLayer is
running anyway (no heartbeat): when a `slayer` process starts, or a running server records an
action, and the last successful send was 24 hours ago or more. The send merges all spool files
and runs in a background thread; at exit SLayer waits for it at most about one second. Unsent
data stays in the spool (capped at about 100 files / 1 MB) for a later try.

Telemetry never changes a command's output or exit code: every telemetry failure is ignored.

Files live in the platform config directory, not in SLayer's model storage:
`$XDG_CONFIG_HOME/slayer` (default `~/.config/slayer`) on Linux,
`~/Library/Application Support/slayer` on macOS, `%APPDATA%\slayer` on Windows —
`telemetry.json` (setting, install ID, notice and send bookkeeping) and `telemetry-spool/`.

## Destination

Reports go to the PostHog EU capture API (`https://eu.i.posthog.com/i/v0/e/`) as one
`slayer_usage` event per send, keyed by the install ID. Your IP address is not in the report;
PostHog receives it at the network level, our project is configured to discard it, and every
event disables GeoIP lookup. `SLAYER_TELEMETRY_ENDPOINT` overrides the URL.

## Privacy notice

- **Controller**: Motley AI, the maintainer of SLayer.
- **Purpose**: understanding how SLayer is used, to prioritise its development.
- **Processor**: PostHog (EU hosting), under a data processing agreement.
- **Retention**: raw reports (keyed by install ID) are deleted after 13 months; aggregates without
  install IDs are kept. Aggregates are not published; any future summary suppresses every figure
  covering fewer than 10 installs.
- **Your rights**: `slayer telemetry status` prints your install ID; to have its data deleted,
  email privacy@motley.ai with that ID. `slayer telemetry disable` stops collection and deletes
  the local ID and unsent data.
