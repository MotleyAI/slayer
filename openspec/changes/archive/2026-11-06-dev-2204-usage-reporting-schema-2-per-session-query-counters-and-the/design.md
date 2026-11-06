## Context

See proposal.md (Why). The recorder (`slayer/usage_reporting/recorder.py`) learns an action's outcome only from whether it raises; `CountedFastMCP` (`slayer/mcp/server.py`) records each tool call that way and tracks sessions in a `WeakSet` to count each client once. Four MCP tools turn `_DB_FAILURES` into a returned "Database error: …" string, so their failures reach the counter as successes — the structural cause of the miscount. Principles 1–5 of `architecture/python.usage_reporting.arc42.md` bind every change here: fail-silent, vocabulary-only values, no user-derived values, errors by class only, sizes bucketed. No arc42 or `.c4` edits; the specs attach through the existing `usage_reporting` → `usage-reporting` and surfaces → `mcp` node metadata.

## Goals / Non-Goals

**Goals:**
- Exact per-session activation counts and an install-level first-activation date, with every outcome reaching the counter through the one exception path.
- No loss of counts or state when SLayer versions of different report schemas share one config directory, as far as the new version can ensure it.

**Non-Goals:**
- Other MCP tools that return non-database failures as success text — DEV-2207.
- The MCP client in session rows (client breakdown stays in `mcp_clients`).
- Repairing older releases' behaviour (a schema-1 binary still drops unknown config fields and re-prints the notice).

## Decisions

1. **MCP database failures are tool errors** in `query`, `create_model` (both paths), `validate_models`, `ingest_datasource_models`, rather than the tool signalling failure to the counter on a side channel while returning success. A tool raises a dedicated client-safe tool-failure exception carrying the friendly text, chained (`from`) the database exception; FastMCP 1.27 wraps it as `ToolError("Error executing tool <name>: <text>")`. `CountedFastMCP` follows the `__cause__` chain past FastMCP's `ToolError` and the tool-failure exception and counts the first other exception's class token — never a message. `create_datasource`'s auto-ingest note and `describe_datasource`'s connection test stay results (they report status). (user)
2. **First-query date: every query surface** — `mcp:query`, `cli:query`, `rest:/query`, `flight:query`, `pg:query`; probes and catalog queries excluded — rather than MCP and CLI only. (user)
3. **Dry runs are skipped** for both the session counters and the first-query date; `explain` counts (it runs against the database). Install-wide usage counters are unchanged. Each surface with a dry-run mode (MCP `query`, `slayer query --dry-run`, REST `/query`) tells the recorder that the counted query action is a dry run; Flight SQL and the PostgreSQL wire have none. (user)
4. **Sessions are reported once, at process end**, rather than in every batch taken while live (which splits a long session into several, mostly unactivated ones). Mid-process sends exclude sessions; `mcp_clients` keeps counting each session at its first tool call. (user)
5. **Session histogram, not raw rows**: rows `{queries_ok, queries_err, sessions}` over buckets `0` / `1` / `2-5` / `6-20` / `21+`; bounded size, same shape as the per-process `context` histograms, enough for "sessions with ≥1 OK query". Equal bucket pairs merge into one row; rows sort by bucket rank, not lexically. (default, codex)
6. **Session state is bounded**: `CountedFastMCP` hands the recorder an opaque key per session (on client observation and on each recorded tool call); live sessions keep integer counters, and a `weakref.finalize` on the session object moves an ended session's totals into a histogram of ended sessions. Both are emitted only by the process-end flush; a fork clears both. The recorder still imports no SLayer node. (codex)
7. **Sessions on the mounted MCP endpoint of `slayer serve` count too**, through the same session tracking as stdio. (default)
8. **The first-query date lives in the config** (`UsageReportingConfig.first_query_ok`) next to `install_id`, as the issue says, rather than in a separate file. The recorder sets it under the config lock only if unset, on the first qualifying success. It caches "already set" together with the config file's identity (inode, mtime, size) and re-reads the config only when a `stat` shows the file changed, so a rotation by any process is seen without taking a lock per query. (default, codex)
9. **Rotation and `disable` clear the date**: `with_install_id` drops it whenever it creates a new ID; `disable` clears it with the install ID. The date belongs to its install ID, and rotation exists to cut the link to the old one. (default)
10. **Spool file names carry the schema version**: schema-2 files are `v2-spool-…json` (pending) and `v2-claim-…json` (claimed) — names a schema-1 SLayer, which matches only `spool-` / `claim-`, never claims or deletes. A sender claims legacy files and its own and older versions' files; the cap and `purge` cover every version. (user)
11. **Config keeps unknown fields** (`extra="allow"` on `UsageReportingConfig`), so a later schema's fields survive this version's rewrites. (user)
12. **Notice rule**: printed when the recorded `notice_shown_schema` is unset or lower than `SCHEMA_VERSION`; the stored value becomes the maximum of both, never lowered. (default)
13. **Notice wording** (user, approved wording): "SLayer collects anonymous, aggregated usage statistics (command, tool and query-feature counts, query outcomes per MCP session, the date of the first successful query, versions, OS) to guide its development - never queries, names, data or credentials. Details: https://docs.motley.ai/slayer/reference/usage-reporting/ - opt out with SLAYER_USAGE_REPORTING=off, DO_NOT_TRACK=1 or `slayer usage-reporting disable`."
14. **A follow-up issue for other tools' text failures** (DEV-2207) rather than folding them in here. (user)
15. Keeping the first-query date in a separate file keyed by install ID — rejected (codex): it contradicts the issue and cannot stop a schema-1 binary re-printing the notice; the user chose the config field plus decision 11.

## Risks / Trade-offs

- [A schema-1 binary rewriting the config drops `first_query_ok` and resets `notice_shown_schema` to 1] → accepted: the next successful query on a schema-2 binary sets the date again (later), and the notice prints once more after each switch; affects only users alternating versions.
- [A schema-1 `disable` leaves `v2-` spool files on disk] → they are never sent, because the persisted disabled setting stops every later sender; a schema-2 `disable` deletes them.
- [A schema-1 SLayer never sends schema-2 data] → a schema-2 sender sends it.
- [Session rows arrive only after their process ends; a SIGKILL loses them] → acceptable for a weekly readout; stdio sessions end with their client, servers spool on SIGTERM.
- [A `stat` of the config file per qualifying successful query] → one syscall, no lock; the write happens at most once per install ID.

## Migration Plan

Schema-1 spool files are read unchanged (`mcp_sessions` defaults empty) and sent by the first schema-2 send. Existing configs gain `first_query_ok` at the next successful query; the notice prints once per install. EXP-0004's PostHog SQL filters on `schema = 2` for the new fields.
