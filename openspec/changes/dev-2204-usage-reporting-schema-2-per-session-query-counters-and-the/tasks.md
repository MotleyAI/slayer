## 1. Tests (pr-tests; all fail before implementation)

- [x] 1.1 Payload tests (`tests/test_usage_reporting_payload.py`): report `schema` is 2; `first_query_ok` is a date or null; `mcp_sessions` rows merge by bucket pair and sort by bucket rank (`21+` after `6-20`); a schema-1 spool file without `mcp_sessions` loads; the hostile-string sweep covers the new fields — verify they fail now
- [x] 1.2 Session tests over real MCP client sessions (`tests/test_usage_reporting_counting.py`): 2 ok + 1 failing `query` → one row `2-5`/`1`; tools-only session → `0`/`0`; two equal sessions → one row with 2 sessions, differing sessions → two rows; `dry_run` skipped; cancelled `query` → failed bucket; concurrent calls in one session and concurrent sessions on the mounted HTTP endpoint attributed exactly once; many sequential HTTP sessions release live state without losing totals; a send mid-session carries no row and the process-end spool holds the full totals; a fork clears populated session state — verify they fail now
- [x] 1.3 Signal tests (`tests/test_server_shutdown.py` style subprocesses): `slayer mcp` stopped by SIGTERM, and by a forced second signal after recording started, spools its session totals — verify they fail now
- [x] 1.4 First-query-date tests: set by a successful `mcp:query`, `cli:query`, `rest:/query`, `flight:query`, `pg:query`; not set by failures, dry runs (MCP, CLI `--dry-run`, REST) or Flight/PG probes; unchanged by a later success; cleared by rotation (start and send paths) and by `disable`; present in `show` and in the posted event; not stored by library use; at most one config write per install ID per process; another process's rotation seen by a running recorder, which then stores the date for the new ID — verify they fail now
- [x] 1.5 MCP database-failure tests (`tests/test_mcp_server.py` / counting): `query`, `create_model` (both paths), `validate_models`, `ingest_datasource_models` return `isError: true` with the "Database error:" text through FastMCP 1.27's wrapping, and `mcp:<tool>` counts a `db_*` token, not `tool_error` — verify they fail now
- [x] 1.6 Spool/config compatibility tests (`tests/test_usage_reporting_delivery.py`): schema-2 spool names start `v2-`; a legacy `spool-` file is sent and deleted after 2xx; a simulated schema-1 sender (claims only `spool-`/`claim-`) leaves `v2-` files untouched and a later schema-2 send includes them; cap and `disable` cover every version; an unknown config field survives a start's rewrite — verify they fail now
- [x] 1.7 Notice tests (`tests/test_usage_reporting_controls.py`): re-shown once for recorded schema 1; not shown and not lowered for a recorded schema 3; text names the new fields — verify they fail now

## 2. Payload and settings

- [x] 2.1 `payload.py`: `SCHEMA_VERSION = 2`, `Literal[2]`, query-count bucket token, `McpSessionCount` rows on `Batch` with merge by pair and rank sort, `first_query_ok` on `UsageReport` — 1.1 passes
- [x] 2.2 `settings.py`: `first_query_ok` on `UsageReportingConfig`, `extra="allow"`, `with_install_id` clears the date on a new ID — rotation and unknown-field tests pass

## 3. Recorder

- [x] 3.1 Session counters: per-session key from `CountedFastMCP`, live integer counters, `weakref.finalize` into the ended histogram, emitted only by the process-end flush, cleared after fork; dry-run flag skips — 1.2 and 1.3 pass
- [x] 3.2 First-query date: qualifying keys, set-if-unset under the config lock, stat-validated cache — 1.4 passes
- [x] 3.3 Notice rule (lower-than, store max) and new `NOTICE` text — 1.7 passes

## 4. Surfaces

- [x] 4.1 `slayer/mcp/server.py`: session key to the recorder; the four tools raise the tool-failure exception chained from the database error; the counter follows the cause chain — 1.5 passes
- [x] 4.2 Dry-run marking for MCP `query`, `slayer query --dry-run` (`slayer/cli.py`) and REST `/query` (`slayer/api/server.py`) — dry-run cases of 1.2 and 1.4 pass

## 5. Spool, sender, control

- [x] 5.1 `spool.py`: `v2-spool-`/`v2-claim-` names; claim own, older and legacy files; cap and purge over all versions — 1.6 passes
- [x] 5.2 `sender.build_report` and `control.pending_report` carry `first_query_ok`; `control.disable` clears it — 1.4 `show`/event/disable cases pass

## 6. Docs and gates

- [x] 6.1 `docs/reference/usage-reporting.md`: table rows for `first_query_ok` and `mcp_sessions`, one sentence that `usage-reporting.json` holds the first-query date — page reads correctly and `tests/test_doc_examples.py` passes
- [x] 6.2 Full unit suite, `ruff check slayer/ tests/`, `basedpyright` (no new errors), `la-arch-check`, `openspec validate --strict` — all green
