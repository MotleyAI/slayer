## Why

`engine.execute(query, explain=True)` fails on every SQL Server datasource: `TsqlDialect` sends `SET SHOWPLAN_ALL ON; <sql>; SET SHOWPLAN_ALL OFF` as one batch through `client.execute`, and SQL Server rejects it (error 1067, "The SET SHOWPLAN statements must be the only statements in the batch"; reproduced live on SQL Server 2022). The cause is structural: EXPLAIN is modelled as one text sent through `execute`. That shape cannot express a plan mode that is session state, and it cannot guarantee the state is undone; a pooled connection left with SHOWPLAN on keeps it after being returned.

## What Changes

- EXPLAIN becomes a typed statement set, `ExplainStatements`, built by the dialect: the statement whose rows are the plan, plus an optional pair of session statements that switch plan mode on and off (built as sqlglot AST). SQL Server's EXPLAIN is the bare query between `SET SHOWPLAN_ALL ON` and `SET SHOWPLAN_ALL OFF`; every other dialect keeps its single-statement EXPLAIN.
- The SQL client contract gains `explain(statements)`. It runs the statements on one connection, always restores the session (on failure and on cancellation too), and never returns a connection whose restore is unconfirmed to the pool. `DelegatingSqlClient` forwards it.
- The query engine runs `explain=True` through `explain`, never through `execute`.
- A client written for the current contract keeps working: single-statement EXPLAIN reaches its `execute` with a deprecation warning until SLayer 2.0. A session-mode EXPLAIN on such a client is refused with `ExplainUnsupportedError`, and never bypasses a gateway.
- `ExplainUnsupportedError` (a `SlayerError`, so still a `ValueError`) replaces the bare `ValueError` BigQuery's EXPLAIN raises.
- `SqlDialect.build_explain_sql` is deprecated until 2.0; `explain_postfix` and the engine's `_build_explain_sql` are removed.
- Both new 2.0 removals are pinned by the removal tripwire.
- The conformance suite checks `explain`.
- Docs: the client contract documents `explain`; the stale claim that BigQuery's `explain=True` returns the dry-run SQL is corrected.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `sql/client-contract`: the contract gains `explain`, with its session-restore guarantee; delegation forwards it; the conformance suite checks it; clients without it keep working through a deprecated route.
- `sql/execution`: explain statements reach the factory's client through `explain`.
- `compatibility/removal-tripwire`: the covered workarounds include the deprecated `build_explain_sql` and the explain-through-`execute` route.

## Impact

- Code: `slayer/sql/dialects/base.py`, `slayer/sql/dialects/tsql.py`, `slayer/sql/client.py`, `slayer/sql/conformance.py`, `slayer/engine/query_engine.py`, `slayer/core/errors.py`.
- Public API: `SqlClient` gains `explain`; `ExplainStatements` and `ExplainUnsupportedError` are new; `SqlDialect.build_explain_sql` warns.
- Tests: dialect, client, conformance and tripwire tests; a live SQL Server integration test; tests pinning the old single-text EXPLAIN shape are aligned.
- Docs: `docs/configuration/sql-clients.md`, `docs/database-support.md`.
- Architecture: `system.arc42.md` §3.6 and `python.sql.arc42.md` §3.1 name the EXPLAIN keyword prefix as the one text composition sqlglot cannot represent (approved).
