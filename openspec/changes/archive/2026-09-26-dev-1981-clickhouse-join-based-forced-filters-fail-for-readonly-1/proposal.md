## Why

A join-based forced filter scopes each join target with a correlated `EXISTS`, and on ClickHouse SLayer attaches `SETTINGS allow_experimental_correlated_subqueries = 1` to it. On ClickHouse 25.4–25.7 that setting defaults to 0, and a `readonly = 1` user may not change it, so every query under a join-based session policy fails with `Code: 164 (READONLY)`. Below 25.4 such policies fail closed outright, and correlated subqueries never run over `Distributed` tables on any version. The query engine's own semi-join pushdown hits the same readonly failure on 25.4–25.7, surfacing only as the raw database error.

## What Changes

- On ClickHouse, a join rule scopes its target with a non-correlated, null-guarded `IN` semi-join instead of a correlated `EXISTS`: no setting is sent, and join-based policies work on every ClickHouse version, for `readonly = 1` users, and over `Distributed` tables. Other dialects keep the correlated `EXISTS` unchanged.
- The policy path's ClickHouse version gate and its version probe are removed.
- A semi-join-pushdown query on ClickHouse whose user cannot enable correlated subqueries (server < 25.4, or setting off and `readonly = 1`) fails closed before execution with a SLayer error naming the pushed filters, the setting, and the remedies.
- One per-engine server-profile probe (version, readonly level, correlated-subquery setting) replaces both the DEV-1977 timeout permission check and the engine's ClickHouse version probe; a failed probe is no longer cached.
- The correlated-subquery setting is attached while the statement is still an AST, removing the engine's re-parse of rendered SQL.

## Capabilities

### New Capabilities

- `sql/forced-filters`: how a session policy's join rules scope their targets on a database whose correlated subqueries need a server setting.

### Modified Capabilities

- `queries/cross-model-aggregates`: the semi-join pushdown's ClickHouse scenario gains the readonly-user refusal and its error contract.

## Impact

- Code: `slayer/sql/session_policy.py`, `slayer/sql/dialects/{base,clickhouse}.py`, `slayer/sql/client.py`, `slayer/sql/generator.py`, `slayer/engine/query_engine.py`, `slayer/ir` (semi-join plan predicate).
- Tests: session-policy, ClickHouse gate, statement-timeout hook and fake, engine policy tests; live ClickHouse suite gains a 25.4 container.
- Docs: `docs/concepts/row-level-security.md`, `docs/concepts/queries.md`, the RLS example notebook.
- Closes DEV-1981.
