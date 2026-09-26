## Context

See proposal.md — Why. Governing principles: sql §3.1 (AST end to end, no re-parse
of rendered SQL), §3.2 (dialect quirks only in `dialects/`), §3.9 (fail closed),
system §3.15 (RLS fails closed). No arc42 or LikeC4 change: `sql` → `sql.dialects`
and `engine` → `sql` / `ir` are existing arrows.

Live probes (throwaway containers, `readonly = 1` user):
- `allow_experimental_correlated_subqueries` defaults to 0 on 25.4–25.7, 1 from 25.8;
  correlated subqueries do not exist below 25.4.
- `readonly = 1` rejects `SETTINGS x = 1` (Code 164) only when it changes the value;
  re-sending the current value passes.
- Correlated `EXISTS` over `Distributed` tables fails on every version ("not supported
  with remote tables"); non-correlated `IN` runs on 24.x through 26.x, incl. `Distributed`.
- With `transform_null_in = 1`, a plain `IN` admits a NULL-key row when the subquery
  yields a NULL; a `to_col IS NOT NULL` guard closes it.
- `getSetting('readonly')` needs no grant; `getSetting` of an unknown setting errors
  (so the correlated setting is read only on 25.4+); `getSettingOrDefault` exists
  only from 24.10.

## Goals / Non-Goals

**Goals:** join-based policies need no server setting on ClickHouse; no
`dialect == …` branching on either correlated path; one server-profile probe; the
correlated setting attached as AST.

**Non-Goals:** changing the semi-join pushdown's SQL shape (it stays a correlated
`EXISTS` — conjuncts may mix outer and inner columns, be negated, and use
null-extended spines); changing any dialect's RLS SQL other than ClickHouse.

## Decisions

**D1 — Capability flag.** `SqlDialect` gains a boolean (working name
`correlated_subqueries_gated`, default `False`; `True` on `ClickhouseDialect`):
correlated subqueries need a server setting this user may be unable to set. Two
consumers read it: the policy rewrite (D2) and the engine's semi-join gate (D4).
Rejected: a dialect AST hook building the RLS predicate (Codex) — it would split the
RLS builder across nodes and cannot serve the second consumer; flags such as
`approx_count_distinct_native` are the established data-shaped §3.2 pattern. Rejected
(user): emitting `IN` on every dialect — changes every dialect's SQL for a narrow
version range of one database.

**D2 — Guarded `IN` for gated dialects.** `session_policy.py` reads the flag. Gated:
each targeting rule becomes `_rls_src.<from0> IN (SELECT _rls_j0.<to0> FROM <hop0> AS
_rls_j0 [INNER JOIN <hop i> AS _rls_j<i> ON …] WHERE _rls_j0.<to0> IS NOT NULL AND
_rls_j<N>.<col> <op> <value>)`, AND-combined inside the same `_rls_src` wrapper and
alias. Equivalent to the correlated `EXISTS` in this positive position: a NULL outer
key matches nothing in both; the guard makes it independent of `transform_null_in`.
Non-gated dialects keep the `EXISTS` byte-for-byte; `scope_check.allow_rls_correlation`
stays for them.

**D3 — Policy path loses its ClickHouse machinery.** Deleted: the
`on_correlated_emitted` parameter and callback, `_clickhouse_correlated_guard`,
`_policy_has_join_rules` and its preflight branch, and the preflight calls that exist
only for policies (refresh-scan, `get_column_types`, `_side_stats`).

**D4 — One server profile, owned by the client.** A Pydantic `ServerProfile` in
`dialects/base.py` with optional fields: version `(major, minor)`, readonly level,
correlated-subqueries-enabled. Hooks on `SqlDialect` (defaults: no probe, empty
profile), replacing DEV-1977's `timeout_permission_sql` / `timeout_permitted(value)`:
- `server_profile_sql()` — ClickHouse: `SELECT version(), getSetting('readonly')`;
  failure propagates (as DEV-1977's permission check does).
- `correlated_setting_sql(profile)` — ClickHouse on 25.4+: `SELECT
  getSetting('allow_experimental_correlated_subqueries')`, else `None`; failure leaves
  the field unknown (timeouts unaffected; the semi-join gate fails closed). Two
  statements instead of one `system.settings` read so hardened servers
  (`select_from_system_db_requires_grant`) cannot break the timeout path (Codex).
- `parse_server_profile(...)` — tolerant: NULL, `""`, `"0"`/`"1"`, ints, malformed
  version and missing columns normalise to unknown, never raise; absorbs
  `_parse_clickhouse_version`.
- `timeout_permitted(profile)` — ClickHouse: `readonly != 1`.
- `correlated_subquery_refusal(profile) -> str | None` — ClickHouse: `None` iff version
  ≥ 25.4 and (setting on, or readonly known and ≠ 1); otherwise the reason text
  (version below 25.4 / undeterminable; setting off under `readonly = 1`, remedies:
  enable in profile, `readonly = 2`, and upgrade to ≥ 25.8 only when version < 25.8).

The client's lock-guarded `WeakKeyDictionary` becomes `[Engine, ServerProfile]`, filled
on first use by one helper per path (sync/async) that runs the hooks' statements via
`_exec_verbatim` / `_exec_verbatim_async` on the checked-out connection — never
through `execute()`, which would re-enter the timeout path under the same lock
(Codex). A new async `SlayerSQLClient.server_profile()` opens a connection and fills /
returns it for the engine. A failed fill is not cached. The engine's
`_ch_version_cache`, `_preflight_clickhouse_correlated` and its `SELECT version()`
probe are deleted.

**D5 — Deterministic SQL.** When allowed, the setting is attached as today, even if
already on (`readonly = 1` accepts the same value), so rendered SQL and the cache key
never depend on server state.

**D6 — ClickHouse parts live in `dialects/clickhouse.py`; attach as AST.**
`_attach_ch_correlated_setting` and `_settings_holder` move from `session_policy.py`,
`_parse_clickhouse_version` from the engine, via the deterministic-refactor skill (rope
updates every import, tests included). A dialect hook attaches the setting to a
statement AST (no-op by default). `plan_has_semi_join_filters` moves from
`engine/query_engine.py` to `slayer/ir` (same skill), and `generate_planned_stages`
calls the attach hook on the statement AST before `_finish_statement` when any plan
carries semi-join filters and the dialect is gated — deleting the engine's re-parse of
rendered SQL (§3.1). The engine keeps one dialect-agnostic step in `_prepare`: for a
semi-join plan on a gated dialect, `await client.server_profile()` and raise
`SlayerError` (naming the pushed filters) on a refusal; a profile failure raises the
existing "version could not be determined" error.

## Risks / Trade-offs

- [Engines differ in how they plan an uncorrelated `IN`] → only ClickHouse gets it,
  whose native `IN` builds a hash set once per query.
- [The profile is cached per SQLAlchemy engine; a changed user level is seen only after
  the engine is rebuilt] → same as DEV-1977's accepted trade-off.
- [One extra round trip per engine on ClickHouse 25.4+] → once per engine lifetime.
- [The 25.4 live container adds CI time to the ClickHouse workflow] → module-scoped,
  only in the path-gated ClickHouse workflow.

## Migration Plan

No data migration. `SlayerSQLClient.server_profile()` is internal API.
