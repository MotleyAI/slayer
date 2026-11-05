## Context

See proposal.md - Why. The 1.6.0 client shim (item 0) is already in the working tree: `adapt_client` / `_CategoryProbeClient` in `slayer/sql/client.py`, applied in `SlayerQueryEngine.client_for`, OSI `collect_metadata` and `validate_datasource(client=...)`, with `tests/test_legacy_client_type_probe.py`. Since 1.6.0 one type-name mapper serves reflection and the probe (DEV-2163), and drift deletes any column whose stored type bucket conflicts with the live one (`_type_buckets_conflict` in `slayer/engine/schema_drift.py`), so every refinement of that mapper turns stored verdicts into deletions.

Principles that apply: system P13 (one finding kind per meaning), P14 (ingestion additive; the list re-type stays the only re-type exception, no new one), P17 (one database door: every probe goes through the engine's client, adapted once), sql P2 (each dialect's 1.6.0 classification lives in its own `dialects/` file), core P5 (broken dependants are structured warnings, also on the result), engine P8 (a re-type finding never edits a model). No arc42 or `.c4` edit.

## Goals / Non-Goals

**Goals:** nothing a user wrote for 1.6.0 is silently broken or deleted by 1.7.0; a broken dependant is named where the break happens.

**Non-Goals:**
- Re-ingest does not re-type scalar columns (would need a new P14 exception and overwrites user types).
- Broken dependants are reported at re-ingest only, not on the drift finding before it.
- The conformance suite is not adapted to 1.6.0 clients.
- A stored aggregation named like a built-in in another case gets no warning (it is unreachable, as in 1.6.0).
- Released-but-documented changes stay as they are and go to the release notes: granularities renamed on load, `Decimal` for declared exact derived columns, JSON list cells, dialect-hook signatures.

## Decisions

1. **A list re-type is saved and its broken dependants reported, not withheld.** (user)
2. **A type mismatch on an existing column is never a delete; it is always a re-type finding.** Options: (A) keep 1.6.0's probe and reflection classifiers per dialect and turn only mismatches they explain into findings; (B) no type-mismatch deletes at all. Taken: B — one rule closing the whole classifier-refinement class, no second type vocabulary, never destroys user metadata over a type verdict; a genuinely retyped column is reported, not auto-removed. (user)
3. **One `retype` finding kind for lists and scalars** (P13); the 1.7.0-only `ListRetypeFinding` / `list_retype` is replaced, no compatibility cost. (default)
4. **Scalar remedy: set the column's type to the live type** (edit the model); re-ingest re-types lists only. (default — the remedy stated in option B)
5. **Dependant scope: measures, derived columns, model filters, saved queries.** The brief named measures and saved queries; derived columns and filters break the same way. (default)
6. **Dependants are found by the drift cascade's reference walk** — the definitions a delete of `M.X` would cascade to (derived chains, measures, filters, cross-model references within the datasource, query-backed models) — never by text search; names follow system P16. (codex)
7. **A dependant is broken iff it plans before the re-type and fails after**, any typed planning error, recorded with its type; planning is a dry run (measure: one-measure query; derived column: one-dimension query; filter: `count(*)` on its model; saved query: run-by-name). (codex)
8. **The check runs before the re-type is saved**, against a read-only model-overlay view of the storage holding the re-typed models; a check that cannot run is reported as unchecked with its reason, never dropped. (codex)
9. **The check borrows the ingest's client**: a non-owning wrapper whose `aclose` / `close` do nothing, so the short-lived engine never closes a caller's client. (codex)
10. **Drift carries live database type names**: `LiveTable` keeps every column's reflected type name and the `sql`-mode probe keeps `ResultColumnType`, so the finding names `live_db_type` when the database reports one. (codex)
11. **Re-type findings use the existing read-set attribution**, with REST / MCP / CLI shape tests; no loader for the old `list_retype` shape (it was never persisted or released). (codex; the backward-loading part rejected (codex))
12. **1.6.0 category parity is exact, quirks included** (e.g. DuckDB `INTEGER[]` → `number`), because the deprecated path exists to answer what 1.6.0 answered. A column the database does not type takes its SLayer type's category: HEAD's value and census typing keep nothing 1.6.0's first-row rule read. (user) Each dialect declares its classification as data; parity fixtures are 1.6.0's own inputs (cursor descriptions with 1.6.0's type codes, value-inferred SQLite rows, unknown and absent codes), run through HEAD's full typed description — where that loses a distinction 1.6.0 made, the dialect's description carries the type name. (default; fixture source per codex) The 1.6.0 classification tables are read only by `type_categories`, which only the tagged deprecated probes call; a structural tripwire test pins both, so the 2.0 removal of the probes cannot leave the tables behind. (user)
13. **Built-in names resolve exactly** in the one lookup (`aggregation_definition`), tested over every spelling, stored measures, saved queries and case variants. (default; coverage per codex)
14. **MONEY / SMALLMONEY are exact numerics, folded into this change** (raised in this session). Coverage: raw names, SQLAlchemy types, cursor descriptions, `lossy_db_type`; executed Postgres; SQL Server executed in its CI integration suite. (default; coverage per codex)
15. **The conformance suite checks the current contract** and is not adapted. (default)

## Risks / Trade-offs

- [A genuinely retyped live column is no longer auto-removed by `--force-clean`] → the re-type finding names it with the remedy; a query failing on it raises `SchemaDriftError` carrying the finding.
- [Planning every dependant twice slows re-ingest on large datasources] → only dependants of columns re-typed this pass are planned, normally none.
- [Exact 1.6.0 parity preserves known 1.6.0 misclassifications] → only on the deprecated path, which is removed in 2.0.

## Migration Plan

None: stored models are not rewritten. Release notes list the `retype` finding (and that `--force-clean` no longer deletes type-mismatched columns), the MONEY exactness, and the 1.6.0 client compatibility.
