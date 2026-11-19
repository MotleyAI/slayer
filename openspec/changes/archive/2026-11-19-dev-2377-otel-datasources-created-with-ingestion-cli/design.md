## Context

See proposal.md — Why. Seven surfaces ingest a datasource. Five already call the idempotent ingest (`_ingest_idempotent`, `slayer/engine/ingestion.py`), which carries the `ingest` probe: `slayer ingest`, MCP `ingest_datasource_models`, REST `/ingest`, `--ingest-on-startup`, and the demo create (`ingest_demo_models`). The other two call the scan (`ingest_datasource_report_async`) and persist on their own: `_create_ingest_and_report` in `slayer/cli.py` and `create_datasource` in `slayer/mcp/server.py`. `origin/main` has the same shape (`slayer/live_schema/ingestion.py`).

Applicable principles: `system.arc42.md` §3.14 (idempotent, additive-only ingestion — restored by this change) and §3.17 (one database door — unchanged); `python.otel.arc42.md` §3.5 (observing changes nothing — no probe changes a result) and §3.6 (one way in — `surfaces → otel` already exists; no new import edge).

## Goals / Non-Goals

**Goals:**
- One persisting ingest pipeline: every surface that ingests runs the idempotent pass, so the `ingest` span, duration and dataset counts cannot be missed or drift.
- A law test that keeps it that way.

**Non-Goals:**
- Preserving a datasource's own config (description, granularities) across `datasources create` / `create_datasource` re-creation: replacing the config is those commands' contract, and the CLI confirms it. Only stored models are protected (§3.14).
- Any probe or behaviour change in `ingest_datasource_report_async` or the library entry points (`engine.ingest`, the module scan functions).
- Choosing the release number.

## Decisions

1. **The bug is a second ingest pipeline; delete it.** Route both create callers through the idempotent ingest instead of adding an `ingest` probe to the create pipeline (the reporter's suggestion) or a shared probed helper. Those alternatives keep two pipelines whose attribute sets must agree by hand, keep the §3.14 overwrite and miss merge / drift / embeddings. Re-creating over stored models now merges additively. (user)
2. **No patch-release constraint.** The issue's "ships as a 1.9.x patch" is dropped; the change targets `1.x`, the version is decided at release (a minor fits the behaviour change), and it reaches `main` via the 1.x → main forward-merge. (user)
3. **CLI `datasources create` opens a request span**, `@otel.request(surface="cli", token="datasources.create")` — the usage token the parser already assigns — rather than leaving `slayer.ingest` a root span on the CLI. A create without `--ingest` gets a short request span too. (user)
4. **One ingest door, enforced.** `tests/test_law_ingest_door.py`, modelled on `tests/test_law_database_door.py`: an AST scan of every module of the `surfaces` bucket (`slayer.api`, `slayer.mcp`, `slayer.cli`, `slayer.client`, `slayer.inspect`, `slayer.demo`, `slayer.__main__`) and the `protocols` bucket (`slayer.flight`, `slayer.pg_facade`, `slayer.facade`) finds no call to `ingest_datasource_report_async`, `ingest_datasource_report`, `ingest_datasource_async` or `ingest_datasource`, whether by attribute or by name, including names imported under an alias (`from … import x as y`). (user) The §3.14 clause, (user, approved wording):
   ```
       **One ingest door**: surfaces ingest only through the idempotent pass.
       [enforced: test:tests/test_law_ingest_door.py]
   ```
   appended under the existing §3.14 text, which keeps its `[review]` tag.
5. **The law also bans `.ingest(` / `.ingest_sync(` attribute calls** in those modules (the engine's scan-only wrappers); none exist today. (codex)
6. **Each create surface reuses its own ingest renderer**: CLI `_print_ingest_addition` + `_print_ingest_drift_and_errors`; MCP `_render_ingest_result`. The `Ingested: X (n columns, m measures)` lines go — ingestion never creates measures. (default)
7. **CLI create exit code**: 0 once the datasource is saved, whatever the result reports; an exception raised by the ingest keeps today's `Ingestion failed: <e>` + exit 1. MCP keeps "Auto-ingestion failed: <friendly>" for database failures and propagates others. (default; failure semantics made explicit by codex)
8. **CLI prompts**: the model-collision prompt and `_persist_ingested_models` are removed (nothing is overwritten); the datasource-overwrite prompt stays. (default)
9. **No probe on `ingest_datasource_report_async`** — `slayer ingest` would export a nested second span. (default)
10. **One full-shape emptiness check** shared by `slayer ingest` and `datasources create --ingest`: the empty-ingest message only when the result reports nothing (no objects, additions, drift, skips, skipped schemas, hidden internals, broken dependents, errors, imported description). Fixes today's `slayer ingest` hiding skipped-schema reasons behind "No tables or views found". `slayer ingest` exit codes unchanged. (codex)
11. **Metric assertions**: the OTel tests assert `slayer.ingest.duration` and `slayer.ingest.datasets` (with `slayer.ingest.change` labels) for CLI create, MCP create and `slayer ingest`, not only spans. (codex)
12. **Rejected (codex): carry the prior datasource description across re-create.** Replacing the datasource config is the create commands' contract (confirmed on the CLI) and today's behaviour; §3.14 protects re-ingested models, not datasource configs. The re-create tests use a user-edited **model** description.

## Risks / Trade-offs

- [Re-create semantics change: a user who re-created to reset models to a fresh scan now gets a merge] → documented in `docs/reference/cli.md` / `docs/reference/mcp.md`; `slayer models delete` / `delete_model` first resets a model.
- [Create now also runs drift validation and the embeddings refresh, adding time on large schemas] → the same cost `slayer ingest` already pays; required for parity.
- [Existing tests pin the old create output (`TestDatasourcesCreateReporting` in `tests/test_ingest_internal_tables.py`, create tests in `tests/test_ingestion_schema_qualification.py`)] → aligned to the plan with per-file user approval in `pr-tests`.
- [Forward-merge to `main`, where ingestion lives in `slayer/live_schema/ingestion.py`] → same call shapes on `main`; the law test's banned names must resolve there too.
