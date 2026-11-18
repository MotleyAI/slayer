## Context

See proposal.md (Why) for the motivation.

- **Branches.** The branch starts at `origin/1.x` (`f1db50fb6`), and the PR targets `1.x`.
- **Source.** The source is PR #602, in the state that merges to `main`: the diff between its merge commit and that commit's first parent. This plan was drafted from #602 head `93c4f4f1b`, while #602 was still in review.
- **1.x differs from `main` in three ways that matter here:**
  - **Moved files.** Three files that #602 hooks live elsewhere on 1.x: `slayer/database/client.py` → `slayer/sql/client.py`, and `slayer/live_schema/{ingestion,schema_drift}.py` → `slayer/engine/{ingestion,schema_drift}.py`. Every other hooked file has the same path on both branches.
  - **No client `explain()`.** `query_engine.py` builds the EXPLAIN SQL itself and runs it through `client.execute`.
  - **Sync entry points.** `ingest_datasource_idempotent` and `validate_datasource` are sync `@marked_coroutine_function` shims; they keep the deprecated no-`client` path. Their async work is in `_ingest_idempotent` and `_validate_datasource`.

**Hook points on 1.x:**

| Stage / request | 1.x location |
|---|---|
| `db.execute` (extract `_db_stage_attributes`, result `db.response.returned_rows`) | `SlayerSQLClient._typed_execute`, `slayer/sql/client.py` |
| `db.metadata` | `SlayerSQLClient._metadata` |
| `db.retry` event | `_log_transient_or_raise` |
| `query`, `query.normalize`, `query.prepare`, `query.policy`, `query.run` | `SlayerQueryEngine.execute`, `_normalize_input`, `_prepare_pipeline`, `_apply_policy`, `_run_data_query` |
| `query.cache.get` / `query.cache.put` | `QueryCache.get` / `put` |
| `ingest` (extract + `_ingest_outcome`) | `_ingest_idempotent`, `slayer/engine/ingestion.py` |
| `ingest.merge` | `_run_additive_pass` |
| `ingest.scan.schema` | `_scan_one_schema` (`data_source`) |
| `ingest.embeddings` | `_refresh_datasource_embeddings` (`datasource_name`) |
| `ingest.drift` | `_validate_datasource`, `slayer/engine/schema_drift.py` |
| request: CLI `query` / `ingest` | `_run_query`, `_run_ingest`; `otel.start()` / `otel.shutdown()` in `_run_command`; `slayer otel status` / `slayer otel test` |
| request: REST | `otel.RequestMiddleware` in `create_app` |
| request: MCP | `_counted.call_tool` with `_tool_operation` |
| request: Flight | `_derive_query_schema`, `_execute_full` |
| request: Postgres facade | `PgConnection._run_query` |
| usage vocabulary | `cli:otel.status`, `cli:otel.test`, and the `otel` extra token in `slayer/usage_reporting/payload.py` |

**Architecture principles that apply:**
- system §3.2: `core` imports no other node; `stages.py` is dependency-free.
- §3.3: `slayer.otel` is claimed by exactly one node.
- §3.4 and §3.5: model truth; the `#legacy` baseline stays at 8.
- §3.6 and python.sql §13: SQL stays an AST and executes verbatim, because a stage never alters a statement.
- §3.7: async-first.
- §3.12: Pydantic, never dataclasses.
- §3.17: one database door; the `db.*` stages sit on the door itself.
- Optional-extra imports inside functions carry `# ALLOW(import-not-top): optional extra`, as in #602.

## Goals / Non-Goals

**Goals:**
- 1.x carries #602's behaviour, as specified by the three `otel/*` deltas, with the deviations below.
- Every 1.x deviation from `main` is deliberate, pinned by a test or the docs, and listed on DEV-2358 ("Forward-merge notes (1.x → main)") with its forward-merge resolution.
- The next 1.x → `main` merge stays mechanical: the specs, the architecture model and the diagrams are identical on both branches.

**Non-Goals:**
- Backporting `main`'s client `explain()` / `ExplainStatements` refactor.
- Changing 1.x's ingest or drift entry points; the deprecation shims stay.
- A version bump. 1.x bumps versions in separate chore commits.

## Decisions

1. **Timing against the still-open #602:** plan now from head `93c4f4f1b`; pr-tests starts only after #602 merges, behind a drift gate (task 1.1). Alternatives: pause planning until the merge; implement against the head and re-sync later. Chosen because the issue's precondition holds and planning isn't blocked. (user)
2. **OpenSpec reconciliation:** this change has its own id, with byte-identical copies of #602's three spec deltas. Alternatives: `skip_specs: true`, which leaves 1.x's corpus without OTel and forks `model.c4`; copying the `gh-483-…` change folder, which gives two owners of one id and colliding archives. Chosen because the corpus, the model and the forward-merge all stay consistent. (user)
3. **EXPLAIN span:** on 1.x an EXPLAIN is `slayer.db.execute`. `_db_stage_attributes` reads `sql` only, with no `statements` branch. The 1.x docs' database-span row drops `slayer.db.explain` and notes the 1.x behaviour. Alternatives: a stage-name override parameter in the client; backporting the client `explain()`. Chosen because the spec's wording admits it, and the alternatives change 1.x's client only for a span name. (user)
4. **Forward-merge adjustments** are recorded as a comment on DEV-2358 and kept up to date there. (user)
5. **Architecture edits, approved exactly as presented:**
   - `architecture/model.c4`: `otel = node 'OpenTelemetry'` (`package 'slayer.otel'`, `specs ['otel']`) between `usage_reporting` and `surfaces`; the arrows `protocols -> otel`, `surfaces -> otel` and `otel -> core`.
   - Diagrams regenerated with `la-arch-diagrams`.
   - `system.arc42.md` lines 78–82 ("Eleven nodes: …") replaced by: "Nodes, package claims and spec mapping are node metadata in [model.c4](model.c4); the legacy-arrow baseline is in [index.yaml](index.yaml)." (user, approved wording)
6. **Ingest and drift stages** go on `_ingest_idempotent` and `_validate_datasource`, the async functions that both the client path and the short-lived path reach. A `@staged` on the sync shims would time only the creation of the coroutine. (default)
7. **1.x module paths** in tests and in `examples/otel/README.md` (logger `slayer.sql.client`). The docs' sample `slayer otel status` output shows the 1.x version (`slayer 1.8.5`). (default)
8. **`poetry.lock`** is regenerated on 1.x with `poetry lock`, never copied. (default)
9. **No version bump** in this change. (default)
10. **Port mechanics:**
    - New files are copied from the merge. `docs/reference/opentelemetry.md`, `examples/otel/README.md`, and tests that name a moved module are copied and then adapted (decisions 3 and 7).
    - Files at the same path get #602's added lines, three-way applied.
    - The three moved files get their hooks re-applied by hand.
    (default)
11. **Codex: "the EXPLAIN deviation contradicts the byte-identical spec."** Rejected. `collection/spec.md`'s requirement lists `slayer.db.execute` as an allowed name for a database call. The clause "`slayer.db.explain` included" holds vacuously. Staging EXPLAIN as `db.explain` would double the database spans and the `db.client.operation.duration` points; rewriting the delta would fork the spec from `main`. rejected (codex)
12. **Codex: "no test pins the EXPLAIN deviation."** Folded in as a 1.x-only test (task 2.11), dropped on the forward-merge. (codex)
13. **Codex: "no Flight / Postgres-facade request-span tests."** Folded into this change (task 2.12), because #602 is already in review. The tests apply unchanged on `main` and are kept on the forward-merge. (codex)
14. **Codex: "'copied verbatim' contradicts the 1.x path adaptations."** Port mechanics now lists the adapted copies (decision 10). (codex)
15. **Archived OTel specs equal `main`'s.** #602's deltas have no `## Purpose`, so `openspec archive` would stamp `TBD - created by archiving change <changeName>. …` into each new main spec. The change name differs per branch, which would make the specs conflict on the forward-merge. After archiving on 1.x, `openspec/specs/otel/{collection,controls,delivery}/spec.md` are overwritten with `origin/main`'s copies and checked with `cmp` (task 4.3). Alternatives: add our own `## Purpose` to the deltas, whose wording won't match `main`'s; ask #602 to add one, which still needs the `cmp` check. (user)

## Risks / Trade-offs

- [#602 changes before merge] → Task 1.1's gate compares the merged file list and the three deltas, byte for byte, against this plan. Any difference comes back as an exact plan edit for approval.
- [git rename detection carries 1.x hooks into `main`'s moved files on the forward-merge, staging them twice] → DEV-2358 forward-merge note, item 4.
- [The 1.x trace vocabulary differs from `main` for EXPLAIN] → Documented in the 1.x docs, pinned by task 2.11, and resolved on the forward-merge by DEV-2358 note items 1 and 7.
- [`poetry lock` on 1.x moves unrelated pins] → Lock with Poetry 2's default, which keeps existing pins, and review the lock diff, which should only add the OTel packages and their dependencies.
