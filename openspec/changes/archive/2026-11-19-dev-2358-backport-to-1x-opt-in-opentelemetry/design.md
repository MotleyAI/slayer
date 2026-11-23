## Context

See proposal.md (Why) for the motivation.

- **Branches.** The branch starts at `origin/1.x` (`f1db50fb6`), and the PR targets `1.x`.
- **Source.** The source is PR #602, in the state that merges to `main`: the diff between its merge commit and that commit's first parent.
  - This plan was drafted from #602 head `93c4f4f1b` and revised against head `a25ddf900`.
  - `a25ddf900` merged `main` at `c7e0843d7`, which brought in `main`'s models→datasets rename.
- **1.x differs from `main` in four ways that matter here:**
  - **Moved files.** Three files that #602 hooks live elsewhere on 1.x: `slayer/database/client.py` → `slayer/sql/client.py`, and `slayer/live_schema/{ingestion,schema_drift}.py` → `slayer/engine/{ingestion,schema_drift}.py`. Every other hooked file has the same path on both branches.
  - **No client `explain()`.** `query_engine.py` builds the EXPLAIN SQL itself and runs it through `client.execute`.
  - **Sync entry points.** `ingest_datasource_idempotent` and `validate_datasource` are sync `@marked_coroutine_function` shims; they keep the deprecated no-`client` path. Their async work is in `_ingest_idempotent` and `_validate_datasource`.
  - **Model vocabulary.** 1.x says model where `main` says dataset: `ModelNotFoundError`, `/models/{name}`, `ModelAddition`, `model_name`, and the MCP tools `create_model` etc.

**Hook points on 1.x:**

| Probe / request | 1.x location |
|---|---|
| `db.execute` (extract `_db_probe_attributes`, result `_returned_rows`) | `SlayerSQLClient._typed_execute`, plus a new `_typed_execute_sync` split out of `execute_sync`'s sync-driver path, `slayer/sql/client.py` |
| `db.metadata` | `SlayerSQLClient._metadata` |
| `db.retry` event (`_record_retry`, after the backoff sleep) | `_retry_with_backoff` and `_execute_with_retry_sync` |
| `server.*` / `db.namespace` attributes | the new `SlayerSQLClient.connection_url()` |
| `query`, `query.normalize`, `query.prepare`, `query.policy`, `query.run` | `SlayerQueryEngine.execute`, `_normalize_input`, `_prepare_pipeline`, `_apply_policy`, `_run_data_query` |
| `query.cache.get` / `query.cache.put` | `QueryCache.get` / `put` |
| `ingest` (extract `_ingest_probe_attributes`, result `_ingest_outcome`) | `_ingest_idempotent`, `slayer/engine/ingestion.py` |
| `ingest.merge` | `_run_additive_pass` |
| `ingest.scan.schema` | `_scan_one_schema` (`data_source`) |
| `ingest.embeddings` | `_refresh_datasource_embeddings` (`datasource_name`) |
| `ingest.drift` | `_validate_datasource`, `slayer/engine/schema_drift.py` |
| request: CLI `query` / `ingest` | `_run_query`, `_run_ingest`; `otel.start()` (skipped for the `otel` command) and `otel.shutdown()` in `_run_command`; `slayer otel status` / `slayer otel test` |
| request: REST, with error marking at status ≥500 | `otel.RequestMiddleware` in `create_app` |
| request: MCP | `_counted.call_tool` with `_tool_operation` |
| request: Flight | `_derive_query_schema`, `_execute_full` |
| request: Postgres facade, with error marking on a handled failure | `PgConnection._run_query` |
| usage vocabulary | `cli:otel.status`, `cli:otel.test`, and the `otel` extra token in `slayer/usage_reporting/payload.py` |

**Architecture principles that apply:**
- system §3.2: `core` imports no other node; `probes.py` is dependency-free.
- §3.3: `slayer.otel` is claimed by exactly one node.
- §3.4 and §3.5: model truth; the `#legacy` baseline stays at 8.
- §3.6 and python.sql §13: SQL stays an AST and executes verbatim, because a probe never alters a statement.
- §3.7: async-first.
- §3.12: Pydantic, never dataclasses.
- §3.17: one database door; the `db.*` probes sit on the door itself.
- `python.otel.arc42.md` §3.1–§3.6: off unless asked, fail-silent, no content without the opt-in, metrics never carry content, observing changes nothing, one way in.
- Optional-extra imports inside functions carry `# ALLOW(import-not-top): optional extra`, as in #602.

## Goals / Non-Goals

**Goals:**
- 1.x carries #602's behaviour, as specified by the three `otel/*` deltas, with the deviations below.
- Every 1.x deviation from `main` is deliberate, pinned by a test or the docs, and listed on DEV-2358 ("Forward-merge notes (1.x → main)") with its forward-merge resolution.
- The next 1.x → `main` merge stays mechanical: the architecture model, the diagrams and the OTel code are identical on both branches, apart from the listed deviations.

**Non-Goals:**
- Backporting `main`'s client `explain()` / `ExplainStatements` refactor.
- Changing 1.x's ingest or drift entry points; the deprecation shims stay.
- Adopting `main`'s models→datasets vocabulary beyond the telemetry names (decision 16).
- A version bump. 1.x bumps versions in separate chore commits.

## Decisions

1. **Timing against the still-open #602:** plan now; pr-tests starts only after #602 merges, behind a drift gate (task 1.1). Alternatives: pause planning until the merge; implement against the head and re-sync later. Chosen because the issue's precondition holds and planning isn't blocked. (user)
2. **OpenSpec reconciliation:** this change has its own id and carries #602's three spec deltas.
   - *Revised after `a25ddf900`:* the deltas are #602's merged deltas translated by decision 16, with a `## Purpose` added. They are no longer byte-identical, because #602's deltas now name `main`-only facts (`DatasetNotFoundError`, `/datasets/{name}`).
   - Alternatives: `skip_specs: true`, which leaves 1.x's corpus without OTel; copying the `gh-483-…` change folder, which gives two owners of one id and colliding archives.
   (user)
3. **EXPLAIN span:** on 1.x an EXPLAIN is `slayer.db.execute`. `_db_probe_attributes` reads `sql` only, with no `statements` branch. The 1.x docs' database-span row drops `slayer.db.explain` and notes the 1.x behaviour. Alternatives: a probe-name override parameter in the client; backporting the client `explain()`. Chosen because the spec's wording admits it, and the alternatives change 1.x's client only for a span name. (user)
4. **Forward-merge adjustments** are recorded as a comment on DEV-2358 and kept up to date there. (user)
5. **Architecture edits** (revised after `a25ddf900`; this replaces the earlier approval of an `otel -> core` edge and `specs ['otel']` on the node):
   - `architecture/model.c4`: `probes = node 'Probes'` nested in `core`; `otel = node 'OpenTelemetry'` (`package 'slayer.otel'`) between `usage_reporting` and `surfaces`; the arrows `protocols -> otel`, `surfaces -> otel` and `otel -> core.probes`.
   - `architecture/index.yaml`: `otel: {touches: [python.otel, python.core.probes, python.engine, python.sql, python.surfaces, python.protocols]}` under `cross_cutting_specs`.
   - New `architecture/python.otel.arc42.md`: #602's file verbatim, except its Rationale cites `(`dev-2358-backport-to-1x-opt-in-opentelemetry`)` in place of `(`gh-483-opentelemetry-instrumentation`)`. (user, approved wording)
   - Diagrams regenerated with `la-arch-diagrams`.
   - `system.arc42.md` lines 78–82 ("Eleven nodes: …") replaced by: "Nodes, package claims and spec mapping are node metadata in [model.c4](model.c4); the legacy-arrow baseline is in [index.yaml](index.yaml)." (user, approved wording)
6. **Ingest and drift probes** go on `_ingest_idempotent` and `_validate_datasource`, the async functions that both the client path and the short-lived path reach. A probe on the sync shims would time only the creation of the coroutine. (default)
7. **1.x module paths** in tests and in `examples/otel/README.md` (logger `slayer.sql.client`). The docs' sample `slayer otel status` output shows the 1.x version (`slayer 1.8.5`). (default)
8. **`poetry.lock`** is regenerated on 1.x with `poetry lock`, never copied. (default)
9. **No version bump** in this change. (default)
10. **Port mechanics:**
    - New files are copied from the merge. `docs/reference/opentelemetry.md`, `examples/otel/README.md`, and tests that name a moved module or a `main`-only name are copied and then adapted (decisions 3, 7 and 16).
    - Files at the same path get #602's added lines, three-way applied.
    - The three moved files get their hooks re-applied by hand.
    (default)
11. **Codex: "the EXPLAIN deviation contradicts the spec."** Rejected. `collection/spec.md`'s requirement lists `slayer.db.execute` as an allowed name for a database call. The clause "`slayer.db.explain` included" holds vacuously. Probing EXPLAIN as `db.explain` would double the database spans and the `db.client.operation.duration` points. rejected (codex)
12. **Codex: "no test pins the EXPLAIN deviation."** Folded in as a 1.x-only test (task 2.11), dropped on the forward-merge. (codex)
13. **Codex: "no Flight / Postgres-facade request-span tests."** Folded into this change (task 2.12), because #602 is already in review; #602 tests only the Postgres-facade failure marking. The tests apply unchanged on `main` and are kept on the forward-merge. (codex)
14. **Codex: "'copied verbatim' contradicts the 1.x path adaptations."** Port mechanics now lists the adapted copies (decision 10). (codex)
15. **Archived OTel specs** (revised after `a25ddf900`). #602's deltas have no `## Purpose`, and they now differ from 1.x's in vocabulary, so 1.x's archived `openspec/specs/otel/*` can't equal `main`'s. Our deltas carry their own `## Purpose`, so archiving stamps no `TBD` placeholder. On the forward-merge, the spec files take `main`'s version. The earlier overwrite-with-`main`'s-copy step is dropped, because it would put `main`-only names into 1.x's corpus. (user)
16. **Vocabulary on 1.x.**
    - Names that 1.x's code fixes use 1.x vocabulary: `ModelNotFoundError`, the route `/models/{name}`, the MCP tool names, and spec prose ("model").
    - The telemetry names SLayer chooses are `main`'s: the ingest outcome attributes `slayer.ingest.datasets.added/.updated/.skipped` and the counter `slayer.ingest.datasets`. Operators' dashboards and alerts then survive the upgrade to 2.0.
    - The delta translation, applied to #602's `collection/spec.md`, is exactly:
      - `/datasets/orders` → `/models/orders`
      - `slayer.operation=/datasets/{name}` → `slayer.operation=/models/{name}`
      - "existing datasets with a reported change" → "existing models with a reported change"
      - "Ingest of one changed dataset" → "Ingest of one changed model"
      - "updates one dataset" → "updates one model"
      - "Unknown dataset" → "Unknown model"
      - "names a dataset that does not exist" → "names a model that does not exist"
      - `DatasetNotFoundError` → `ModelNotFoundError`
      - "count ingested datasets in" → "count ingested models in"

      `controls` and `delivery` need no translation.
    - The 1.x docs add one sentence: the ingest metric names use the 2.0 term "dataset" for a 1.x model.

    Alternative: 1.x names (`slayer.ingest.models.*`). Rejected because every 1.x operator's ingest dashboards would break at the 2.0 upgrade. (user)
17. **Bundled #602 fixes are ported** with 1.x names, because they belong to the merged state (decision 1):
    - `outside_stacklevel()` in `slayer/core/deprecation.py`, used at every fixed-`stacklevel` warning site that a probed call reaches;
    - `ModelAddition.metadata_changed` and `has_changes`, `+key metadata` in `_updated_detail_lines`, and the MCP renderer using them in place of `_addition_has_changes` / `_addition_update_details`;
    - the Flight translation-failure log;
    - the v3-migration log template;
    - `requests` in the `otel` extra.
    (default)

## Risks / Trade-offs

- [#602 changes before merge] → Task 1.1's gate checks the merged file list and the three deltas. Each of our deltas must equal `## Purpose` plus #602's delta with decision 16's translation applied, byte for byte. Any difference comes back as an exact plan edit for approval.
- [git rename detection carries 1.x hooks into `main`'s moved files on the forward-merge, probing them twice] → DEV-2358 forward-merge note.
- [The 1.x trace vocabulary differs from `main` for EXPLAIN and for code-fixed names] → Documented in the 1.x docs, pinned by tests, and resolved on the forward-merge by the DEV-2358 note.
- [A fixed-`stacklevel` warning site reached through a probe is missed] → The ported "Warning location unchanged" test, plus an audit of 1.x's fixed-`stacklevel` sites against the probed call paths (task 3.4).
- [`poetry lock` on 1.x moves unrelated pins] → Lock with Poetry 2's default, which keeps existing pins, and review the lock diff, which should only add the OTel packages, `requests` if missing, and their dependencies.
