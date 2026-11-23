## Context

See proposal.md — Why. `origin/1.x` is already fully merged into `main`; the only `1.x` work missing is #611 (3 commits on `origin/egor/dev-2377-otel-datasources-created-with-ingestion-cli`: plan `3df6a280e`, tests `79bac0950`, implementation `944b82391`), still open and unarchived when this plan was written.

On `main` the create paths have #611's "before" shape in `main`'s vocabulary: `_create_ingest_and_report` + `_persist_ingested_datasets` in `slayer/cli.py`, the hand-rolled save in `create_datasource` (`slayer/mcp/server.py`), both calling `ingest_datasource_report_async` from `slayer/live_schema/ingestion.py`; `slayer/engine/ingestion.py` is a `raise_moved` stub. `main` alone reports failed key reads on every ingest surface (`models/fk-ingestion`, `IdempotentIngestResult.key_read_failures`, rendered by the CLI `_print_ingest_drift_and_errors` and MCP `_render_ingest_result`). `main`'s ingestion axiom lives in `architecture/python.live_schema.arc42.md` §3.1, not `system.arc42.md` §3.14 as on `1.x`; `main`'s "One database door" likewise lives in its owner node's file (`python.database.arc42.md` §3.1). The `surfaces` / `protocols` bucket packages in `architecture/model.c4` are identical on both lines.

CI's forward-merge guard (`scripts/forward_merge_specs.py`) requires every requirement the merged `1.x` range adds or modifies to survive verbatim on `main` (body hash including scenarios) or be listed in `openspec/forward-merge-ledger.yaml`; a ledger entry whose `(title, one_x_hash)` matches no `1.x` requirement at the merged `1.x` point fails as stale.

Applicable principles: `python.live_schema.arc42.md` §3.1 (idempotent, additive-only ingestion — restored on the create paths; gains "One ingest door"); `python.database.arc42.md` §3.1 (one database door — unchanged); `python.otel.arc42.md` §3.5 (observing changes nothing) and §3.6 (one way in — `surfaces → otel` exists; no new import edge); `system.arc42.md` §3 2 (model truth) and 5 (async-first).

Structural cause: a second, hand-rolled ingest pipeline beside the idempotent pass (#611 design decision 1). Fix: delete it on `main` too — one persisting pipeline, so the probe cannot be missed — and enforce it with the ingest-door law.

## Goals / Non-Goals

**Goals:**
- `main` contains every `1.x` commit; the create surfaces on `main` run the idempotent ingest, in `main`'s layout and vocabulary.
- Each behaviour change has a test on `main` that fails without it; every surface named by the ingest span-tree requirement has an OTel test.

**Non-Goals:**
- Any behaviour beyond #611's, except the `main`-only key-read-failure adaptations (decisions 6, 7).
- Changing `architecture/system.arc42.md` or `architecture/*.c4`.
- Editing #611 from this branch (decision 10).

## Decisions

1. **Merge #611's head now; merge `origin/1.x` again once #611 is merged and archived; the PR opens only after the second merge.** Options: A head now + second merge / B plan now, merge only after #611 lands. Taken: A — as DEV-2365; late #611 fixes and the archive arrive with the second merge. (user)
2. **Conflict rule: #611's behaviour, `main`'s layout, vocabulary and tool names** (datasets / fields / metrics, `slayer/live_schema/ingestion.py`, MCP `ingest_datasource` / `datasets_summary`). (default)
3. **#611's requirements are carried by this change's delta in `main`'s wording; `openspec/specs/**` resolve to `main`'s side at both merges**, as DEV-2365. (default)
4. **Ledger:** one entry per #611 requirement `main` words differently — "An ingest is one span tree with its outcome" (datasets), "datasources create --ingest runs the idempotent ingest" (datasets, key-read-failure section), "The empty-ingest message needs an empty result" (datasets, failed key reads), "create_datasource auto-ingest runs the idempotent ingest" (`ingest_datasource`, datasets, failed key reads). "Which CLI, Flight and Postgres calls open a request span" survives verbatim (decision 10). The existing "An ingest is one span tree with its outcome" entry (`f0bbc12a…`) is **replaced** by the new `1.x` hash, since the guard fails stale entries. (default)
5. **"One ingest door" goes under `python.live_schema.arc42.md` §3.1, not `system.arc42.md`**: `main`'s home of the ingestion axiom, matching `main`'s placement of "One database door". `system.arc42.md` resolves to `main`'s. The clause, appended directly under §3.1's existing text, which keeps its `[review]` tag, (user, approved wording):
   ```
      **One ingest door**: surfaces ingest only through the idempotent pass.
      [enforced: test:tests/test_law_ingest_door.py]
   ```
6. **The shared emptiness check also counts `key_read_failures`**: `main` reports them on every ingest surface (`models/fk-ingestion`), and MCP's `_render_ingest_result` already counts them. (default)
7. **MCP create relies on `_render_ingest_result` for the key-read-failure section** and drops its own. (default)
8. **Tests are written on `main` before the merge**, so each new test fails without the fix; at the merge each new test file resolves to its pr-tests version. (default)
9. **The law test's AST fixture sources spell `slayer.live_schema.ingestion`** (`main`'s module); the suffix match works for either spelling. (default)
10. **#611's stray "Dry run" scenario** (its `otel/collection` delta copies "A dry run runs no query"'s scenario into "Which CLI, Flight and Postgres calls open a request span") **is fixed in #611**, recorded as a comment on DEV-2377 for that session; this delta omits it. Options: A fix in #611, mirror verbatim / B leave #611, omit here and ledger. Taken: A. (user)
11. **Demo create OTel test**: `slayer datasources create demo --ingest` yields exactly one `slayer.ingest` span under the `datasources.create` request span, four step children, one duration point, its added-dataset count. The demo takes its own branch (`ingest_demo_datasets`). (codex)
12. **OTel coverage for the other ingesting surfaces** named by the span-tree requirement: REST `/ingest`, MCP `ingest_datasource`, startup ingest over two datasources — exactly one `slayer.ingest` span and one duration point per datasource. Tests only; requirement wording unchanged. Rejected alternative (codex): narrowing the requirement — it would drop text #611 lands on `1.x`. (codex)

## Risks / Trade-offs

- [Git rename detection carries 1.x hunks (`slayer/engine/ingestion.py`) into `slayer/live_schema/ingestion.py` or resurrects the stub's body] → after resolving, `git diff origin/main -- slayer/live_schema/ slayer/engine/ingestion.py`; the only allowed difference is `_ingest_result_is_empty`.
- [Clean auto-merges carry 1.x vocabulary silently (docs, tests)] → `git diff origin/main --stat` after resolving; every path justified against proposal.md's Impact.
- [Main-only tests pin the old create output] → found by the pr-tests run; aligned with per-file user approval.
- [#611 changes again before it merges (review fixes, the Dry run fix)] → the second merge brings them; re-run the gate and re-check the delta against #611's final text.
- [Ledger entries can only be finished after the second merge] → task 4 runs then, before the PR opens.
