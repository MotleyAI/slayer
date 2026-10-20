# Tasks

Gate: group 6 needs `living-architecture@0.3.1` on npm (`npm view living-architecture@0.3.1 version` succeeds). Until then do groups 1–5 and 7; never work around the gate in CI. Every edit to `architecture/*.arc42.md`, `model.c4`, `views.c4` needs the user's OK on the exact diff at landing time. Codex review is off locally (`living-architecture.local.yaml`).

## 1. Typed REST responses

- [x] 1.1 Pin current JSON bodies of `/health`, `GET /models`, `GET /models/{name}`, `POST /models`, `DELETE /models/{name}`, `GET /datasources`, `POST /datasources`, `GET`/`PUT /datasources/priority`, `POST /memories`, `DELETE /memories/{memory_id}`, `/search`, `/inspect`, `/recommend-root-model` in tests; verify they pass before any route change
- [x] 1.2 Add Pydantic v2 response models in `slayer/api` (`HealthResponse`, `ModelSummary`, `DatasourceSummary`, `DatasourcePriority`, `InspectResponse`, status responses); declare `response_model` on those routes reusing `SearchResponse`, `RootModelRecommendation`, `SaveMemoryResponse`, `ForgetMemoryResponse`; `GET /models/{name}` via `responses=` only; verify 1.1 tests still pass and an OpenAPI test finds a named schema for every listed route
- [x] 1.3 Regenerate `ts-sdk/src/schema.ts` (`npm --prefix ts-sdk run gen`); verify `npm --prefix ts-sdk run check:schema` is clean after commit

## 2. Parity harness

- [x] 2.1 Write `tests/client_parity.json`: operations (python, ts, method, path), `python_only` (`*_sync`, `query_df`), per-language call args + expected request for every operation and the spec scenarios (name+refine, stage list, variables merge, flags omitted, search/inspect defaults, encoded model name, token + headers, data_source query param), and a fixed-clock token-signing case; verify it parses and every operation has ≥1 case
- [x] 2.2 Write `tests/test_client_parity.py` (method set = manifest; replay cases on a mocked httpx; query bodies compared after normalising each query/stage through `SlayerQuery`); verify it fails against the current client for the expected reasons
- [x] 2.3 Write `ts-sdk/test/parity.test.ts` (method set = manifest; replay on mocked `fetch`; exact JSON) and `ts-sdk/test/portability.test.ts` (no `node:` import outside `src/node.ts`; no `dependencies` in package.json); verify parity fails against the current client and portability passes

## 3. Python client

- [x] 3.1 Constructor `token=`, `headers=`, `timeout=60.0` (httpx gets it explicitly; `None` disables); token str or sync/async callable resolved per request (sync transport via `run_sync`); local mode + `token`/`headers` raises `ValueError`; verify unit tests for each, incl. default 60 s passed to httpx
- [x] 3.2 `with_token`, `health`, `delete_model(name, data_source=None)` (local mode via `storage.delete_model`); percent-encode model names in paths; remote `_parse_response` keeps `warnings`; honest return annotations; verify unit tests and `poetry run basedpyright` shows no new errors
- [x] 3.3 `sign_gateway_token(claims, secret, ttl_seconds=3600)` in `slayer.client` (stdlib only, ≥32-byte secret, injectable clock, TS-identical JSON/key order); verify unit tests and the parity signing case
- [x] 3.4 Verify `tests/test_client_parity.py` passes; update `docs/getting-started/python.md` (token, headers, timeout incl. the 5 s → 60 s change, `with_token`, `delete_model`, `health`, `sign_gateway_token`) and verify its snippets run

## 4. TypeScript client

- [x] 4.1 Options `{ url = "http://localhost:5143", token, headers, timeoutMs = 60_000 | null, fetch }`; `query(q: Query | Query[] | string, { refine, variables, dryRun, explain, signal })` with Python's body rules (refine on non-string throws `TypeError`); `sql`, `explain`; `SlayerResponse<T>` from generated `QueryResponse`; remove `queryStages`, `run`; verify unit tests
- [x] 4.2 `listModels({ dataSource })`, `getModel(name, { dataSource })`, `createModel` (POST), `deleteModel(name, { dataSource })`, `listDatasources`, `createDatasource`, `getDatasourcePriority`, `setDatasourcePriority`, `saveMemory`, `forgetMemory`, typed `search` / `inspect` (returns `result` string), `recommendRootModel(items, { dataSource, rootHint })`, `health`, `withToken`; remove `saveModel`; types from `schema.ts`; verify unit tests
- [x] 4.3 Verify `parity.test.ts` passes, `npm --prefix ts-sdk run typecheck` and `build` succeed; update `test/gateway.e2e.test.ts` and `examples/express.ts` to the new API
- [x] 4.4 Rewrite `ts-sdk/README.md`; add `docs/getting-started/typescript.md` and its `zensical.toml` nav entry; grep `docs/` for `baseUrl`, `queryStages`, `run(`, `saveModel`, `QueryResult`; verify no stale hits

## 5. CI

- [x] 5.1 Move `la-arch-check` into its own always-run job in `.github/workflows/ci.yml` with SHA-pinned `actions/setup-node`; add `tests/client_parity.json` to the `ts-sdk` job's path filter; verify the YAML parses and job conditions read correctly

## 6. Living architecture (gated on the npm twin)

- [ ] 6.1 `living-architecture.yaml`: `commands.typecheck.typescript: tsc --noEmit -p ts-sdk/tsconfig.json`, `commands.test` chains `npm --prefix ts-sdk test`; verify `la-doctor` ok
- [ ] 6.2 `architecture/index.yaml`: `typescript: {root_package: src, source_root: ts-sdk, tsconfig: ts-sdk/tsconfig.json}`, `cross_cutting_specs.clients: {touches: [python.surfaces, typescript.client]}`, `diagrams` entry for `ts-sdk.arc42.md: [ts_sdk]`
- [ ] 6.3 With user OK on the exact diff: `model.c4` `typescript` root with `client` (package `src/client`, claims `src/errors`, `src/types`, `src/schema`, `src/index`) and `nodejs` (package `src/node`), no arrow; `views.c4` `view ts_sdk of typescript`
- [ ] 6.4 With user OK on the exact diff: `system.arc42.md` minimal edits (§2 second root, §3.3 `ts-sdk/src` units, §5 language-root wording); new `architecture/ts-sdk.arc42.md` (purpose, `likec4:ts_sdk` marker, principles: mirrors the Python client [enforced: both parity tests], portable entry [enforced: arch_check:model-truth + portability test], no runtime deps [enforced: portability test], generated wire types [review], rationale linking this change)
- [ ] 6.5 Run `la-arch-diagrams`; verify `poetry run la-arch-check` and `npx -y likec4@1.47.0 validate architecture` pass
- [ ] 6.6 Write the TS `la-typecheck` baseline; if 0.3.1 cannot ratchet TypeScript (DEV-2026), STOP and ask the user

## 7. Integration checks

- [ ] 7.1 Verify `poetry run pytest -m "not integration"`, `npm --prefix ts-sdk test`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors) all pass

## Workflow follow-up

- Close PR #499 (superseded by this change) once this PR is open.
- Archive this OpenSpec change in the pr-review stage.
