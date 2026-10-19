# Proposal

## Why

SLayer now ships two clients — the Python `SlayerClient` and the TypeScript SDK (`ts-sdk/`, `@motley-ai/slayer`) — that grew apart: different method sets, option names, defaults, auth support and response shapes. The CTO asked that they look as similar as possible (apart from language idiom), and that the TS code join the living-architecture model so its boundaries are enforced like the Python ones.

## What Changes

- **Living architecture:** add a `typescript` root to the model with two nodes (`client`, `nodejs`) and no arrow between them; a `typescript:` section and a `clients` cross-cutting spec in `index.yaml`; a new `architecture/ts-sdk.arc42.md`; minimal wording edits to `system.arc42.md` for a second language root; TS type-check and test commands in `living-architecture.yaml`; `la-arch-check` in its own always-run CI job.
- **REST API:** every route either client calls declares its response schema in OpenAPI (reusing `SearchResponse`, `RootModelRecommendation`, `SaveMemoryResponse`, `ForgetMemoryResponse`; new small response models for the rest). JSON bodies stay byte-identical.
- **Python client:** `token=` (string or sync/async callable), `headers=`, `timeout=` (default 60 s), `with_token()`, `health()`, `delete_model()`, `sign_gateway_token()`; remote mode passes `warnings` through; model names in paths are percent-encoded; return annotations made honest. **BREAKING (behaviour):** the effective HTTP timeout changes from httpx's implicit 5 s to 60 s.
- **TS SDK:** **BREAKING** (0.1.0, unpublished): `baseUrl` → `url` (default `http://localhost:5143`), `timeoutMs` default 60 000; one `query()` accepting an object, a stage list or a saved name, with `refine`; new `sql`, `explain`, `createModel`, `createDatasource`, `getDatasourcePriority`, `setDatasourcePriority`, `saveMemory`, `forgetMemory`, `recommendRootModel`; typed `search`/`inspect`; `QueryResult` → `SlayerResponse`; `queryStages`, `run`, `saveModel` removed.
- **Parity enforcement:** a shared `tests/client_parity.json` (operations + wire-level request cases) replayed by a pytest and a vitest; a TS portability test.
- **Docs:** Python getting-started updated, new TypeScript getting-started page (in `zensical.toml` nav), SDK README rewritten.

## Capabilities

### New Capabilities
- `clients`: the Python and TypeScript SDK clients' remote behaviour — operation parity, the naming rule, request construction, auth, timeouts, local-mode refusals, typed responses and TS portability.

### Modified Capabilities
<!-- none: REST response bodies are unchanged; only their OpenAPI declaration is added -->

## Impact

- Code: `slayer/client/`, `slayer/api/server.py` (+ response models), `ts-sdk/src/`, `ts-sdk/test/`, `tests/`.
- Architecture: `architecture/model.c4`, `views.c4`, `index.yaml`, `system.arc42.md`, new `ts-sdk.arc42.md`; `living-architecture.yaml`.
- CI: `.github/workflows/ci.yml` (architecture job; `ts-sdk` job paths).
- Dependency gate: `living-architecture@0.3.1` must be published to npm before `la-arch-check` can measure TypeScript.
- Users: Python callers relying on the 5 s timeout; TS 0.1.0 API is reshaped (not yet published).
