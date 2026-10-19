# Design

## Context

See proposal.md — Why. The Python client (`slayer/client/slayer_client.py`, in the `surfaces` bucket) has a remote (httpx) mode and a local-engine mode with different return shapes per mode. The TS SDK (`ts-sdk/`) was written against the gateway and diverged in names, shapes and defaults. Several REST routes return `dict[str, Any]`, so the generated `ts-sdk/src/schema.ts` has no response types for them. `la-arch-check` currently measures Python only; the npm twin it needs for TypeScript (`living-architecture@0.3.1`) is not yet published.

## Goals / Non-Goals

**Goals:**
- One operation set and one wire behaviour across both clients, enforced by tests in both languages.
- TS response types generated from the OpenAPI schema, never hand-copied.
- TS SDK modelled in the living architecture, with the portable/Node-only boundary enforced.

**Non-Goals:**
- Covering REST endpoints neither client has today (`GET`/`DELETE /datasources/{name}`, `/validate-models`, `/ingest`, `PUT /models/{name}`) — later features.
- Unifying the Python client's local- vs remote-mode return shapes (user decision: keep the split; TS mirrors remote mode).
- A caller-supplied HTTP client for Python (TS keeps its `fetch` option as language idiom).
- Retries, caching, MCP client.

## Decisions

1. **Casing rule.** SDK parameters follow the language (TS camelCase options object; Python kwargs); SLayer data in and out keeps wire snake_case. Alternatives: snake_case everywhere (un-idiomatic TS) or camelCase everywhere (mapping layer for nested data and user keys, docs/YAML no longer paste in).
2. **Calling shape in TS.** Required arguments positional; Python's optional and keyword-only arguments go in one trailing options object (`getModel(name, { dataSource })`). Where Python is keyword-only for required args too (`save_memory`, `search`, `inspect`), TS takes one object (`saveMemory({ learning, linkedEntities })`).
3. **Response schemas on routes.** Reuse the Pydantic classes the Python client already validates against (`SearchResponse`, `RootModelRecommendation`, `SaveMemoryResponse`, `ForgetMemoryResponse`) as `response_model`; add small Pydantic v2 models in `slayer/api` for the rest (`HealthResponse`, `ModelSummary`, `DatasourceSummary`, `DatasourcePriority`, `InspectResponse`, status responses). `GET /models/{name}` returns a dump with hidden columns removed and `None`s excluded, which `response_model` re-serialisation would alter, so it declares its schema via `responses=` (`SlayerModel`) instead. FastAPI drops undeclared fields under `response_model`, so each route gets a test pinning its JSON body to the pre-change body. Alternative (hand-written TS interfaces) rejected: two sources of truth that drift silently.
4. **Python remote vs local shapes stay split** (user decision). Annotations become honest unions: `list_models -> list[str] | list[dict[str, Any]]`, `get_model -> SlayerModel | dict[str, Any] | None`, `list_datasources -> list[dict[str, Any]]`. TS mirrors remote mode with generated types.
5. **Auth.** Token = `str | Callable[[], str | Awaitable[str]]` in Python, `string | () => string | Promise<string>` in TS, resolved per request. Python's sync transport resolves an awaitable token through `run_sync`. `with_token` copies `url`, `headers`, `timeout`. Local mode refuses `token`/`headers`/`with_token` with `ValueError`, matching the existing `policy=`-over-HTTP refusal; `timeout` is ignored in local mode (it has a non-`None` default, so it cannot signal intent).
6. **Timeout 60 s in both.** Python passes `timeout=` to httpx explicitly (was httpx's implicit 5 s); TS `timeoutMs` defaults to `60_000`, `null` disables. A per-request `signal` stays TS-only (Python cancels via asyncio).
7. **Query body construction mirrors Python's `_build_query_body`.** String → `{name, refine?}`; array → `{queries}`; object → as given; runtime `variables` merged over a single query's own; `dry_run`/`explain` only when true; `refine` with a non-string query is an error before any request (Python already raises via `resolve_run_input`; TS throws `TypeError`).
8. **Parity enforcement = shared manifest + wire cases.** `tests/client_parity.json` lists operations (Python name, TS name, HTTP method, path template), the Python-only exceptions, and cases giving each language's call arguments and the expected request. `tests/test_client_parity.py` and `ts-sdk/test/parity.test.ts` each check their client's public method set equals the manifest and replay every case against a mocked transport. The Python client normalises single-query bodies through `SlayerQuery` (string shorthand expansion), so the Python side compares query bodies after validating both expected and actual through the server's request model; the TS side compares JSON exactly. Alternatives: regex over `client.ts` (fragile, names only) or review-only (drifts).
9. **Gateway token signing in Python** lives in `slayer.client` using `hmac`/`hashlib`/`base64`/`json` from the standard library. Byte-identity with TS requires the same header and payload JSON: compact separators, key order `alg, typ` and `iat, exp, ...claims`, base64url without padding. Both accept an injectable clock for the parity case.
10. **Architecture model.** `typescript` root with nodes `client` (`src/client`, claims `src/errors`, `src/types`, `src/schema`, `src/index`) and `nodejs` (`src/node`; `node` would clash with the element kind). No arrow between them, so model-truth forbids `client` importing `nodejs`; a vitest closes the gap model-truth cannot see (direct `node:` imports outside `src/node.ts`). The `clients` spec attaches via `cross_cutting_specs` touching `python.surfaces` and `typescript.client`.
11. **CI.** `la-arch-check` moves from the Python test job (skipped on TS-only changes) to its own always-run job with Node (SHA-pinned `actions/setup-node`) so the npm twin can run. The `ts-sdk` job also triggers on `tests/client_parity.json`.

## Risks / Trade-offs

- [npm twin unpublished] → architecture tasks are gated in tasks.md; everything else proceeds first. No CI workaround (user decision).
- [TS `la-typecheck` ratchet may be unsupported in 0.3.1 (DEV-2026 Backlog)] → stop and ask the user if it fails; do not work around.
- [`response_model` silently drops or reorders fields] → per-route body-pinning tests written before the change.
- [Python timeout 5 s → 60 s changes behaviour] → called out in docs/changelog; only the safer direction.
- [Parity cases test requests, not response parsing] → existing unit tests in each language cover parsing.
- [Python normalises query bodies, TS does not] → semantic comparison on the Python side; the server accepts both forms (already exercised by the TS end-to-end tests).

## Migration Plan

TS 0.1.0 is unpublished, so its API reshape needs no migration path. Python additions are backward compatible apart from the timeout default and corrected annotations. Rollback = revert the PR.
