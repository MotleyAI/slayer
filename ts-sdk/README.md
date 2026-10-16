# @motley-ai/slayer

A TypeScript client for [SLayer](https://github.com/MotleyAI/slayer) and the [SLayer Gateway](https://github.com/MotleyAI/slayer-custom).

- Zero runtime dependencies. It uses the built-in `fetch` (Node 20.3+, Deno, Bun, browsers, edge runtimes).
- Request and response types are generated from the SLayer OpenAPI schema.
- It works with both the gateway (JWT, roles, tenants) and a plain `slayer serve`.

## Install

```bash
npm install @motley-ai/slayer
```

## Usage

```ts
import { SlayerClient } from "@motley-ai/slayer";

const slayer = new SlayerClient({ baseUrl: "http://slayer-gateway:5143", token: userJwt });

const { data } = await slayer.query<{ "orders.customers.name": string; "orders.revenue": number }>({
  source_model: "orders",
  dimensions: ["customers.name"],
  measures: ["revenue"],
  time_dimensions: [{ dimension: "created_at", granularity: "month", date_range: "last 12 months" }],
  order: [{ column: "revenue", direction: "desc" }],
});
```

### Tokens

- `token` takes a string, or a function that returns a fresh token (sync or async) for each request.
- `client.withToken(token)` returns a copy with another token. In a backend, create one client and call `slayer.withToken(req.userToken)` per request. The gateway then applies that user's tenant, role, row filters and column rules.
- For local development and service accounts, `signGatewayToken` signs an HS256 token for the gateway's `auth.hs256_secret`:

```ts
import { signGatewayToken } from "@motley-ai/slayer/node";

const token = signGatewayToken({ sub: "report-job", tenant: "acme", roles: ["analyst"] }, process.env.GATEWAY_JWT_SECRET!);
```

### Methods

| Method | Endpoint |
|---|---|
| `query(query, { variables, dryRun, explain, signal })` | `POST /query` |
| `queryStages([stage1, stage2])` | `POST /query` (multi-stage) |
| `run(name)` | `POST /query` (saved query-backed model) |
| `listModels(dataSource?)`, `getModel(name)` | `GET /models` |
| `inspect(request)`, `search(request)` | `POST /inspect`, `POST /search` |
| `saveModel(model)`, `deleteModel(name)` | `PUT/DELETE /models/{name}` |
| `listDatasources()` | `GET /datasources` |
| `health()` | `GET /health` |

### Errors

A non-2xx response throws `SlayerApiError`. It has `status`, `detail`, `isUnauthorized` (401) and `isForbidden` (403).

### MCP

The gateway also serves MCP over SSE at `/mcp/sse`. For AI agents, connect `@modelcontextprotocol/sdk` with the same bearer token instead of using this client.

## Development

```bash
npm install
npm test                                   # unit tests
SLAYER_E2E_URL=http://localhost:5143 npm test   # plus end-to-end tests against the slayer-custom demo stack
npm run gen                                # regenerate src/schema.ts from SLayer's Python API (needs `poetry install` in the repo root)
npm run build
```
