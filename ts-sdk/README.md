# @motley-ai/slayer

A TypeScript client for [SLayer](https://github.com/MotleyAI/slayer) and the [SLayer Gateway](https://github.com/MotleyAI/slayer-custom). It mirrors SLayer's Python `SlayerClient`: the same operations, sending the same requests.

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

const slayer = new SlayerClient({ url: "http://slayer-gateway:5143", token: userJwt });

const { data } = await slayer.query<{ "orders.customers.name": string; "orders.revenue": number }>({
  source_model: "orders",
  dimensions: ["customers.name"],
  measures: ["revenue"],
  time_dimensions: [{ dimension: "created_at", granularity: "month", date_range: "last 12 months" }],
  order: [{ column: "revenue", direction: "desc" }],
});
```

`query` also takes a list of stages or the name of a saved query-backed model (`slayer.query("rev_by_region", { refine: { limit: 5 } })`). SDK options are camelCase; queries, models and responses keep SLayer's own field names.

Full guide, with every method and its Python counterpart: [TypeScript SDK docs](https://github.com/MotleyAI/slayer/blob/main/docs/getting-started/typescript.md).

### Tokens

- `token` takes a string, or a function that returns a fresh token (sync or async) for each request.
- `client.withToken(token)` returns a copy with another token. In a backend, create one client and call `slayer.withToken(req.userToken)` per request. The gateway then applies that user's tenant, role, row filters and column rules.
- For local development and service accounts, `signGatewayToken` signs an HS256 token for the gateway's `auth.hs256_secret`:

```ts
import { signGatewayToken } from "@motley-ai/slayer/node";

const token = signGatewayToken({ sub: "report-job", tenant: "acme", roles: ["analyst"] }, process.env.GATEWAY_JWT_SECRET!);
```

### Timeouts and errors

Requests time out after 60 seconds (`timeoutMs`; `null` turns it off). A non-2xx response throws `SlayerApiError`, with `status`, `detail`, `isUnauthorized` (401) and `isForbidden` (403).

### MCP

The gateway also serves MCP over SSE at `/mcp/sse`. For AI agents, connect `@modelcontextprotocol/sdk` with the same bearer token instead of using this client.

## Development

```bash
npm install
npm test                                   # unit and parity tests
SLAYER_E2E_URL=http://localhost:5143 npm test   # plus end-to-end tests against the slayer-custom demo stack
npm run gen                                # regenerate src/schema.ts from SLayer's Python API (needs `poetry install` in the repo root)
npm run build
```

`test/parity.test.ts` and the Python `tests/test_client_parity.py` replay the shared `tests/client_parity.json`. Add an entry there when you add a method to either client.
