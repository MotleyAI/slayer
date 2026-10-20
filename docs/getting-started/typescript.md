# TypeScript SDK

`@motley-ai/slayer` is a TypeScript client for a running SLayer server or the [SLayer Gateway](https://github.com/MotleyAI/slayer-custom). It offers the same operations as the [Python client](python.md), sends the same requests, and has no runtime dependencies: it uses the built-in `fetch`, so it runs on Node 20.3+, Deno, Bun, browsers and edge runtimes.

## Install

```bash
npm install @motley-ai/slayer
```

## Query

```ts
import { SlayerClient } from "@motley-ai/slayer";

const slayer = new SlayerClient({ url: "http://localhost:5143" });

const result = await slayer.query<{ "orders.status": string; "orders._count": number }>({
  source_model: "orders",
  measures: ["count(*)"],
  dimensions: ["status"],
});
console.log(result.row_count, result.data);
```

`query` takes the same three shapes as the Python client:

```ts
await slayer.query({ source_model: "orders", measures: ["revenue"] });          // one query
await slayer.query([                                                               // stages; the last is the result
  { name: "by_customer", source_model: "orders", measures: ["sum(amount)"], dimensions: ["customer_id"] },
  { source_model: "by_customer", measures: ["avg(amount_sum)"] },
]);
await slayer.query("rev_by_region", { refine: { dimensions: ["status"] } });      // a saved query-backed model
```

Options: `refine` (saved queries only), `variables` (override the query's own), `dryRun`, `explain` and an abort `signal`. `sql(query)` returns the generated SQL without running it, and `explain(query)` runs the database's EXPLAIN.

## Naming

SDK parameters are camelCase (`{ dryRun: true }`, `{ dataSource: "mydb" }`). SLayer data keeps its own field names, both in queries and models you pass in (`source_model`, `time_dimensions`) and in responses (`row_count`, `population_inferred`). So query examples from the rest of these docs paste in unchanged.

## Connection settings

```ts
const slayer = new SlayerClient({
  url: "https://slayer-gateway.example.com",   // default "http://localhost:5143"
  token: async () => getAccessToken(),         // or a string; called for every request
  headers: { "X-Request-Id": "abc" },
  timeoutMs: 60_000,                           // the default; null turns it off
});

// Same settings, another token: create one client and swap the token per user
const asUser = slayer.withToken(req.userToken);
```

For local development and service accounts, `signGatewayToken` (Node only) signs an HS256 token for the gateway's `auth.hs256_secret`:

```ts
import { signGatewayToken } from "@motley-ai/slayer/node";

const token = signGatewayToken({ sub: "report-job", tenant: "acme", roles: ["analyst"] }, process.env.GATEWAY_JWT_SECRET!);
```

## Other methods

| TypeScript | Python | Endpoint |
|---|---|---|
| `listModels({ dataSource })`, `getModel(name, { dataSource })` | `list_models`, `get_model` | `GET /models`, `GET /models/{name}` |
| `createModel(model)`, `deleteModel(name, { dataSource })` | `create_model`, `delete_model` | `POST /models`, `DELETE /models/{name}` |
| `listDatasources()`, `createDatasource(config)` | `list_datasources`, `create_datasource` | `GET`/`POST /datasources` |
| `getDatasourcePriority()`, `setDatasourcePriority(names)` | `get_datasource_priority`, `set_datasource_priority` | `GET`/`PUT /datasources/priority` |
| `saveMemory({ learning, linkedEntities, id, description })`, `forgetMemory(id)` | `save_memory`, `forget_memory` | `POST /memories`, `DELETE /memories/{id}` |
| `search({ question, entities, query, datasource, maxResults, cypherFilter, compact })` | `search` | `POST /search` |
| `inspect({ entityType, reference, compact, format, numRows, showSql, sections, descriptionsMaxChars })` | `inspect` | `POST /inspect` |
| `recommendRootModel(items, { dataSource, rootHint })` | `recommend_root_model` | `POST /recommend-root-model` |
| `health()` | `health` | `GET /health` |

Response types are generated from SLayer's OpenAPI schema. `inspect` returns its text directly, as in Python.

## Errors

A non-2xx response throws `SlayerApiError`, with `status`, `detail`, `isUnauthorized` (401) and `isForbidden` (403). A timeout rejects with a `TimeoutError`.

## Not included

The Python client's synchronous variants, `query_df` and local mode (`storage=`) have no TypeScript equivalent. For AI agents, connect an MCP client such as `@modelcontextprotocol/sdk` to the server's `/mcp/sse` endpoint instead.
