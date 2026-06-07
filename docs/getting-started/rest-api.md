# REST API Setup — App Developers

Use SLayer from any language via HTTP. No Python needed in your application — just start the server and call the API.

## Install and start

```bash
uv tool install motley-slayer
slayer serve
```

For databases other than SQLite, add the driver extra (see [full list](../configuration/datasources.md#database-drivers)):

```bash
uv tool install 'motley-slayer[postgres]'
```

The API runs at `http://localhost:5143`. An MCP SSE endpoint is also available at `/mcp/sse`.

## Connect a database

Create a datasource config file:

```yaml
# slayer_data/datasources/my_pg.yaml
name: my_pg
type: postgres
host: localhost
port: 5432
database: myapp
username: analyst
password: ${DB_PASSWORD}
```

The recommended one-shot is to start the server with `--ingest-on-startup`, which walks every configured datasource on boot and runs idempotent auto-ingestion before the port opens:

```bash
slayer serve --ingest-on-startup
```

Or ingest manually (re-runnable without restarting the server):

```bash
slayer ingest --datasource my_pg
```

Or do everything via the API:

```bash
# Create datasource
curl -X POST http://localhost:5143/datasources \
  -H "Content-Type: application/json" \
  -d '{"name": "my_pg", "type": "postgres", "host": "localhost", "database": "myapp", "username": "analyst", "password": "secret"}'

# Ingest models
curl -X POST http://localhost:5143/ingest \
  -H "Content-Type: application/json" \
  -d '{"datasource": "my_pg"}'
```

## Query

```bash
# Count orders by status
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{
    "source_model": "orders",
    "measures": ["*:count"],
    "dimensions": ["status"]
  }'
```

Response:

```json
{
  "data": [
    {"orders.status": "completed", "orders._count": 42},
    {"orders.status": "pending", "orders._count": 15}
  ],
  "columns": ["orders.status", "orders._count"],
  "row_count": 2
}
```

## More examples

```bash
# Monthly revenue with date range
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{
    "source_model": "orders",
    "measures": ["revenue:sum"],
    "time_dimensions": [{"dimension": "created_at", "granularity": "month", "date_range": ["2024-01-01", "2024-12-31"]}]
  }'

# Top 5 customers
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{
    "source_model": "orders",
    "measures": ["revenue:sum"],
    "dimensions": ["customers.name"],
    "order": [{"column": "revenue:sum", "direction": "desc"}],
    "limit": 5
  }'

# List models
curl http://localhost:5143/models

# Get model details
curl http://localhost:5143/models/orders
```

## Verify it works

```bash
# Health check
curl http://localhost:5143/health
# {"status": "ok"}

# List models (should return your ingested models)
curl http://localhost:5143/models
```

If `/models` returns an empty list, restart with `slayer serve --ingest-on-startup` or run `slayer ingest --datasource my_pg`.

## Search & memories

`POST /search` returns memories and canonical entity discovery hits in a single flat ranked list, fused across up to three channels (BM25 over memory entity tags + Tantivy full-text + optional dense embeddings via `motley-slayer[advanced_search]` plus a provider API key).

```bash
# Question-driven
curl -X POST http://localhost:5143/search \
  -H "Content-Type: application/json" \
  -d '{"question": "What should I know about returns?", "max_results": 10}'

# Entity-driven; cypher_filter narrows to memories only (naive form, always available)
curl -X POST http://localhost:5143/search \
  -H "Content-Type: application/json" \
  -d '{
    "entities": ["mydb.orders.is_returned"],
    "cypher_filter": "MATCH (n:Memory) RETURN n.id AS id"
  }'

# Persist a learning so the next session inherits it
curl -X POST http://localhost:5143/memories \
  -H "Content-Type: application/json" \
  -d '{
    "learning": "orders.is_returned in {0,1,NULL}; treat NULL as not returned",
    "linked_entities": ["mydb.orders.is_returned"],
    "id": "kb.returns.null-handling"
  }'

# Delete (cascade-strips memory:<id> refs)
curl -X DELETE http://localhost:5143/memories/kb.returns.null-handling
```

Each hit in the response carries a `kind` discriminator (`"memory"` for prior notes, `"datasource"` / `"model"` / `"column"` / `"measure"` / `"aggregation"` for entity discovery hits) and a `score`. `cypher_filter` accepts full openCypher when `advanced_search` is installed (LadybugDB property graph with `Memory` / `Datasource` / `Model` / `ModelColumn` / `Measure` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `JOINS` edges); without the extra, only the naive `MATCH (n:Label) RETURN n.id AS id` kind-filter form is accepted — anything richer returns HTTP 400. See [Search](../concepts/search.md), [Memories](../concepts/memories.md), and the [REST API Reference](../reference/rest-api.md#memories-semantic-search).

## Using from other languages

SLayer is just HTTP + JSON — use any HTTP client:

**JavaScript:**
```javascript
const res = await fetch("http://localhost:5143/query", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({
    source_model: "orders",
    measures: ["*:count"],
    dimensions: ["status"],
  }),
});
const {data} = await res.json();
```

**Go:**
```go
body := `{"source_model": "orders", "measures": ["*:count"]}`
resp, _ := http.Post("http://localhost:5143/query", "application/json", strings.NewReader(body))
```

See the [REST API Reference](../reference/rest-api.md) for all endpoints.
