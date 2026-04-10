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

Ingest models from the schema:

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
    "fields": [{"formula": "count"}],
    "dimensions": [{"name": "status"}]
  }'
```

Response:

```json
{
  "data": [
    {"orders.status": "completed", "orders.count": 42},
    {"orders.status": "pending", "orders.count": 15}
  ],
  "columns": ["orders.status", "orders.count"],
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
    "fields": [{"formula": "revenue_sum"}],
    "time_dimensions": [{"dimension": {"name": "created_at"}, "granularity": "month", "date_range": ["2024-01-01", "2024-12-31"]}]
  }'

# Top 5 customers
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{
    "source_model": "orders",
    "fields": [{"formula": "revenue_sum"}],
    "dimensions": [{"name": "customers.name"}],
    "order": [{"column": {"name": "revenue_sum"}, "direction": "desc"}],
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

If `/models` returns an empty list, run `slayer ingest --datasource my_pg` first.

## Using from other languages

SLayer is just HTTP + JSON — use any HTTP client:

**JavaScript:**
```javascript
const res = await fetch("http://localhost:5143/query", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({
    source_model: "orders",
    fields: [{formula: "count"}],
    dimensions: [{name: "status"}],
  }),
});
const {data} = await res.json();
```

**Go:**
```go
body := `{"source_model": "orders", "fields": [{"formula": "count"}]}`
resp, _ := http.Post("http://localhost:5143/query", "application/json", strings.NewReader(body))
```

See the [REST API Reference](../reference/rest-api.md) for all endpoints.
