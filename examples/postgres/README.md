# Postgres + Docker Compose Example

Run SLayer with a real Postgres database using Docker Compose.

## Quick Start

```bash
cd examples/postgres
docker compose up -d
```

This starts:
- **Postgres** (port 5433) with sample e-commerce data
- **SLayer API** (port 5143) with auto-ingested models

## Try It

```bash
# Health check
curl http://localhost:5143/health

# List models
curl http://localhost:5143/models

# Query: orders by status
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{"model": "orders", "fields": [{"formula": "count"}], "dimensions": [{"name": "status"}]}'

# Query: orders by product category (rollup join)
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{"model": "orders", "fields": [{"formula": "count"}], "dimensions": [{"name": "products__category"}]}'

# Query: orders by region (transitive rollup)
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{"model": "orders", "fields": [{"formula": "count"}], "dimensions": [{"name": "regions__name"}]}'
```

## Verify

```bash
python verify.py
```

Runs assertions against the REST API to validate everything works.

## Connect via MCP

You can also connect Claude Code to the Postgres instance directly:

```bash
claude mcp add slayer -- slayer mcp --models-dir ./slayer_data
```

## Clean Up

```bash
docker compose down -v
```
