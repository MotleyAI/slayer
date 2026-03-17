# ClickHouse + Docker Compose Example

Run SLayer with a ClickHouse database using Docker Compose.

## Quick Start

```bash
cd examples/clickhouse
docker compose up -d
```

This starts:
- **ClickHouse** (HTTP port 8123, native port 9001) with sample e-commerce data
- **SLayer API** (port 5143) with auto-ingested models

## Verify

```bash
python verify.py
```

Runs assertions against the seeded data to validate SLayer is working correctly.

## Try Yourself

```bash
# List models
curl http://localhost:5143/models

# Query: orders by status
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{"model": "orders", "fields": [{"formula": "count"}], "dimensions": [{"name": "status"}]}'
```

## Notes

ClickHouse does not support foreign key constraints, so rollup joins are not auto-generated during ingestion. Models are created as simple table references. You can manually define rollup queries using the `sql` field in model definitions.

## Clean Up

```bash
docker compose down -v
```
