# ClickHouse + Docker Compose Example

Run SLayer with a ClickHouse database using Docker Compose.

## Quick Start

```bash
cd examples/clickhouse
docker compose up -d
```

This starts:
- **ClickHouse** (HTTP port 8123, native port 9001) with sample e-commerce data
- **SLayer API** (port 5143) with auto-ingested datasets

## Verify

```bash
python verify.py
```

Runs assertions against the seeded data to validate SLayer is working correctly.

## Try Yourself

```bash
# List datasets
curl http://localhost:5143/datasets

# Query: orders by status
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{"population": "orders", "metrics": ["count(*)"], "dimensions": ["status"]}'
```

## Notes

ClickHouse does not support foreign key constraints, so rollup relationships are not auto-generated during ingestion. Datasets are created as simple table references. You can manually define rollup queries using the `sql` field in dataset definitions.

## Clean Up

```bash
docker compose down -v
```
