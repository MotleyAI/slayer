# MySQL + Docker Compose Example

Run SLayer with a MySQL database using Docker Compose.

## Quick Start

```bash
cd examples/mysql
docker compose up -d
```

This starts:
- **MySQL 8** (port 3307) with sample e-commerce data
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

## Clean Up

```bash
docker compose down -v
```
