# MySQL + Docker Compose Example

Run SLayer with a MySQL database using Docker Compose.

## Quick Start

```bash
cd examples/mysql
docker compose up -d
```

This starts:
- **MySQL 8** (port 3307) with sample e-commerce data
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
  -d '{"model": "orders", "fields": [{"formula": "*:count"}], "dimensions": [{"name": "status"}]}'
```

## Clean Up

```bash
docker compose down -v
```
