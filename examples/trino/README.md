# Trino + Docker Compose Example

Run SLayer with a Trino database using Docker Compose.

## Quick Start

```bash
cd examples/trino
docker compose up -d
```

This starts:
- **Trino** (port 8080) with sample e-commerce data in the `memory` catalog
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
  -d '{"source_model": "orders", "measures": ["count(*)"], "dimensions": ["status"]}'
```

## Notes

- The `memory` catalog lives in the Trino server's RAM: restarting the `trino` container drops the data, so re-run `docker compose up -d seed` afterwards.
- The `memory` connector has no primary or foreign keys, so no joins are auto-generated during ingestion.
- `median` and `percentile` run as Trino's `approx_percentile`, so they return an approximate value.

## Clean Up

```bash
docker compose down -v
```
