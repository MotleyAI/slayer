# Embedded Example

Self-contained SLayer example using SQLite — no server or Docker needed.

## Run

```bash
cd examples/embedded
python run.py
```

This will:
1. Create a SQLite database with sample e-commerce data
2. Auto-ingest models with rollup joins
3. Run 5 sample queries demonstrating filters, joins, ordering

## Verify

```bash
python verify.py
```

Runs assertions against the seeded data to validate SLayer is working correctly.

## What It Demonstrates

- **Auto-ingestion** from a SQLite database
- **Rollup joins**: querying `orders` grouped by `products.category` or `customers.name`
- **Transitive rollup**: `orders → customers → regions`
- **Filters**: completed orders only
- **Ordering + limit**: top 3 customers
