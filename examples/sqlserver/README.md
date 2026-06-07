# SLayer + SQL Server Example

Runs SLayer against a SQL Server 2022 database using Docker Compose.

## Prerequisites

- Docker and Docker Compose
- Python 3.11+

## Quick start

```bash
cd examples/sqlserver
docker compose up -d
# Wait ~30 s for SQL Server to be ready and the seed to complete, then:
python verify.py
```

## What it does

1. Starts a SQL Server 2022 container
2. Creates the `slayer_demo` database
3. Seeds it with the shared e-commerce dataset (regions, customers, products, orders)
4. Starts a SLayer API server on port 5143

## Notes

- SQL Server 2022 is required — `DATETRUNC` (used for time-dimension truncation) was added in 2022.
- `median` and `percentile` are not supported on T-SQL; SLayer raises `NotImplementedError` for those.
- `corr`, `covar_samp`, and `covar_pop` use a variance-decomposition formula (no native T-SQL equivalent).
- The `Dockerfile` in this directory extends the standard SLayer image with `msodbcsql18` (Microsoft ODBC Driver 18).
