# BigQuery Example — `bigquery-public-data.thelook_ecommerce`

Run SLayer against a real BigQuery dataset. No Docker — BigQuery is a managed
service, so the example just points SLayer at the public dataset and starts the
API server.

## Prerequisites

- A Google Cloud project (any project; it's only used for billing the BQ jobs).
- A service account in that project with the `roles/bigquery.jobUser` role.
  - The dataset (`bigquery-public-data.thelook_ecommerce`) is world-readable;
    no extra grant is needed on it.
- A JSON key file for that service account.

## Configure auth

SLayer reads BigQuery credentials via Google Application Default Credentials,
the same as every other Google client library. Set two env vars:

```bash
export GCP_PROJECT_ID="your-billing-project-id"
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/sa-key.json"
```

The datasource YAML (`slayer_data/datasources/thelook.yaml`) interpolates
`$GCP_PROJECT_ID` into the connection string; the `google-cloud-bigquery`
client auto-picks up `$GOOGLE_APPLICATION_CREDENTIALS` at query time.

> **Never commit your JSON key.** The `.gitignore` in this directory blocks
> `*.json` to make accidents loud.

## Install the BigQuery extra

```bash
poetry install -E bigquery
# or
pip install "motley-slayer[bigquery]"
```

This pulls in `sqlalchemy-bigquery` + `google-cloud-bigquery`.

## Start the server

```bash
cd examples/bigquery
./start.sh
```

## Verify

In another terminal:

```bash
python examples/bigquery/verify.py
```

The verify script runs identity-style checks (sum-of-grouped == ungrouped
total, min ≤ avg ≤ max, etc.) rather than hardcoded row counts — the public
dataset isn't strictly frozen.

## Models

Four hand-authored models map the thelook tables:

| Model         | Backing table                                          |
|---------------|--------------------------------------------------------|
| `orders`      | `bigquery-public-data.thelook_ecommerce.orders`        |
| `order_items` | `bigquery-public-data.thelook_ecommerce.order_items`   |
| `products`    | `bigquery-public-data.thelook_ecommerce.products`      |
| `users`       | `bigquery-public-data.thelook_ecommerce.users`         |

Joins are declared explicitly (BigQuery has no FK metadata, so `slayer ingest`
auto-resolution cannot derive them).

## Why not `slayer ingest`?

`sqlalchemy-bigquery`'s Inspector lets you introspect schemas inside your
billing project. Cross-project public datasets aren't introspectable that way
— the project in the connection URL is fixed. For your own datasets in your
billing project, `slayer ingest` works fine; see [docs/concepts/ingestion.md].

## Try a query

```bash
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{
        "source_model": "order_items",
        "measures": ["*:count", "sale_price:sum"],
        "dimensions": ["products.category"],
        "order": [{"column": "count", "direction": "desc"}],
        "limit": 5
      }'
```
