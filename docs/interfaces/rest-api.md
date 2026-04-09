# REST API

SLayer provides a FastAPI-based REST API on port **5143** by default.

## Start the Server

```bash
slayer serve --models-dir ./slayer_data
slayer serve --host 0.0.0.0 --port 8080 --models-dir ./slayer_data
```

## Endpoints

### Health Check

```
GET /health
```

```bash
curl http://localhost:5143/health
# {"status": "ok"}
```

### Query

```
POST /query
```

```bash
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{
    "source_model": "orders",
    "fields": ["count"],
    "dimensions": ["status"],
    "limit": 10
  }'
```

Response:

```json
{
  "data": [
    {"orders.status": "completed", "orders.count": 42},
    {"orders.status": "pending", "orders.count": 15}
  ],
  "row_count": 2,
  "columns": ["orders.status", "orders.count"]
}
```

### Models

```
GET    /models              # List all models
GET    /models/{name}       # Get model definition
POST   /models              # Create a model
PUT    /models/{name}       # Update a model
DELETE /models/{name}       # Delete a model
```

```bash
# List models
curl http://localhost:5143/models

# Get model definition (hidden dimensions/measures excluded)
curl http://localhost:5143/models/orders

# Create a model
curl -X POST http://localhost:5143/models \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "sql_table": "public.orders", "data_source": "mydb", ...}'
```

### Datasources

```
GET    /datasources              # List all datasources
GET    /datasources/{name}       # Get datasource (credentials masked)
POST   /datasources              # Create a datasource
DELETE /datasources/{name}       # Delete a datasource
```

```bash
# List datasources
curl http://localhost:5143/datasources

# Get datasource (password/connection_string shown as ***)
curl http://localhost:5143/datasources/my_postgres
```

### Ingestion

```
POST /ingest
```

```bash
curl -X POST http://localhost:5143/ingest \
  -H "Content-Type: application/json" \
  -d '{"datasource": "my_postgres", "schema_name": "public"}'
```

Response:

```json
{
  "status": "ingested",
  "models": ["orders", "customers", "products"]
}
```
