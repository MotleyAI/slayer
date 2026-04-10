# Python Client

The Python SDK supports both **remote mode** (connects to a running server) and **local mode** (no server needed).

## Installation

```bash
pip install motley-slayer[client]   # httpx + pandas
```

## Usage

### Remote Mode

```python
from slayer.client.slayer_client import SlayerClient
from slayer.core.query import SlayerQuery

client = SlayerClient(url="http://localhost:5143")

query = SlayerQuery(
    source_model="orders",
    fields=["*:count", "revenue:sum"],
    dimensions=["status"],
    limit=10,
)

# Get raw data
data = client.query(query)
# [{"orders.status": "completed", "orders._count": 42, ...}, ...]

# Get pandas DataFrame
df = client.query_df(query)
print(df)
```

### Local Mode

No server needed — queries execute directly against the storage backend:

```python
from slayer.client.slayer_client import SlayerClient
from slayer.storage.yaml_storage import YAMLStorage

client = SlayerClient(storage=YAMLStorage(base_dir="./slayer_data"))

# Same query API as remote mode
data = client.query(query)
df = client.query_df(query)
```

### Other Methods

```python
# List models
models = client.list_models()

# Get model definition
model = client.get_model("orders")

# Create a model
client.create_model({"name": "orders", "sql_table": "public.orders", ...})

# List datasources
datasources = client.list_datasources()

# Create a datasource
client.create_datasource({"name": "mydb", "type": "postgres", ...})
```

## Direct Engine Access

For maximum control, use the query engine directly:

```python
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

storage = YAMLStorage(base_dir="./slayer_data")
engine = SlayerQueryEngine(storage=storage)

result = engine.execute(query=query)
# result.data      — list of row dicts
# result.columns   — list of column names
# result.meta      — dict mapping column names to FieldMetadata (label, and more coming soon)
#
# client.query() returns SlayerResponse with all fields above
# client.sql(query) returns just the generated SQL string
# client.explain(query) returns SlayerResponse with EXPLAIN ANALYZE output
# result.row_count — number of rows
# result.sql       — generated SQL string
```
