# Python Setup — SDK & Embedded Use

Use SLayer as a Python library — either as a client to a running server, or embedded directly in your application with no server at all.

## Install

```bash
pip install motley-slayer                # Base (SQLite works out of the box)
pip install motley-slayer[all]           # Everything
```

For specific database drivers or optional extras, see the [full list](../configuration/datasources.md#database-drivers):

```bash
pip install motley-slayer[postgres]      # PostgreSQL driver
pip install motley-slayer[client]        # httpx + pandas for remote mode
```

## Embedded mode (no server)

Use SLayer directly in your Python code — no HTTP, no server process:

```python
from slayer.core.models import DatasourceConfig
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# Set up storage
storage = YAMLStorage(base_dir="./slayer_data")

# Connect a database
ds = DatasourceConfig(
    name="my_pg",
    type="postgres",
    host="localhost",
    database="myapp",
    username="analyst",
    password="${DB_PASSWORD}",  # resolved from env vars
)
storage.save_datasource(ds)

# Auto-generate models from schema
models = ingest_datasource(datasource=ds, schema="public")
for model in models:
    storage.save_model(model)
    print(f"  {model.name}: {len(model.dimensions)} dims, {len(model.measures)} measures")
```

## Query

```python
from slayer.core.query import SlayerQuery

engine = SlayerQueryEngine(storage=storage)

result = engine.execute(query=SlayerQuery(
    source_model="orders",
    fields=["*:count", "revenue:sum"],
    dimensions=["status"],
))

for row in result.data:
    print(row)
# {"orders.status": "completed", "orders.count": 42, "orders.revenue_sum": 12345.67}
```

The response object:

```python
result.data        # list of row dicts
result.columns     # list of column names
result.row_count   # number of rows
result.sql         # generated SQL (when dry_run or explain is set)
result.attributes  # ResponseAttributes with .dimensions and .measures dicts
```

## Remote mode (client → server)

Connect to a running SLayer server:

```python
from slayer.client.slayer_client import SlayerClient
from slayer.core.query import SlayerQuery

# Connect to remote server
client = SlayerClient(url="http://localhost:5143")

# Query — returns SlayerResponse (same as embedded mode)
result = client.query(SlayerQuery(
    source_model="orders",
    fields=["*:count"],
    dimensions=["status"],
))
print(result.data)
```

## DataFrame integration

```python
# With pandas (requires motley-slayer[client] extra)
df = client.query_df(SlayerQuery(
    source_model="orders",
    fields=["*:count", "revenue:sum"],
    dimensions=["status"],
))
print(df)
```

## SQLite storage

For single-file storage instead of YAML directories:

```python
from slayer.storage.sqlite_storage import SQLiteStorage

storage = SQLiteStorage(db_path="slayer.db")
# Use exactly like YAMLStorage
```

Or use the factory:

```python
from slayer.storage.base import resolve_storage

storage = resolve_storage("./slayer_data")   # YAML
storage = resolve_storage("slayer.db")       # SQLite
```

## Verify it works

```python
from slayer.storage.base import resolve_storage
from slayer.engine.query_engine import SlayerQueryEngine

storage = resolve_storage("./slayer_data")
engine = SlayerQueryEngine(storage=storage)

# Should list your ingested models
print(storage.list_models())

# Should return data
result = engine.execute(query={"source_model": "orders", "fields": ["*:count"]})
print(f"{result.row_count} row(s), columns: {result.columns}")
```

See the [Python Client Reference](../reference/python-client.md) for the full API.
