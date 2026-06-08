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
    print(f"  {model.name}: {len(model.columns)} columns, {len(model.measures)} measures")
```

## Query

```python
from slayer.core.query import SlayerQuery

engine = SlayerQueryEngine(storage=storage)

result = engine.execute_sync(query=SlayerQuery(
    source_model="orders",
    measures=["*:count", "revenue:sum"],
    dimensions=["status"],
))

for row in result.data:
    print(row)
# {"orders.status": "completed", "orders._count": 42, "orders.revenue_sum": 12345.67}
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
    measures=["*:count"],
    dimensions=["status"],
))
print(result.data)
```

## DataFrame integration

```python
# With pandas (requires motley-slayer[client] extra)
df = client.query_df(SlayerQuery(
    source_model="orders",
    measures=["*:count", "revenue:sum"],
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
result = engine.execute_sync(query={"source_model": "orders", "measures": ["*:count"]})
print(f"{result.row_count} row(s), columns: {result.columns}")
```

## Search & memories

`SlayerClient.search`, `save_memory`, and `forget_memory` are the single retrieval surface for prior notes **and** canonical entity discovery. All three are async; wrap them with `run_sync` for synchronous use. Local mode (`storage=`) goes through `SearchService` / `MemoryService` directly; remote mode (`url=`) POSTs to `/search` / `/memories`.

```python
from slayer.async_utils import run_sync

# Save a learning so the next session inherits it
run_sync(client.save_memory(
    learning="orders.is_returned in {0,1,NULL}; treat NULL as not returned",
    linked_entities=["mydb.orders.is_returned"],
    id="kb.returns.null-handling",   # optional; auto-allocated if omitted
))

# Three retrieval channels (BM25 + Tantivy + optional dense embeddings)
# fused into one flat ranked list:
resp = run_sync(client.search(
    question="What should I know about returns?",
    max_results=10,
))

for hit in resp.results:
    # kind: "memory" | "datasource" | "model" | "column" | "measure" | "aggregation"
    # hit.query is not None marks a saved example query
    print(hit.kind, hit.id, round(hit.score, 3), hit.text[:80])
```

`client.search` also accepts `cypher_filter` for graph-shaped narrowing — full openCypher with the `advanced_search` extra (LadybugDB property graph with `Memory` / `Datasource` / `Model` / `ModelColumn` / `Measure` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `JOINS` edges), naive `MATCH (n:Label) RETURN n.id AS id` kind-filter otherwise. Without `advanced_search` (or a provider API key) the dense-embedding channel emits a single warning into `SearchResponse.warnings` and search degrades to BM25 + Tantivy. Column hits embed the structured `sampled_values` snapshot (top 50 by frequency, JSON-encoded) plus a `Distinct count: N` line on overflow; stale profiles are refreshed lazily inside `search()`.

See [Search](../concepts/search.md), [Memories](../concepts/memories.md), and the [Python Client Reference](../reference/python-client.md#memories-semantic-search) for the full signature.

## Embedded REST / MCP servers

If you're mounting SLayer's REST or MCP surface inside your own process and want models freshly ingested by the time the server starts handling requests, pass `ingest_on_startup=True` to the constructor:

```python
from slayer.api.server import create_app
from slayer.mcp.server import create_mcp_server

app = create_app(storage=storage, ingest_on_startup=True)
mcp = create_mcp_server(storage=storage, ingest_on_startup=True)
```

The constructor runs idempotent auto-ingestion across every configured datasource **before returning** the app/server object. Same opt-in / continue-on-failure / read-only-drift semantics as the CLI flag. See [Ingesting at Startup](../concepts/ingestion.md#ingesting-at-startup).

See the [Python Client Reference](../reference/python-client.md) for the full API.
