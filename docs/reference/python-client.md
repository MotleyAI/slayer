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

client = SlayerClient(url="http://localhost:5143")

query = {
    "source_model": "orders",
    "measures": ["*:count", "revenue:sum"],
    "dimensions": ["status"],
    "limit": 10,
}

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

### Accepted Input Shapes

`client.query` / `query_sync` / `sql` / `sql_sync` / `explain` / `explain_sync` / `query_df` all accept the same input union (mirroring `engine.execute`):

- A **dict** — a single query.
- A **`SlayerQuery`** instance.
- A **list of dicts or `SlayerQuery`** — a multi-stage DAG. Earlier stages are named sub-queries; the last entry is the root. Order doesn't matter (the engine auto-sorts). See [Query Lists](../concepts/queries.md#query-lists).
- A **string** — runs the backing query of a query-backed model by name.

```python
# Multi-stage DAG
client.query_sync([
    {"name": "by_customer", "source_model": "orders", "measures": [{"formula": "amount:sum"}], "dimensions": [{"name": "customer_id"}]},
    {"source_model": "by_customer", "measures": [{"formula": "amount_sum:avg"}]},
])

# Run-by-name (query-backed model)
client.query_sync("rev_by_region")

# Raw rows — opt out of the dim-only auto-dedup. Per-stage in DAG queries.
client.query_sync({
    "source_model": "orders",
    "dimensions": ["status", "amount"],
    "filters": ["amount > 100"],
    "limit": 100,
    "distinct_dimension_values": False,
})
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

### Inspect

`inspect` / `inspect_sync` is a single-entity point lookup (DEV-1588): the rendered detail for **exactly one** entity by `reference` + required `entity_type`. No fusion / ranking / bundled memories — use `search` for an entity *in context*. Same arguments as the MCP `inspect` tool and `POST /inspect`; returns the rendered string.

```python
# Compact default: schema skeleton for a model (column / measure / aggregation
# names + joins, zero DB calls); description-only for the other kinds.
print(client.inspect_sync(reference="mydb.orders", entity_type="model"))

# Full render of one column (compact=False); join paths resolve to the owner.
print(client.inspect_sync(
    reference="mydb.orders.customers.region", entity_type="column",
    compact=False,
))

# async form:  await client.inspect(reference="mydb.orders", entity_type="model")
```

`entity_type` is required (`datasource` / `model` / `column` / `measure` / `aggregation` / `memory`) and asserts the resolved kind. The model-only `num_rows` / `show_sql` / `sections` apply for `entity_type="model"`; `descriptions_max_chars` applies to every kind. `format="json"` returns a JSON string instead of Markdown.

### Memories + Semantic Search

`SlayerClient` exposes the same single retrieval surface as MCP / REST. All three are async (`run_sync` wraps them for synchronous use); local mode (`storage=`) goes through `SearchService` / `MemoryService` directly, remote mode (`url=`) POSTs to `/search` and `/memories`. See [Search](../concepts/search.md) and [Memories](../concepts/memories.md).

```python
from slayer.async_utils import run_sync

# Save a learning
run_sync(client.save_memory(
    learning="orders.is_returned in {0,1,NULL}; treat NULL as not returned",
    linked_entities=["mydb.orders.is_returned"],
    id="kb.returns.null-handling",   # optional; auto-allocated if omitted
))

# Save a query-bearing memory — pass a SlayerQuery / dict for linked_entities
run_sync(client.save_memory(
    learning="Top customers by lifetime spend",
    linked_entities={
        "source_model": "orders",
        "measures": [{"formula": "amount:sum", "name": "lifetime_spend"}],
        "dimensions": ["customers.name"],
        "order": [{"column": "lifetime_spend", "direction": "desc"}],
        "limit": 5,
    },
    id="kb.top-customers",
))

# Search
resp = run_sync(client.search(
    question="What should I know before comparing Brooklyn revenue to other stores?",
    max_results=10,
))

for hit in resp.results:
    if hit.kind == "memory":
        kind = "example_query" if hit.query is not None else "learning"
        print(f"[{kind}] {hit.id} score={hit.score:.3f}  {hit.text[:80]}")
    else:
        print(f"[{hit.kind}] {hit.id} score={hit.score:.3f}")

# Forget by id (cascade-strips memory:<id> refs from other memories)
run_sync(client.forget_memory("kb.returns.null-handling"))
```

`client.search` signature (keyword-only):

```python
async def search(
    self,
    *,
    entities: Optional[List[str]] = None,
    query: Optional[Union[SlayerQuery, Dict[str, Any]]] = None,
    question: Optional[str] = None,
    datasource: Optional[str] = None,
    max_results: int = 10,
    cypher_filter: Optional[str] = None,
) -> SearchResponse: ...
```

`cypher_filter` accepts full openCypher when the `advanced_search` extra is installed (LadybugDB property graph with `Memory` / `Datasource` / `Model` / `ModelColumn` / `Measure` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `JOINS` edges; mutation clauses rejected). Without the extra, only the naive form `MATCH (n:Label1:Label2…) RETURN n.id AS id` is accepted as a label/kind filter — anything richer raises with an install hint.

`SearchResponse` carries a single flat ranked list. Each `SearchHit` has `kind` (`"memory"` / `"datasource"` / `"model"` / `"column"` / `"measure"` / `"aggregation"`), `id`, `score`, `text`, `matched_entities`, and `query` (the attached `SlayerQuery` for query-bearing memories, else `None`). Unresolved input tokens land in `SearchResponse.warnings` instead of raising. Column hits include the structured `sampled_values` snapshot (top 50 by frequency, JSON-encoded) and a `Distinct count: N` line when the true cardinality overflows the snapshot; stale column profiles are refreshed lazily inside `search()`.

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
# result.attributes — ResponseAttributes with .dimensions and .measures dicts (column → FieldMetadata)
#
# client.query() returns SlayerResponse with all fields above
# client.sql(query) returns just the generated SQL string
# client.explain(query) returns SlayerResponse with EXPLAIN ANALYZE output
# result.row_count — number of rows
# result.sql       — generated SQL string
```
