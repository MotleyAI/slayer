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
    "population": "orders",
    "metrics": ["count(orders.*)", "sum(orders.order_total)"],
    "dimensions": ["orders.status"],
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

Pass `client_factory=` to run every database access through your own client — for example a gateway that holds the credentials; see [SQL Clients](../configuration/sql-clients.md).

### Accepted Input Shapes

`client.query` / `query_sync` / `sql` / `sql_sync` / `explain` / `explain_sync` / `query_df` all accept the same input union (mirroring `engine.execute`):

- A **dict** — a single query.
- A **`SlayerQuery`** instance.
- A **list of dicts or `SlayerQuery`** — a multi-stage DAG. Earlier stages are named sub-queries; the last entry is the root. Order doesn't matter (the engine auto-sorts). See [Query Lists](../concepts/saved-and-multistage-queries.md#query-lists).
- A **string** — runs the backing query of a query-backed dataset by name.

Each also takes `variables=` — runtime values for `{placeholders}`, overriding saved defaults.

`query`, `sql` and `explain` (and their sync twins) take `conformance="ossie-l3"` to run the strict [Ossie Layer 3 binding](../interfaces/ossie-layer-3.md); a refusal raises `OssieQueryError`, in remote mode too.

```python
# Multi-stage DAG
client.query_sync([
    {"name": "by_customer", "population": "orders", "metrics": ["sum(orders.order_total)"], "dimensions": ["orders.customer_id"]},
    {"population": "by_customer", "metrics": ["avg(by_customer.order_total_sum)"]},
])

# Run-by-name (query-backed dataset), optionally refined
client.query_sync("rev_by_region")
client.query_sync("rev_by_region", refine={"dimensions": ["orders.status"]})
client.query_sync("rev_by_status", variables={"status": "refunded"})

# Raw rows — opt out of the dim-only auto-dedup. Per-stage in DAG queries.
client.query_sync({
    "population": "orders",
    "dimensions": ["orders.status", "orders.order_total"],
    "filter": ["orders.order_total > 100"],
    "limit": 100,
    "distinct_dimension_values": False,
})
```

### Other Methods

```python
# List datasets
datasets = client.list_datasets()

# Get dataset definition
dataset = client.get_dataset("orders")

# Create a dataset
client.create_dataset({"name": "orders", "sql_table": "public.orders", ...})

# List datasources
datasources = client.list_datasources()

# Create a datasource
client.create_datasource({"name": "mydb", "type": "postgres", ...})
```

### Semantic models

Async methods, each with a `_sync` twin (`list_semantic_models_sync`, …). See [Semantic models](../concepts/semantic-models.md).

```python
await client.list_semantic_models(data_source="mydb")          # [{"data_source": "mydb", "name": "sales"}]
text = await client.get_semantic_model("sales", data_source="mydb")  # the stored text; None when absent

# From an Ossie document (YAML/JSON text or a dict; name defaults to its own), or by moving standalone datasets in
await client.create_semantic_model(data_source="mydb", content=open("sales.yaml").read())
await client.create_semantic_model(data_source="mydb", name="sales", datasets=["orders", "customers"])

await client.replace_semantic_model("sales", data_source="mydb", content=new_text)
await client.delete_semantic_model("sales", data_source="mydb")  # deletes its datasets too; False when absent
```

### Ontologies and mappings

Async methods, each with a `_sync` twin. See [Ontologies and mappings](../concepts/ontologies.md).

```python
name = await client.create_ontology(content=open("flights.ontology.yaml").read())   # stored under its own name
notes = await client.create_ontology_mapping(data_source="flights", content=open("flights.mapping.yaml").read())
await client.list_ontologies()                                  # [{"name": "Flights"}]
await client.list_ontology_mappings(data_source="flights")      # [{"data_source": "flights", "name": "flights_mapping"}]
text = await client.get_ontology("Flights")                     # the stored text; None when absent
await client.replace_ontology_mapping("flights_mapping", data_source="flights", content=new_text)
await client.delete_ontology_mapping("flights_mapping", data_source="flights")
await client.delete_ontology("Flights")                         # refused while a mapping resolves against it
```

A refused document raises a `ValueError` listing every problem.

### Inspect

`inspect` / `inspect_sync` is a point lookup: the rendered detail for **exactly one** entity by `reference` + required `entity_type`. No fusion / ranking / bundled memories — use `search` for an entity *in context*. Same arguments as the MCP `inspect` tool and `POST /inspect`; returns the rendered string. `reference` also accepts a **list** — a homogeneous-kind batch (one `entity_type` for every id), returning one block per id in input order with per-id error isolation. `reference=None` (or `[]`) is the **collection** view — lists the whole kind (`dataset` grouped by datasource, or `datasource`); other kinds raise. Subsumes `datasets_summary` / `list_datasources`.

```python
# Compact default: schema skeleton for a dataset (field / metric / aggregation
# names + relationships, zero DB calls); description-only for the other kinds.
print(client.inspect_sync(reference="jaffle_shop.orders", entity_type="dataset"))

# Collection: all datasets grouped by datasource (one terse line per dataset).
print(client.inspect_sync(reference=None, entity_type="dataset"))

# Full render of one field (compact=False); relationship paths resolve to the owner.
print(client.inspect_sync(
    reference="jaffle_shop.orders.stores.regions.name", entity_type="field",
    compact=False,
))

# Batch: several same-kind fields in one round-trip.
print(client.inspect_sync(
    reference=["jaffle_shop.orders.order_total", "jaffle_shop.orders.customer_id"],
    entity_type="field", compact=False,
))

# async form:  await client.inspect(reference="jaffle_shop.orders", entity_type="dataset")
```

`entity_type` is required (`datasource` / `dataset` / `field` / `metric` / `aggregation` / `memory`) and asserts the resolved kind. The dataset-only `num_rows` / `show_sql` / `sections` apply for `entity_type="dataset"`; `descriptions_max_chars` applies to every kind. `format="json"` returns a JSON string instead of Markdown.

### Memories + Semantic Search

`SlayerClient` exposes the same single retrieval surface as MCP / REST. All three are async (`run_sync` wraps them for synchronous use); local mode (`storage=`) goes through `SearchService` / `MemoryService` directly, remote mode (`url=`) POSTs to `/search` and `/memories`. See [Search](../concepts/search.md) and [Memories](../concepts/memories.md).

```python
from slayer.async_utils import run_sync

# Save a learning
run_sync(client.save_memory(
    learning="Refunded and cancelled orders keep their order_total; count only completed orders for net revenue",
    linked_entities=["jaffle_shop.orders.status", "jaffle_shop.orders.order_total"],
    id="kb.orders.refunds",   # optional; auto-allocated if omitted
))

# Save a query-bearing memory — pass a SlayerQuery / dict for linked_entities
run_sync(client.save_memory(
    learning="Top customers by lifetime spend",
    linked_entities={
        "population": "orders",
        "metrics": [{"metric": "sum(orders.order_total)", "alias": "lifetime_spend"}],
        "dimensions": ["customers.name"],
        "order": [{"field": "lifetime_spend", "direction": "desc"}],
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
run_sync(client.forget_memory("kb.orders.refunds"))
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

`cypher_filter` accepts full openCypher when the `advanced_search` extra is installed (LadybugDB property graph with `Memory` / `Datasource` / `Dataset` / `DatasetField` / `Metric` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `RELATIONSHIPS` edges; mutation clauses rejected). Without the extra, only the naive form `MATCH (n:Label1:Label2…) RETURN n.id AS id` is accepted as a label/kind filter — anything richer raises with an install hint.

`SearchResponse` carries a single flat ranked list. Each `SearchHit` has `kind` (`"memory"` / `"datasource"` / `"dataset"` / `"field"` / `"metric"` / `"aggregation"`), `id`, `score`, `text`, `matched_entities`, and `query` (the attached `SlayerQuery` for query-bearing memories, else `None`). Unresolved input tokens land in `SearchResponse.warnings` instead of raising. Field hits include the structured `sampled_values` snapshot (top 50 by frequency, JSON-encoded; overflow fields are marked `50+ distinct` in the text snapshot); stale field profiles are refreshed lazily inside `search()`.

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
# result.attributes — ResponseAttributes with .dimensions and .metrics dicts (column → FieldMetadata)
#
# client.query() returns SlayerResponse with all fields above
# client.sql(query) returns just the generated SQL string
# client.explain(query) returns SlayerResponse with the database's EXPLAIN output
# result.row_count — number of rows
# result.sql       — generated SQL string
```

The response also carries `population`, `population_inferred` and `row_sources`: where its rows came from (see
[Population](../concepts/queries.md#population)).
