# REST API

SLayer provides a FastAPI-based REST API on port **5143** by default.

## Start the Server

```bash
slayer serve --storage ./slayer_data
slayer serve --host 0.0.0.0 --port 8080 --storage ./slayer_data
slayer serve --ingest-on-startup --storage ./slayer_data   # idempotent ingest across every configured datasource before the port opens
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
    "measures": ["*:count"],
    "dimensions": ["status"],
    "limit": 10
  }'
```

Response:

```json
{
  "data": [
    {"orders.status": "completed", "orders._count": 42},
    {"orders.status": "pending", "orders._count": 15}
  ],
  "row_count": 2,
  "columns": ["orders.status", "orders._count"]
}
```

The body accepts the same fields as a `SlayerQuery`, plus `dry_run`, `explain`, and `variables`. Notable optional fields:

- `whole_periods_only` (bool) — snap date filters to bucket boundaries.
- `distinct_dimension_values` (bool, default `true`) — set `false` to emit raw rows (no top-level `GROUP BY`); rejects any measure reference in `measures` / `filters` / `order`.

Multi-stage DAG bodies use `{"queries": [...]}` — each stage in the list honours its own `distinct_dimension_values`.

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

# Get datasource (credential fields shown as ***)
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

### Inspect (single-entity point lookup)

`POST /inspect` returns the rendered detail for **exactly one** entity by `reference` + required `entity_type` (DEV-1588). Unlike `POST /search`, there is no fusion / ranking / `cypher_filter` and no bundled memories — use `/search` when you want an entity surfaced *in context*.

**`POST /inspect` body:**

| Field | Type | Description |
|-------|------|-------------|
| `reference` | str \| array[str] \| null | Canonical id (`mydb.orders.amount`), bare name, join path (`orders.customers.region` → resolved to the owning model), or `memory:<id>`. Normalised via the shared resolver; the normalised id is echoed in the result. **A list is a homogeneous-kind batch** (DEV-1612): one `entity_type` for every id; `result` is one block per id in input order (a `## <canonical>` header per block in markdown, a JSON-array string under `format="json"`). Per-id resolution errors are isolated; a non-string member returns HTTP 422. **`null` / omitted (or `[]`) is the collection view** (DEV-1667): lists the whole kind at `entity_type` — `model` (all models grouped by datasource) or `datasource` (all datasources); other kinds return HTTP 400. `compact` toggles verbosity; the JSON `result` is a `{"entity_type", "collection": true, "datasources": [...], "warnings": []}` envelope string. Subsumes `models_summary` / `list_datasources`. |
| `entity_type` | str | **Required.** One of `datasource`, `model`, `column`, `measure`, `aggregation`, `memory`. Disambiguates the 3-part canonical collision (a name shared by, e.g., a column and an aggregation) and asserts the kind — a mismatch returns HTTP 400. |
| `compact` | bool | Default `true`. Description-only for column / measure / aggregation / datasource / memory; a cheap schema **skeleton** (column / measure / aggregation names + join targets, zero DB calls) for `model`. `false` returns the full render, and a per-model skeleton for each visible model for `datasource`. |
| `format` | str | `"markdown"` (default) or `"json"`. |
| `num_rows` | int | Sample-data rows for `entity_type="model"`. Ignored (with a warning) for other kinds. Default `3`. |
| `show_sql` | bool | Include generated SQL for `entity_type="model"`. Ignored (with a warning) for datasource / memory; a silent no-op for column / measure / aggregation. |
| `sections` | array[str] | Section subset for `entity_type="model"` (`columns` / `measures` / `aggregations` / `joins` / `samples` / `learnings`). Ignored (with a warning) for other kinds. |
| `descriptions_max_chars` | int | Truncate every description field to this many characters (must be `>= 0`). Applies to every kind. |

Renders hidden entities (deliberate escape hatch). Unknown fields are rejected (HTTP 422); a bad `entity_type` / `format`, a negative `descriptions_max_chars`, or an unresolvable reference returns HTTP 400.

```bash
curl -X POST http://localhost:5143/inspect \
  -H "Content-Type: application/json" \
  -d '{"reference": "jaffle_shop.orders", "entity_type": "model"}'
```

The response is always `{"result": <string>}` — the rendered Markdown (or, with `format="json"`, a JSON string):

```json
{"result": "# `orders`\nOne row per placed order.\nColumns: id, order_total, ...\nMeasures: ...\nAggregations: _(none)_\nJoins to: stores"}
```

### Memories + Semantic Search

`POST /search` is the single retrieval surface. It returns memories **and** entity discovery hits in one flat list, fused across up to three channels (BM25 over memory entity tags, Tantivy full-text, and — when `motley-slayer[advanced_search]` is installed and a provider API key is set — dense embeddings) via Reciprocal Rank Fusion (`k=60`). See [Search](../concepts/search.md) and [Memories](../concepts/memories.md).

```
POST   /search                # Run a semantic search
POST   /memories              # Persist a memory
DELETE /memories/{id}         # Delete a memory (cascade-strips memory:<id> refs)
```

**`POST /search` body:**

| Field | Type | Description |
|-------|------|-------------|
| `entities` | array[str] | Canonical entity strings (`mydb.orders.amount`, `memory:42`, …). Aggregation suffixes are stripped (`revenue:sum` → `mydb.orders.revenue`). Drives the BM25 channel. Unresolved tokens emit warnings rather than errors. |
| `query` | object | Inline SLayer query; its `source_model` / dimensions / measures / time dims / filters are walked for canonical entities. |
| `question` | str | Free-text question. Drives the Tantivy channel and (when available) the embedding channel. |
| `datasource` | str | Pre-narrows every channel to ids rooted at the named datasource. Unknown name → HTTP 400. |
| `cypher_filter` | str | Graph pre-filter applied to all three channels. Full openCypher when `advanced_search` is installed (LadybugDB property graph with `Memory` / `Datasource` / `Model` / `ModelColumn` / `Measure` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `JOINS` edges; read-only — mutation clauses rejected). Without the extra, only the naive form `MATCH (n:Label1:Label2…) RETURN n.id AS id` is accepted as a label/kind filter; anything richer returns HTTP 400 with an install hint. |
| `max_results` | int | Applied **after** RRF fusion and after the `cypher_filter` allowlist. Default `10`. |

```bash
curl -X POST http://localhost:5143/search \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What should I know before comparing Brooklyn revenue to other stores?",
    "max_results": 10
  }'
```

Response (`SearchResponse`):

```json
{
  "results": [
    {"kind": "memory",  "id": "42", "score": 0.13, "text": "Brooklyn switched POS in late 2024 …", "matched_entities": [], "query": null},
    {"kind": "column",  "id": "jaffle_shop.orders.order_total", "score": 0.11, "text": "...\nSample values: [\"100.00\", \"42.50\", …]", "matched_entities": [], "query": null},
    {"kind": "model",   "id": "jaffle_shop.stores", "score": 0.09, "text": "...", "matched_entities": [], "query": null}
  ],
  "resolved_input_entities": [],
  "warnings": []
}
```

`kind` is one of `"memory"`, `"datasource"`, `"model"`, `"column"`, `"measure"`, `"aggregation"`. For memory hits, `id` is the raw memory id (suitable for `DELETE /memories/{id}`); `query` carries the saved `SlayerQuery` when the memory is query-bearing. Column hits embed the structured `sampled_values` (top 50 by frequency, JSON-encoded; overflow columns are marked `50+ distinct` in the text snapshot); stale profiles are refreshed lazily inside `/search`.

**`POST /memories` body:**

```json
{
  "learning": "orders.is_returned in {0,1,NULL}; treat NULL as not returned",
  "linked_entities": ["mydb.orders.is_returned"],
  "id": "kb.returns.null-handling"
}
```

`linked_entities` accepts either an array of canonical entity strings (strict resolution; `memory:<id>` valid for cross-memory refs) **or** an inline `SlayerQuery` dict — the entities are auto-extracted and the query is persisted on the memory. `id` is optional; omit to auto-allocate (`max(int-shaped id) + 1`). Forbidden charset on user-supplied ids: `:`, `/`, `?`, `#`, whitespace, ASCII control. Duplicate id → unconditional upsert; `created_at` preserved.

Response:

```json
{"memory_id": "kb.returns.null-handling", "entities": ["mydb.orders.is_returned"], "warnings": []}
```

**`DELETE /memories/{id}`:**

```bash
curl -X DELETE http://localhost:5143/memories/kb.returns.null-handling
```

Removes the memory and the matching embedding row, and strips every `memory:<id>` reference to it from every other memory's `entities` list (exact-match only — `memory:42` does not strip `memory:421`).

A second cleanup pass runs on `slayer ingest` / `--ingest-on-startup` (best-effort, transient failures keep the ref intact).
