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
    "population": "orders",
    "metrics": ["count(orders.*)"],
    "dimensions": ["orders.status"],
    "limit": 10
  }'
```

Response:

```json
{
  "data": [
    {"orders.status": "completed", "orders._count": 248238},
    {"orders.status": "refunded", "orders._count": 19288},
    {"orders.status": "cancelled", "orders._count": 8315}
  ],
  "row_count": 3,
  "columns": ["orders.status", "orders._count"],
  "warnings": []
}
```

The response also carries `population` (the named population, else the inferred dimension anchor), `population_inferred`
(whether the rows were inferred) and `row_sources`, the datasets whose rows supplied the result (`["unit"]` with no dimensions) — see
[Population](../concepts/queries.md#population).

Cell values use SLayer's one JSON encoding: a list is a JSON array, a decimal a number with its exact digits, a
UUID its string, bytes `\x` + lowercase hex, an interval an ISO-8601 duration and NaN / ±Infinity `null`;
`"distinct_dimension_values": false` returns one row per record, list fields included.

`warnings` carries advisories about the query itself; it is `[]` for a clean
query. Every entry has a `kind` discriminator — switch on it rather than on the
presence of a field:

| `kind` | Meaning | Extra fields |
| -- | -- | -- |
| `normalization` | The input was rewritten to canonical form | `rule_id`, `original`, `normalized`, `location`, `rule_doc_url` (nullable) |
| `deprecated_input` | A deprecated input form was converted to its replacement ([Upgrading to 2.0](../upgrading/2.0.md)) | `old`, `new`, `removal` |
| `deprecated_spelling` | A deprecated expression spelling was read as its canonical twin ([Upgrading to 2.0](../upgrading/2.0.md)) | `spelling`, `written`, `canonical`, `removal` |

The body accepts the same fields as a `SlayerQuery`, plus `dry_run`, `explain`, and `variables`. Notable optional fields:

- `whole_periods_only` (bool) — snap time bounds to bucket boundaries; see [Time](../concepts/time.md#whole_periods_only).
- `time_zone` (str, optional) — IANA zone timestamps are bucketed, filtered and returned in; the response reports it as `time_zone`. See [Time zones](../concepts/time.md#time-zones).
- `distinct_dimension_values` (bool, default `true`) — set `false` to emit raw rows (no top-level `GROUP BY`); rejects any metric reference in `metrics` / `filter` / `order`.
- `to_many_handling` (str, optional) — how an aggregate resolves query dimensions [unattributable from its root](../concepts/cross-dataset-queries.md#cross-dataset-metrics): omitted or `associate` (per-cell value over the distinct associated rows) or `error` (refuse); `broadcast` is deprecated, removed in 3.0 — give each aggregate `partition_by=` instead. Rejected at the top level of run-by-name bodies (`{"name": ...}`) — put it in `refine` instead.

Run-by-name bodies (`{"name": ...}`) run a query-backed dataset's stored query and allow only `name`, `variables`, `dry_run`, `explain` and `refine`; query clauses go inside `refine`, which merges them into the saved query's final stage ([Refining a saved query](../concepts/saved-and-multistage-queries.md#refining-a-saved-query)).

```bash
curl -X POST http://localhost:5143/query \
  -H "Content-Type: application/json" \
  -d '{"name": "monthly_revenue", "variables": {"region": "US"}}'
```

`POST /query?conformance=ossie-l3` runs the body under the strict [Ossie Layer 3 binding](../interfaces/ossie-layer-3.md): the body is the query, beside the wrappers `variables`, `dry_run`, `explain`, `name` and `refine`, and a refusal answers 400 with `detail.code`.

Multi-stage DAG bodies use `{"queries": [...]}` — each stage in the list honours its own `distinct_dimension_values`.

### Datasets

```
GET    /datasets              # List all datasets
GET    /datasets/{name}       # Get dataset definition
POST   /datasets              # Create a dataset
PUT    /datasets/{name}       # Update a dataset
DELETE /datasets/{name}       # Delete a dataset
```

```bash
# List datasets
curl http://localhost:5143/datasets

# Get dataset definition (hidden fields/metrics excluded)
curl http://localhost:5143/datasets/orders

# Create a dataset
curl -X POST http://localhost:5143/datasets \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "sql_table": "public.orders", "data_source": "mydb", ...}'

# Create a query-backed dataset
curl -X POST http://localhost:5143/datasets \
  -H "Content-Type: application/json" \
  -d '{
    "name": "monthly_revenue",
    "data_source": "mydb",
    "source_queries": [{
      "population": "orders",
      "metrics": ["sum(orders.order_total)"],
      "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}]
    }],
    "query_variables": {"region": "US"}
  }'
```

`POST /datasets` and `PUT /datasets/{name}` return `{"status": "created" | "updated", "name": ..., "warnings": [...]}`; `warnings` holds the same `kind`-tagged entries as a query response, here `deprecated_input`, `deprecated_spelling` and `narrowed_access` (the saved dataset reads tagged datasets whose access tags do not cover its own: `data_source`, `dataset`, `narrowed_by`).

A query-backed dataset's `fields` are generated at save, so supplying them returns 400.

A dataset of a semantic model is updated and deleted inside its document.

A delete that would break a stored model-scoped metric or ontology mapping returns 400 listing each one, or, when one is hidden from the caller, a 400 hidden-content conflict naming none; see [Semantic models](../concepts/semantic-models.md#editing).

### Semantic models

```
GET    /semantic-models                        # List: [{"data_source", "name"}]; ?data_source= filters
GET    /semantic-models/{data_source}/{name}   # {"data_source", "name", "text"}: the stored text
POST   /semantic-models                        # Create
PUT    /semantic-models/{data_source}/{name}   # Replace the document's text
DELETE /semantic-models/{data_source}/{name}   # Delete it and its datasets
```

`POST` takes `{"data_source", "name", "content"}`, where `content` is an Ossie `0.3.0.dev0` document as YAML/JSON text or an object, or `{"data_source", "name", "datasets": [...]}` to move standalone datasets into a new document. `PUT` takes `{"content"}`. A refused document, or a delete that would break a stored dependent, returns 400 with the reasons; an unknown document returns 404. See [Semantic models](../concepts/semantic-models.md).

```bash
curl -X POST http://localhost:5143/semantic-models \
  -H "Content-Type: application/json" \
  -d '{"data_source": "mydb", "name": "sales", "datasets": ["orders", "customers"]}'
```

### Ontologies and mappings

```
GET    /ontologies                                # List: [{"name"}]
GET    /ontologies/{name}                         # {"name", "text"}: the stored text
POST   /ontologies                                # Create, under the document's own name
PUT    /ontologies/{name}                         # Replace; the document must declare {name}
DELETE /ontologies/{name}                         # Delete; 400 while a mapping resolves against it
GET    /ontology-mappings                         # List: [{"data_source", "name"}]; ?data_source= filters
GET    /ontology-mappings/{data_source}/{name}    # {"data_source", "name", "text"}
POST   /ontology-mappings                         # Create: {"data_source", "content"}
PUT    /ontology-mappings/{data_source}/{name}    # Replace: {"content"}, declaring {name}
DELETE /ontology-mappings/{data_source}/{name}    # Delete
```

`content` is an Ossie `0.3.0.dev0` document as YAML/JSON text or an object. A refused document returns 400 listing every problem; an unknown one returns 404. Mapping saves return `notes`, such as the expressions whose type is unknown; ontology, mapping and semantic-model saves return `warnings`, the identifier and multiplicity mismatches they touch. See [Ontologies and mappings](../concepts/ontologies.md).

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

### Datasource priority

```
GET /datasources/priority   # {"priority": [...]}, most preferred first
PUT /datasources/priority   # body {"priority": [...]}; [] clears it
```

A bare dataset name that exists in several datasources resolves to the first listed datasource that has it; a name in the list that is not a registered datasource returns 400.

### Validate datasets

`POST /validate-datasets` with body `{"data_source": null}` (or a datasource name) diffs the stored datasets against the live schemas, read-only, and returns the drift findings: the deletes needed to keep them valid plus report-only `retype`, `narrowed_access`, `untyped` and `ontology` entries ([Ontologies](../concepts/ontologies.md)); an unknown datasource returns 404 and an introspection failure 422. See [Schema Drift](../concepts/schema-drift.md).

### Recommend a population

`POST /recommend-population` with body `{"items": [...], "data_source": null, "root_hint": null}` returns the recommended query root and each item's reference path from it; unresolvable, wrong-kind or cross-datasource items return 400. See [Choosing a root dataset](../concepts/cross-dataset-queries.md#choosing-a-root-dataset).

### Ingestion

```
POST /ingest
```

```bash
curl -X POST http://localhost:5143/ingest \
  -H "Content-Type: application/json" \
  -d '{"datasource": "my_postgres", "schema_name": "public"}'
```

The response is the ingest result (`additions`, `to_delete`, `errors`, `skipped`, `objects`, `skipped_schemas`, `broken_dependents`, …); `key_read_failures` lists each key read that failed (table `location`, `read`, `error`); a re-ingest that reads the key fills it, unless a stored field with a key column's name reads something else. A 200 response:

```json
{
  "additions": [],
  "errors": [],
  "key_read_failures": [
    {"read": "primary key", "error": "...", "location": {"name": "customers", "schema_name": "main", "catalog": null}}
  ]
}
```

When `errors` is non-empty the status is 422 and the same result sits under `detail`: `{"detail": {"additions": [], "errors": [...], ...}}`.

### Inspect (single-entity point lookup)

`POST /inspect` returns the rendered detail for **exactly one** entity by `reference` + required `entity_type`. Unlike `POST /search`, there is no fusion / ranking / `cypher_filter` and no bundled memories — use `/search` when you want an entity surfaced *in context*.

**`POST /inspect` body:**

| Field | Type | Description |
|-------|------|-------------|
| `reference` | str \| array[str] \| null | Canonical id (`mydb.orders.amount`), bare name, relationship path (`orders.customers.region` → resolved to the owning dataset), or `memory:<id>`. Normalised via the shared resolver; the normalised id is echoed in the result. **A list is a homogeneous-kind batch**: one `entity_type` for every id; `result` is one block per id in input order (a `## <canonical>` header per block in markdown, a JSON-array string under `format="json"`). Per-id resolution errors are isolated; a non-string member returns HTTP 422. **`null` / omitted (or `[]`) is the collection view**: lists the whole kind at `entity_type` — `dataset` (all datasets grouped by datasource) or `datasource` (all datasources); other kinds return HTTP 400. `compact` toggles verbosity; the JSON `result` is a `{"entity_type", "collection": true, "datasources": [...], "warnings": []}` envelope string. Subsumes `datasets_summary` / `list_datasources`. |
| `entity_type` | str | **Required.** One of `datasource`, `dataset`, `field`, `metric`, `aggregation`, `memory`. Disambiguates the 3-part canonical collision (a name shared by, e.g., a field and an aggregation) and asserts the kind — a mismatch returns HTTP 400. |
| `compact` | bool | Default `true`. Description-only for field / metric / aggregation / datasource / memory (`compact` never shortens a `help.*` memory; `descriptions_max_chars` still does); a cheap schema **skeleton** (field / metric / aggregation names + relationship targets, zero DB calls) for `dataset`. `false` returns the full render, and a per-dataset skeleton for each visible dataset for `datasource`. |
| `format` | str | `"markdown"` (default) or `"json"`. |
| `num_rows` | int | Sample-data rows for `entity_type="dataset"`. Ignored (with a warning) for other kinds. Default `3`. |
| `show_sql` | bool | Include generated SQL for `entity_type="dataset"`. Ignored (with a warning) for datasource / memory; a silent no-op for field / metric / aggregation. |
| `sections` | array[str] | Section subset for `entity_type="dataset"` (`fields` / `metrics` / `aggregations` / `relationships` / `samples` / `learnings`). Ignored (with a warning) for other kinds. |
| `descriptions_max_chars` | int | Truncate every description field to this many characters (must be `>= 0`). Applies to every kind. |

Renders hidden entities (deliberate escape hatch). Unknown fields are rejected (HTTP 422); a bad `entity_type` / `format`, a negative `descriptions_max_chars`, or an unresolvable reference returns HTTP 400.

```bash
curl -X POST http://localhost:5143/inspect \
  -H "Content-Type: application/json" \
  -d '{"reference": "jaffle_shop.orders", "entity_type": "dataset"}'
```

The response is always `{"result": <string>}` — the rendered Markdown (or, with `format="json"`, a JSON string):

```json
{"result": "# `orders`\nOne row per placed order.\nFields: id, order_total, ...\nMetrics: ...\nAggregations: _(none)_\nRelationships to: stores"}
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
| `entities` | array[str] | Canonical entity strings (`mydb.orders.amount`, `memory:42`, …). Aggregation suffixes are stripped (`sum(revenue)` → `mydb.orders.revenue`). Drives the BM25 channel. Unresolved tokens emit warnings rather than errors. |
| `query` | object | Inline SLayer query; its `population` / dimensions / metrics / time dims / filter are walked for canonical entities. |
| `question` | str | Free-text question. Drives the Tantivy channel and (when available) the embedding channel. |
| `datasource` | str | Pre-narrows every channel to ids rooted at the named datasource. Unknown name → HTTP 400. |
| `cypher_filter` | str | Graph pre-filter applied to all three channels. Full openCypher when `advanced_search` is installed (LadybugDB property graph with `Memory` / `Datasource` / `Dataset` / `DatasetField` / `Metric` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `RELATIONSHIPS` edges; read-only — mutation clauses rejected). Without the extra, only the naive form `MATCH (n:Label1:Label2…) RETURN n.id AS id` is accepted as a label/kind filter; anything richer returns HTTP 400 with an install hint. |
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
    {"kind": "field",   "id": "jaffle_shop.orders.order_total", "score": 0.11, "text": "...\nSample values: [\"100.00\", \"42.50\", …]", "matched_entities": [], "query": null},
    {"kind": "dataset", "id": "jaffle_shop.stores", "score": 0.09, "text": "...", "matched_entities": [], "query": null}
  ],
  "resolved_input_entities": [],
  "warnings": []
}
```

`kind` is one of `"memory"`, `"datasource"`, `"dataset"`, `"field"`, `"metric"`, `"aggregation"`. For memory hits, `id` is the raw memory id (suitable for `DELETE /memories/{id}`); `query` carries the saved `SlayerQuery` when the memory is query-bearing. Field hits embed the structured `sampled_values` (top 50 by frequency, JSON-encoded; overflow fields are marked `50+ distinct` in the text snapshot); stale profiles are refreshed lazily inside `/search`.

**`POST /memories` body:**

```json
{
  "learning": "Refunded and cancelled orders keep their order_total; count only completed orders for net revenue",
  "linked_entities": ["jaffle_shop.orders.status", "jaffle_shop.orders.order_total"],
  "id": "kb.orders.refunds"
}
```

`linked_entities` accepts either an array of canonical entity strings (strict resolution; `memory:<id>` valid for cross-memory refs) **or** an inline `SlayerQuery` dict — the entities are auto-extracted and the query is persisted on the memory. `id` is optional; omit to auto-allocate (`max(int-shaped id) + 1`). Forbidden charset on user-supplied ids: `:`, `/`, `?`, `#`, whitespace, ASCII control. Duplicate id → unconditional upsert; `created_at` preserved.

Response:

```json
{"memory_id": "kb.orders.refunds", "entities": ["jaffle_shop.orders.status", "jaffle_shop.orders.order_total"], "warnings": []}
```

**`DELETE /memories/{id}`:**

```bash
curl -X DELETE http://localhost:5143/memories/kb.orders.refunds
```

Removes the memory and the matching embedding row, and strips every `memory:<id>` reference to it from every other memory's `entities` list (exact-match only — `memory:42` does not strip `memory:421`).

A second cleanup pass runs on `slayer ingest` / `--ingest-on-startup` (best-effort, transient failures keep the ref intact).
