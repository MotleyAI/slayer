# Semantic models

A semantic model is an [Ossie](https://github.com/apache/ossie) (`0.3.0.dev0`) document that SLayer stores exactly as written: datasets, the relationships between them, and metrics that belong to the model as a whole. Every valid Ossie document is valid SLayer configuration and keeps its Ossie meaning.

```yaml
version: "0.3.0.dev0"
name: sales
description: Orders and the customers who place them
datasets:
  - name: orders
    source: orders
    primary_key: [order_id]
    fields:
      - {name: order_id, expression: {sql: id}, datatype: Integer}
      - {name: customer_id, expression: {sql: customer_id}, datatype: Integer}
      - name: amount
        expression: {sql: amount}   # no datatype: typed from the live column
    metrics:
      - name: total_revenue
        expression: {sql: SUM(amount)}
  - name: customers
    source: customers
    primary_key: [customer_id]
    fields:
      - {name: customer_id, expression: {sql: id}, datatype: Integer}
      - {name: region, expression: {sql: region}, datatype: String}
relationships:
  - name: orders_to_customers
    from: orders
    to: customers
    from_columns: [customer_id]
    to_columns: [customer_id]
metrics:
  - name: revenue_per_customer
    expression: {sql: AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)}
```

## Where documents live

A document belongs to the datasource it is stored under, since an Ossie document carries no connection:

- **YAML storage**: `semantic_models/<data_source>/<name>.yaml`, beside `models/<data_source>/`.
- **SQLite storage**: the `semantic_models` table, holding the document text verbatim.

The document's `name` must equal its storage name. Loading a document never connects to the database and never writes to storage.

Only Ossie's flat `0.3.0.dev0` shape loads. A key Ossie does not define, at any level, makes the document unloadable with an error naming the key; so do an unknown `version`, the legacy `semantic_model` wrapper, the legacy `custom_extensions` key, a YAML or JSON mapping key repeated at any depth (repeated `<<` merge directives are allowed), and a scalar YAML cannot build, such as `2024-02-30`, each named with its line; a save or edit of such text is refused. Upgrade earlier shapes with [`slayer import-osi`](../osi/osi_import.md). An unloadable document does not affect other documents or datasets.

## What a document becomes

**Datasets.** Each Ossie dataset is a native [dataset](datasets.md) of the datasource, exposing exactly its declared fields: a source column no field declares is not on the dataset. Its `source` is a table or a query. `primary_key` marks each named field (a composite key marks every member, and none is a sole identifier). A one-field `unique_keys` entry marks that field `unique`; a composite one stays in the document but proves nothing. `dimension.is_time`, or Ossie's default for the field's `datatype`, sets the time role. Dataset-scoped `metrics` are the dataset's metrics.

**Relationships.** Each top-level relationship becomes a named, many-to-one, LEFT relationship on its `from` dataset, so it proves the hop to-one. A relationship names only datasets of its own document.

**Metrics.** Each top-level metric is a [model-scoped metric](#model-scoped-metrics).

A document's datasets work everywhere a standalone dataset does: queries, `inspect`, search, memories and relationships from other datasets.

## Model-scoped metrics

A query names a model-scoped metric by its bare name, without a population:

```json
{"metrics": ["revenue_per_customer"]}
```

On four customers with 650 of revenue this returns one row, `revenue_per_customer = 162.5`. Add dimensions as usual:

```json
{"metrics": ["revenue_per_customer"], "dimensions": ["customers.region"]}
```

A bare name in a query resolves to a query alias first, then to a model-scoped metric of the datasource, and otherwise fails with `UnknownMetricError`. A model-scoped metric works anywhere a query metric does: as an entry, inside an expression (`revenue_per_customer * 100`), in a metric filter and as an order key. Its result key is its name unless the query aliases it.

A model-scoped metric evaluates exactly as its expression written in place: each aggregate runs over the rows of the dataset it reads, so `revenue_per_customer` returns the same values as the inline `AGG(orders.total_revenue) / count(distinct customers.customer_id)`.

Its expression reads like a query:

- `d.f` is field `f` of dataset `d` of the same document;
- a bare name is another model-scoped metric;
- `AGG(d.m)` is the dataset metric `m` of `d`.

Saving a model-scoped metric through SLayer refuses an unqualified field, a source column, a dataset of another document, a dataset metric without `AGG`, a window function, a circular definition, and a name equal to a reserved word, a library function or a dataset of the datasource.

Ossie does not define a metric that reads another model-scoped metric, or an aggregate that reads several datasets. SLayer evaluates both, and stores them in the document's `SLAYER` entry rather than in its core `metrics`.

`inspect(reference="revenue_per_customer", entity_type="metric")` shows a model-scoped metric's expression, description, `ai_context`, owning document and any load warning; `datasets_summary` lists a datasource's model-scoped metrics, and search finds them as `<data_source>.<metric>`.

### AGG

`AGG(d.m)` evaluates the dataset metric `d.m` as one aggregate and returns the same values as `d.m`. It works in model-scoped metrics, in queries, and in stored dataset metrics, where it follows that definition's own references (`AGG(m)` for a metric of the same dataset, `AGG(path.m)` across a to-one relationship). Its argument must be a single dataset metric: `AGG` of a field, of a model-scoped metric or of any other expression, and `AGG(...) FILTER (WHERE ...)`, are refused.

In a document's portable `sql`, Ossie allows `AGG` only in metrics: as `AGG(dataset.metric)` in a model-scoped metric, and as `AGG(source.measure)` in a dataset-scoped metric, which evaluates a measure of the dataset's source. A field calling `AGG` makes the document invalid, and a dataset metric using any other `AGG` form is stored in its `SLAYER` entry. SLayer loads, stores and exports `AGG(source.measure)` unchanged but cannot evaluate it yet, because its sources expose no measure columns, so a query reading such a metric fails, and so does any model-scoped metric that reads it.

## One namespace per datasource

A SLayer datasource is the equivalent of one Ossie semantic model, so within a datasource:

- dataset names are unique across all documents and standalone datasets together;
- model-scoped metric names are unique across all documents, and differ from every dataset name.

Names compare as [identifier case](references.md) specifies. A save that would introduce a duplicate is refused, naming both owners. When stored files collide (two hand-placed documents declaring `orders`), the colliding dataset is unloadable in every owner, and so is everything in the same document that depends on it; the rest loads.

## Fields without a datatype

A field that declares no `datatype` loads with an unknown type. Before the first query that reads it, the engine reads the live type of the field's expression and caches it; the type is never written to the document. `inspect` shows such a field's type as `unknown (resolved from the live column at query time)`.

[`slayer validate-datasets`](schema-drift.md#untyped-findings) reports each such field as an `untyped` finding, and `--force-clean` writes the live type into the document.

## Ossie metadata

Each dataset's, field's, dataset metric's and relationship's `ai_context`, and its other vendors' `nonportable_extensions`, are read into its `meta` as `osi_ai_context` and `osi_nonportable_extensions`, so `inspect` and search show them. They stay in the document as written; a save never writes these `meta` keys back.

## SLAYER entries

Settings Ossie has no place for travel in a `nonportable_extensions` entry with `vendor_name: SLAYER` on the entity they belong to: row filters, `hidden`, access tags, custom aggregations, query-backed and `sql`-mode sources, field formats and granularities, a relationship's cardinality or join type, and any definition that is not valid Ossie in its position. A document without a `SLAYER` entry loads as plain Ossie.

`SLAYER` entries are read on every load. An entry that repeats core content applies only while that content is unchanged: after a hand edit of the core, the core wins and `inspect` shows a load warning naming the entity. A malformed entry is ignored with a load warning and affects no other entity, as is a relationship's entry declaring more than one relationship.

## What loads but cannot be evaluated yet

A valid document can hold entities SLayer cannot evaluate yet:

- a window function (`OVER`);
- an expression SLayer's expression reader cannot parse, such as the subquery `(SELECT 1)`;
- a model-scoped metric reading a field unqualified or outside an aggregate;
- a model-scoped aggregate naming no dataset, such as `COUNT(*)`;
- a dataset-scoped metric evaluating a source measure with `AGG(source.measure)`;
- a model-scoped metric reaching any of these, directly or through other model-scoped metrics, whose warning names the chain.

Such an entity loads with a warning shown by `inspect`, `datasets_summary`, search and the import report. A query reading it fails with a typed error naming the entity and the reason; nothing else is affected. SLayer refuses to author such an entity itself.

A query without a population whose metrics read no dataset (`NOW()`) fails with `NoRowSourceError`.

## Editing

**Through the dataset tools.** `edit_dataset`, `delete_dataset`, `create_dataset` with `semantic_model`, the REST and CLI dataset writes, ingestion, sampling, `--force-clean` and `--persist-cardinality` all edit a document's datasets inside the document. Ossie-expressible content is written as Ossie; everything else as a `SLAYER` entry. A relationship added to a document dataset lands in the document's top-level `relationships`.

**Text fidelity.** A save patches only what it changes: comments, key order, quoting, blank lines and formatting of untouched parts are kept, in YAML and SQLite storage alike. Concurrent writes to one document are serialised, so no edit is lost. A document edited by hand is picked up on the next query.

**The document itself.** Create, edit and delete documents through MCP (`create_semantic_model`, `edit_semantic_model`, `delete_semantic_model`), REST (`/semantic-models`), the [Python client](../reference/python-client.md#semantic-models) or the [CLI](../reference/cli.md#slayer-semantic-models). `create_semantic_model(datasets=[...])` and `edit_semantic_model(add_datasets=[...])` move standalone datasets into a document, unchanged, which is how to author model-scoped metrics without writing an Ossie file.

**Deleting.** Deleting a document dataset runs the usual dataset cascade (embeddings, memory links, relationships into it) and removes its relationships from the document; model-scoped metrics reading it get the broken-dependent warning. Deleting a document deletes all its datasets and model-scoped metrics.

A save judges only what it adds or changes, plus whatever depends on that, so a stored metric SLayer cannot evaluate never blocks an unrelated edit.

**Access tags.** Through an [access-filtered store](../configuration/storage.md#access-filtered-storage), a document dataset the caller cannot access reads as deleted, relationships into it are pruned, and every model-scoped metric reading it, directly or through other model-scoped metrics, is unknown. The document's text reads without those entities; a replace or edit keeps them, and a write that names one, or deleting a document that holds one, raises `HiddenContentConflictError`.

## Import and export

- [Importing Ossie documents](../osi/osi_import.md): `slayer import-osi` stores documents natively.
- [Exporting to Ossie](../osi/osi_export.md): `slayer export-osi` writes one document per datasource.
