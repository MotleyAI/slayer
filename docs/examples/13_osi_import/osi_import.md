# From OSI to SLayer

SLayer stores an [OSI](https://open-semantic-interchange.org/) (Open Semantic Interchange) semantic-model document natively, as written — its `datasets`, `relationships`, and `metrics` are queryable as they are, with nothing converted. Here is the whole path, from install to querying it through an agent.

## 1. Install SLayer

With [uv](https://docs.astral.sh/uv/):

```bash
uv tool install 'motley-slayer[advanced_search]'
```

DuckDB and SQLite work out of the box; for other databases add the driver extra (e.g. `motley-slayer[postgres,advanced_search]`).

## 2. Point SLayer at a storage folder

SLayer keeps its datasources and datasets in one folder. Set it once, and every command uses it:

```bash
export SLAYER_STORAGE=~/slayer-data
```

## 3. Register the datasource

The document carries no connection: it belongs to the datasource it is imported into, so register that first (the import itself never connects to the database):

```bash
slayer datasources create duckdb:///path/to/shop.duckdb --name shop_osi
```

## 4. Import the OSI config

```bash
slayer import-osi shop.osi.yaml --datasource shop_osi
```

The document is stored as a [semantic model](../../concepts/semantic-models.md) of `shop_osi`, its text unchanged. See [Importing Ossie documents](../../osi/osi_import.md) for what is checked and reported.

## 5. Connect it to Claude Code

Register SLayer as an MCP server; Claude Code spawns it on demand and calls its tools:

```bash
claude mcp add slayer -- slayer mcp --storage "$SLAYER_STORAGE"
```

Now ask your agent to explore — it calls `datasets_summary`, `inspect`, `search`, and `query` against your imported datasets.

## Try it — the notebooks

Two self-contained, **fully offline** notebooks build a tiny retail DuckDB and run the whole flow end to end:

- [`osi_import_nb.ipynb`](osi_import_nb.ipynb) — the **library** path: import with `import_osi`, query with the `SlayerClient`.
- [`osi_import_agent_nb.ipynb`](osi_import_agent_nb.ipynb) — the **agent** path: the CLI commands above, then the MCP tools an agent calls.

Both import [`shop.osi.yaml`](shop.osi.yaml) and check every answer against gold SQL.

## What the import produces

Each OSI **dataset** is a SLayer dataset exposing exactly its declared fields. Each **relationship** is a many-to-one LEFT relationship on its `from` dataset. Each dataset **metric** is a metric of its dataset, and each top-level metric a **model-scoped** metric, queried by its bare name. OSI `ai_context` and `nonportable_extensions` are read into the entities' `meta`, so an agent reading the dataset sees them.

## The five queries

| Question | SLayer query | OSI feature shown |
|----------|--------------|-------------------|
| Total order value | `orders.total_amount` | dataset metric (`SUM(amount)`) |
| Total by region | `orders.total_amount` by `customers.regions.name` | multi-hop relationship path |
| Average order value | `orders.aov` | derived metric (`SUM(amount) / COUNT(*)`) |
| Amount per distinct customer | `cust_reach` | model-scoped metric across a relationship |
| Amount plus region population | `rev_plus_pop` | multi-hop model-scoped metric with sub-query isolation |

The region grouping and the last two metrics reach across the document's relationships — no manual SQL join is written. Every answer matches the gold SQL exactly.
