# From OSI to SLayer

SLayer ingests an [OSI](https://open-semantic-interchange.org/) (Open Semantic Interchange) config — its `datasets`, `relationships`, and `metrics` — and turns it into queryable SLayer models, filling in column types from live introspection of your database. Here is the whole path, from install to querying it through an agent.

## 1. Install SLayer

With [uv](https://docs.astral.sh/uv/):

```bash
uv tool install 'motley-slayer[advanced_search]'
```

DuckDB and SQLite work out of the box; for other databases add the driver extra (e.g. `motley-slayer[postgres,advanced_search]`).

## 2. Point SLayer at a storage folder

SLayer keeps its datasources and models in one folder. Set it once, and every command uses it:

```bash
export SLAYER_STORAGE=~/slayer-data
```

## 3. Register the datasource

OSI carries no column types, so the importer reads them live — the datasource has to exist first:

```bash
slayer datasources create duckdb:///path/to/shop.duckdb --name shop_osi
```

## 4. Import the OSI config

```bash
slayer import-osi shop.osi.yaml --datasource shop_osi
```

Each dataset becomes a model, each relationship a join, each metric a measure. See [Importing OSI configs](../../osi/osi_import.md) for exactly what converts and what fails cleanly.

## 5. Connect it to Claude Code

Register SLayer as an MCP server; Claude Code spawns it on demand and calls its tools:

```bash
claude mcp add slayer -- slayer mcp --storage "$SLAYER_STORAGE"
```

Now ask your agent to explore — it calls `models_summary`, `inspect`, `search`, and `query` against your imported models.

## Try it — the notebooks

Two self-contained, **fully offline** notebooks build a tiny retail DuckDB and run the whole flow end to end:

- [`osi_import_nb.ipynb`](osi_import_nb.ipynb) — the **library** path: import with `OsiToSlayerConverter`, query with the `SlayerClient`.
- [`osi_import_agent_nb.ipynb`](osi_import_agent_nb.ipynb) — the **agent** path: the CLI commands above, then the MCP tools an agent calls.

Both import [`shop.osi.yaml`](shop.osi.yaml) and check every answer against gold SQL.

## What the import produces

Each OSI **dataset** becomes a SLayer model, with real column *types* from live introspection of the datasource (OSI carries no type hints). Each **relationship** becomes a LEFT join, and each **metric** folds into a `ModelMeasure` formula on the model the converter picks as its anchor. OSI `ai_context` and `custom_extensions` are carried onto the SLayer entities as descriptions and `meta`, so an agent reading the model sees them.

## The five queries

| Question | SLayer query | OSI feature shown |
|----------|--------------|-------------------|
| Total order value | `total_amount` | simple metric (`SUM(amount)` → `amount:sum`) |
| Total by region | `total_amount` by `customers.regions.name` | multi-hop join inferred from relationships |
| Average order value | `aov` | derived metric (`SUM(amount) / COUNT(*)`) |
| Amount per distinct customer | `cust_reach` | cross-dataset metric through a join |
| Amount plus region population | `rev_plus_pop` | multi-hop metric with sub-query isolation |

The region grouping and the last two metrics reach across joins the converter inferred from the OSI relationships — no manual SQL join is written. Every answer matches the gold SQL exactly.
