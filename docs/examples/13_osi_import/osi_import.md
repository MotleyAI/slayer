# From OSI to SLayer

SLayer can ingest an [OSI](https://open-semantic-interchange.org/) (Open Semantic Interchange) config — its `datasets`, `relationships`, and `metrics` — and turn it into queryable SLayer models. This worked example imports a small retail OSI config end-to-end and answers five questions, checking each answer against gold SQL.

The companion notebook ([`osi_import_nb.ipynb`](osi_import_nb.ipynb)) is self-contained and, unlike the [dbt MetricFlow demo](../11_dbt_metricflow/dbt_metricflow.md), **fully offline** — no network access at any point. Everything it generates lands in a gitignored `.cache/` directory next to it:

1. **Build** a tiny retail DuckDB (orders, customers, products, regions) with deterministic rows.
2. **Reference** — run every gold SQL query up front.
3. **Import** the OSI config ([`shop.osi.yaml`](shop.osi.yaml)) with [`OsiToSlayerConverter`](../../osi/osi_import.md).
4. **Query** the imported models with hand-written SLayer queries — and verify against gold.

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

## Gold checks run up front

SLayer's importer introspects the datasource live, opening a read-write SQLAlchemy engine on the DuckDB file — and DuckDB will not let a second raw connection share the file under a different configuration. The notebook therefore runs every gold SQL query **before** the import (Step 3) touches the file, caches the expected numbers, and compares afterwards.

The multi-hop `rev_plus_pop` metric is the instructive one: SLayer aggregates the joined `regions.population` **at the regions grain** (each distinct region once), so adding the measure never fans out the order rows. The gold mirrors that isolation — a naive five-way join would add each region's population once per order and over-count.

## The one-liner

The notebook drives the conversion through the Python API so it can show each step, but the same import is one CLI command against a registered datasource:

```bash
slayer import-osi shop.osi.yaml --datasource shop_osi
```

## Further reading

- [Importing OSI configs](../../osi/osi_import.md) — the full conversion reference, including exactly what converts and what fails cleanly.
- [open-semantic-interchange.org](https://open-semantic-interchange.org/) — the OSI standard.
