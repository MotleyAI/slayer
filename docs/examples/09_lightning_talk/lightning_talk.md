# SLayer in 10 minutes — lightning-talk notebook

This is the slide-deck companion to [`lightning_talk_nb.ipynb`](lightning_talk_nb.ipynb) — a 22-cell notebook that walks through what an AI agent needs to do meaningful data analysis, and how SLayer delivers each one against the bundled Jaffle Shop demo.

## The pitch

An agent that writes raw SQL can produce a perfectly valid query for the *wrong* metric — and lose the context behind every choice the next time it's invoked. SLayer takes a different bet: give the agent (a) a typed map of the data, (b) metrics that compose at query time, (c) a memory of business context that persists across sessions.

## What an agent needs to actually analyse data

1. **See** what data is available
2. **Get** trusted metric definitions
3. **Ask** for ad-hoc queries — including complex ones
4. **Know** which metric to choose when
5. **Have** business context for specific cases
6. **Find** the relevant bits of all of the above quickly

The notebook covers each one in two or three cells.

## The walkthrough

| # | What | Notebook cells |
|---|------|----------------|
| 1 | **See** — auto-ingestion: point SLayer at a database, get typed models with foreign-key joins. | 4, 6 |
| 2 | **Get** — the same `order_total` column, three aggregations in one query. The definition lives in the model; the rollup is the agent's choice per query. | 8 |
| 3 | **Ask** — a hero query that mixes month-over-month growth, year-over-year growth (via `time_shift`), a filter on the transform, and a top-N. | 10, 11 |
| 4 | **Compose** — every query is also a model; multistage falls out of that. Inner stage sums revenue by store-month; outer stage averages those monthly sums per store. | 13 |
| 5+6 | **Have & Find** — save a learning memory ("Brooklyn switched POS in late 2024"), save a known-good query pattern, then retrieve them three ways: question, entity, discovery. | 15–20 |

## The hero query

```json
{
  "source_model": "orders",
  "measures": [
    {"formula": "order_total:sum", "name": "revenue"},
    {"formula": "change_pct(order_total:sum)", "name": "mom_growth"},
    {"formula": "order_total:sum / time_shift(order_total:sum, -1, 'year') - 1", "name": "yoy_growth"}
  ],
  "dimensions": ["stores.name"],
  "time_dimensions": [{"dimension": "ordered_at", "granularity": "month"}],
  "filters": ["change_pct(order_total:sum) > 0"],
  "order": [{"column": "mom_growth", "direction": "desc"}],
  "limit": 10
}
```

One declarative object. SLayer handles the joins, the window function for `change_pct`, the self-join CTE for `time_shift`, the filter-on-transform via a hidden field, the GROUP BY, and the dialect-specific SQL. The agent didn't write a line of it.

## Memories + 3-way search

The notebook saves two memories — one learning (Brooklyn POS) and one query-bearing (top-5 customers by lifetime spend) — and then retrieves them three ways through a single `search` call. Channels:

- **BM25** over each memory's stored entity tags (`<datasource>.<model>.<column>` triples). Strongest when the agent already has an entity reference.
- **tantivy** full-text over learning text plus canonical entities. Best for natural-language questions and entity discovery.
- **embeddings** (dense cosine, via litellm). Optional — degrades gracefully without an API key; runs once added.

Ranks are merged via Reciprocal Rank Fusion. The agent doesn't pick a channel.

## Try it in Claude Code

One command wires SLayer up as an MCP server with the same Jaffle Shop demo:

```bash
claude mcp add slayer -- uvx --from motley-slayer slayer mcp --demo
```

Then ask Claude in any project: *"What stores are in jaffle_shop and which one has the highest revenue?"* — it will call the same tools used in the notebook.

## What's here, what's coming

**Here today:** MIT-licensed. Interfaces: MCP, CLI, REST, Python, Flight SQL. Auto-ingestion. Postgres, MySQL, Snowflake, BigQuery, DuckDB, ClickHouse, SQLite. Multistage queries, named measures, custom aggregations, memories with embedding search.

**Coming next:** proper graph / Cypher support, so memories and entities form an explicit graph the agent can traverse.

## Links

- [Notebook](lightning_talk_nb.ipynb) (runnable)
- [Models concept](../../concepts/models.md)
- [Queries concept](../../concepts/queries.md)
- [Memories concept](../../concepts/memories.md)
- [Search concept](../../concepts/search.md)
- [GitHub](https://github.com/MotleyAI/slayer) · [Discord](https://discord.gg/egWxMctHCA)
