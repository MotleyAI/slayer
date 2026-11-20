# {{product}} — conceptual help

{{product}} is a semantic layer for AI agents: describe what data you want —
**metrics**, **dimensions**, **filters** — and {{product}} generates and executes
the SQL, joins included. One query can compute shares and grand totals, ranks and
top N per group, period-over-period changes, running totals and trailing windows,
aggregates of joined datasets and aggregates of aggregates. The syntax is on the
`query` tool's input schema: read the descriptions of `metrics`, `filter`, `order`,
`time_dimensions[].date_range`, `population` and `dimensions` first.

## Judgment calls the tool docs can't make for you

1. **Choose the population deliberately.** `population` decides which rows exist
   in the result. Omit it and the rows are every dimension combination some
   metric's rows reach (reported back); name it explicitly when the question
   implies a different row set. A query
   rooted at `orders` enriched from `customers` omits customers with no orders;
   rooted at `customers` it keeps them.
2. **Check a field before trusting it.** `inspect` it with `compact=false` and read `Description:` and
   `Sample values:` — if the sampled values are all NULL or not what the name
   suggests, it's the wrong field.
3. **Count rows with `count(<dataset>.*)`** (`count(orders.*)`), never by counting a primary-key field.
4. **Start every reference at its dataset and never write joins yourself.**
   `orders.amount`, `orders.revenue` (a saved metric), `customers.regions.name`: the
   engine routes each to the population. A bare name is an alias of the query.

## Deep dives

Read several at once with
`inspect(reference=["memory:help.<topic>", ...], entity_type="memory")`:

- `memory:help.aggregations` — every aggregation, empty-input values, `partition_by`,
  `window`, nesting.
- `memory:help.transforms` — every transform, the time axis, ranking.
- `memory:help.time` — time points, `date_range` and look-back, running totals,
  granularities.
- `memory:help.relationships` — dotted paths, choosing the root, joined aggregates,
  anti-joins.
- `memory:help.queries` — query shapes, stages, raw rows, variables, debugging.
- `memory:help.workflow` — method, filter literals, verifying a result, error decoder.
- `memory:help.datasets` — authoring datasets: fields, saved metrics, relationships, filters,
  query-backed datasets.
