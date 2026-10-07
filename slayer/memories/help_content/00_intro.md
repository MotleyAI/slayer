# {{product}} — conceptual help

{{product}} is a semantic layer for AI agents: describe what data you want —
**measures**, **dimensions**, **filters** — and {{product}} generates and executes
the SQL, joins included. One query can compute shares and grand totals, ranks and
top N per group, period-over-period changes, running totals and trailing windows,
aggregates of joined models and aggregates of aggregates. The syntax is on the
`query` tool's input schema: read the descriptions of `measures`, `filters`, `order`,
`time_dimensions[].date_range`, `source_model` and `dimensions` first.

## Judgment calls the tool docs can't make for you

1. **Choose the population deliberately.** `source_model` decides which rows exist
   in the result. Omit it to let the engine infer the smallest model determining
   your dimensions and row-level filter columns (the choice is reported back);
   name it explicitly when the question implies a different row set. A query
   rooted at `orders` enriched from `customers` omits customers with no orders;
   rooted at `customers` it keeps them.
2. **Check a column before trusting it.** `inspect` it and read `Description:` and
   `Sample values:` — if the sampled values are all NULL or not what the name
   suggests, it's the wrong column.
3. **Count rows with `count(*)`**, never by counting a primary-key column.
4. **Never write joins yourself.** Reference joined data by dotted path
   (`customers.regions.name`) and let the engine route it.
5. **Fill time gaps with `time_spine`.** A time dimension on `time_spine.timestamp`
   (with a lower `date_range` bound) returns every bucket, empty ones included, and
   lines several facts up on one time axis.

## Deep dives

Read several at once with
`inspect(reference=["memory:help.<topic>", ...], entity_type="memory")`:

- `memory:help.aggregations` — every aggregation, empty-input values, `partition_by`,
  `window`, nesting.
- `memory:help.transforms` — every transform, the time axis, ranking.
- `memory:help.time` — time points, `date_range` and look-back, running totals,
  granularities, gaps.
- `memory:help.joins` — dotted paths, choosing the root, joined aggregates,
  anti-joins.
- `memory:help.queries` — query shapes, stages, raw rows, variables, debugging.
- `memory:help.workflow` — method, filter literals, verifying a result, error decoder.
- `memory:help.models` — authoring models: columns, saved measures, joins, filters,
  query-backed models.
