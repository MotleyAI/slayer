# {{product}} — conceptual help

{{product}} is a semantic layer for AI agents: describe what data you want —
**measures**, **dimensions**, **filters** — and {{product}} generates and executes
the SQL. The query language (functional aggregations like `sum(revenue)` /
`count(*)`, `partition_by=`, transforms, nesting, cross-model dotted references,
multi-stage lists) is documented on the `query` tool and its SlayerQuery schema —
read those first.

## Judgment calls the tool docs can't make for you

1. **Choose the population deliberately.** `source_model` decides which rows exist
   in the result. Omit it to let the engine infer the smallest model determining
   your dimensions (the choice is reported back); name it explicitly when the
   question implies a different row set. A query rooted at `orders` enriched from
   `customers` omits customers with no orders; rooted at `customers` it keeps them.
2. **Check a column before trusting it.** `inspect` it and read `Description:` and
   `Sample values:` — if the sampled values are all NULL or not what the name
   suggests, it's the wrong column.
3. **Count rows with `count(*)`**, never by counting a primary-key column.
4. **Never write joins yourself.** Reference joined data by dotted path
   (`customers.regions.name`) and let the engine route it.

## Deep dives

Read each with `inspect(reference="memory:help.<topic>", entity_type="memory")`:
`memory:help.models` — authoring models: columns, saved measures, joins, filters,
query-backed models. `memory:help.workflow` — tool-chaining for common tasks and
an error decoder.
