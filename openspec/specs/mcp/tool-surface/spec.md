# mcp/tool-surface Specification

## Purpose
Defines what the MCP server advertises to agents so that, within what Claude Code shows the model (descriptions cut at 2048 chars, input schemas passed whole), an agent learns every query capability and can fetch the long-form reference on demand.

## Requirements

### Requirement: Tool descriptions fit the client description budget
Every MCP tool's advertised description SHALL be at most 2048 characters, SHALL carry no indentation inherited from source formatting, and SHALL contain no per-parameter `Args:` section. Building the server with a description over the budget SHALL fail.

#### Scenario: Every advertised description is within budget and dedented
- **WHEN** a client lists the server's tools
- **THEN** each tool's description is at most 2048 characters, equals its own dedented form, and has no `Args:` section

#### Scenario: An over-budget description fails the server build
- **WHEN** the server is built with a tool whose description exceeds 2048 characters
- **THEN** the build fails with an error naming the tool and its length

### Requirement: Server instructions fit the budget and name the help call
The server `instructions` SHALL be at most 2048 characters, SHALL lead their query guidance with the rule in capitals — `ONE QUESTION = ONE QUERY.` followed by `Build the whole answer in a single \`query\` call.` — SHALL forbid splitting an answer into several queries, combining or recomputing query results by hand or in Python, and adding stages for what one stage can do, SHALL state that one query computes totals at any coarser grain and shares of them, ranks and top N per group, period-over-period changes, running totals, trailing windows, aggregates of aggregates and aggregates over several joined models together, SHALL give the reason in one plain sentence — each aggregate runs over its own model's rows, so one query never fans out or double counts — and SHALL give the exact `inspect` call that fetches the help topics. Building the server with over-budget instructions SHALL fail.

#### Scenario: Instructions within budget and pointing at help
- **WHEN** a client initializes a session
- **THEN** the instructions are at most 2048 characters and contain an `inspect` call whose references are `memory:help.*` topics

#### Scenario: Instructions give the fan-out reason
- **WHEN** a client initializes a session
- **THEN** the instructions state that each aggregate runs over its own model's rows, so one query never fans out or double counts

#### Scenario: Instructions lead with the one-query rule
- **WHEN** a client initializes a session
- **THEN** the instructions contain `ONE QUESTION = ONE QUERY. Build the whole answer in a single \`query\` call.` followed by the prohibitions on splitting, recombining results by hand or in Python, and unnecessary stages

### Requirement: Every tool parameter is described in the input schema
Every top-level property of every MCP tool's input schema SHALL carry a non-empty `description`, placed beside the property's type (including a `$ref` / `anyOf` type), not only inside a referenced definition.

#### Scenario: All top-level parameters described
- **WHEN** a client lists the server's tools
- **THEN** every top-level property of every tool's input schema has a non-empty description

#### Scenario: A parameter whose type is a referenced model keeps its own description
- **WHEN** a tool parameter's type is a referenced schema definition (such as the `query` tool's `refine`)
- **THEN** the property carries its own description next to the reference

### Requirement: The query tool description is a capability map
The `query` tool description SHALL open with `ONE QUESTION = ONE QUERY. Build the whole answer in a single \`query\` call.` and SHALL state, within the description budget: that SLayer writes the SQL and joins from a described result; that one query can put several grains on one row (shares of a group or of the grand total), rank and keep the top N per group, sort by values it does not display, compute period-over-period changes and trailing windows that read rows before `date_range` on their own, compute running totals, aggregate joined models' fields over their own rows without double counting, nest aggregates and chain stages; that the syntax is in the descriptions of the query object's fields, naming them; the exact batch `inspect` call for the full help reference; the three input forms (query object, list of stages, saved query-backed model name with `refine`); and that responses without a `limit` stop at 20 rows. It SHALL NOT claim that running totals read rows before `date_range`.

#### Scenario: The capability list and pointers are visible
- **WHEN** a client lists the server's tools
- **THEN** the `query` description starts with the one-query rule, names the field descriptions to read, contains the batch help call, mentions `refine`, and states the 20-row cap, within 2048 characters

#### Scenario: Running totals are not credited with look-back
- **WHEN** a client reads the `query` description's capability list
- **THEN** the look-back before `date_range` is attributed to period-over-period changes and trailing windows, not to running totals

### Requirement: Query-language guidance lives on the owning schema field, once
Each piece of query-language guidance SHALL appear in exactly one place across the `query` tool description and every description in its input schema (field- and definition-level), namely the field where an agent writes that value:
- `metrics`: aggregation call form; `agg(x) FILTER (WHERE c)` aggregating only the rows where `c` holds; arithmetic combining aggregates over different models, with a cross-model ratio example rooted at customers; `partition_by=` with keys spelled like the query's dimensions (dotted paths), a share-of-group example and `partition_by=[]` for the grand total; `window=` with compact durations (`'90d'`, `'3m'`); nested aggregation; one line per transform (`cumsum`, `change` / `change_pct`, `time_shift`, `lag` / `lead`, `rank` / `dense_rank` with required direction, `ntile`, `consecutive_periods`); a joined model's aggregate is computed over that model's own rows and split per group by association across dimensions it cannot be attributed to, its total repeated on every group instead with `partition_by=[]`; aggregates over several child models in one query (orders and returns from customers) are each computed over their own rows; a child row without a parent counts in the NULL row of a parent dimension when that row exists and in an ungrouped total, and is never added again; an aggregate over a model no join path reaches from the population is computed over that model's own rows and repeated on every row, per bucket when a `time_spine` time dimension attributes it through that model's own axis, with an `unrelated_dataset` warning.
- `dimensions`: group by naturally readable labels: a name, never an id alone unless ids are asked for, and the id and the name when a name may repeat.
- `filter`: routing to row / aggregate / post-aggregation filtering; the SQL operators, including `IS [NOT] TRUE` / `IS [NOT] FALSE`, `IS [NOT] DISTINCT FROM` and `CAST`; the list functions `contains`, `contains_any` and `contains_all`; transforms in filters (top N per group via a rank); the anti-join (a joined model's key `is null`); the time-point forms.
- `variables` (a query object's): a placeholder binds a literal value only — its text inside quotes (`'{s}'`), a number, numeric string or boolean in a bare slot, a list as the whole right-hand side of `IN` or as a list-function argument (an empty list only as a list-function argument) — and a placeholder in a name position is an error.
- `order`: sorting by any aggregate or transform, displayed or not, with `limit` for top N; returning only the columns the question asks for, never adding the sort value to the measures.
- `time_dimensions`: buckets without rows are absent; a time dimension on `time_spine.timestamp` with a lower `date_range` bound returns every bucket, empty ones included, and lines several models up on one time axis.
- a time dimension's `date_range`: its forms; that `change`, `change_pct`, `time_shift` and `window=` read rows before the range (a forward `time_shift`, after it) so the range is not widened; that `cumsum` starts at the range start.
- `population`: rooting at the model whose every row must appear when the question asks for every such row, with a named conditional aggregate; otherwise rooting where the facts are, so rows without facts drop out, or omitting it to infer the smallest model determining every dimension and row-level condition column; a per-query column written as an expression in the position that uses it.
- `name` (stages): what needs no stage (top N per group, averaging or bucketing per-entity totals, shares and totals); that a filter on a transform's value applies after it, which is computed over every row the other filters keep; that a stage is needed only to drop rows by a dimension or time condition that a cross-row value must still see, or to join or reuse a whole result, because a row filter or `date_range` applies before a running total, `lag` / `lead`, rank, `ntile`, `consecutive_periods` or `partition_by` total while `change`, `change_pct`, `time_shift` and `window=` read past `date_range` or a filter on their own time dimension only; and that an outer stage references inner dotted columns by their flattened `__` names.

The one-query rule SHALL appear only in the server `instructions` and the `query` tool description. The gap-filling, several-child-models, parentless-child-row, cross-model-ratio, unrelated-model, projection, conditional-rooting and stage guidance SHALL also appear in no help topic and not in the server `instructions`; the instructions' fan-out reason SHALL appear only in the instructions. No description, the instructions or a help topic SHALL tell agents to use a stage to filter after a rank or window, to re-query a whole result, or to answer with one query only "whenever" it can.

Fields that reuse another field's syntax (a refinement's clauses, a measure's `expression`) SHALL point to the owning field instead of repeating its guidance. Guidance too long for its field SHALL be cited by the exact `inspect` call of the help topic that holds it.

#### Scenario: Each guidance marker has one owner
- **WHEN** the `query` tool description and every description in its input schema are collected
- **THEN** each guidance marker (for example the grand-total `partition_by=[]` example, the "rows before the range" rule, the flattened `__` stage-column rule, the stage rule, the readable-label rule) occurs in its owning field's description and nowhere else

#### Scenario: New grammar guidance has one owner
- **WHEN** the same texts are collected
- **THEN** the `FILTER (WHERE` marker occurs only in `metrics`, the `IS DISTINCT FROM` and `contains_any` markers only in `filter`, the whole-`IN` list-binding marker only in a query object's `variables`, and the per-query-expression marker only in `population`

#### Scenario: No inline-extension-column advice remains
- **WHEN** the same texts are collected
- **THEN** none mentions an extension's `columns` or an inline model, and none advises anything but an expression for adding a per-query column

#### Scenario: Moved guidance has no second copy
- **WHEN** the `query` tool description, every description in its input schema, the server instructions and every help topic body are collected
- **THEN** each of the gap-filling (`time_spine.timestamp`), several-child-models, parentless-child-row, cross-model-ratio, unrelated-model, projection, conditional-rooting and stage-rule markers occurs only in its owning field's description, the fan-out-reason marker only in the instructions, and the one-query marker only in the instructions and the `query` description

#### Scenario: No retired stage or one-query wording remains
- **WHEN** the same texts are collected
- **THEN** none tells agents to use stages to filter after a window or rank or to re-query a whole result, none says to answer with one query "whenever" it can, and no help topic keeps a "One query first" section or a "Typical uses" list of stages

#### Scenario: Examples group by readable columns
- **WHEN** the same texts are collected
- **THEN** no example groups, partitions or flattens by a column whose name ends in `_id`

#### Scenario: A refinement clause points to the query field
- **WHEN** a client reads the `refine` argument's `metrics` description
- **THEN** it refers to the query object's `metrics` instead of repeating its guidance

#### Scenario: date_range is described
- **WHEN** a client reads a time dimension's `date_range` description
- **THEN** it states the accepted forms, that `change` / `change_pct` / `time_shift` / `window=` read rows before the range (a forward `time_shift`, after it), and that `cumsum` starts at the range start

### Requirement: Documented query idioms behave as documented
Every query idiom given as an example in the tool or schema descriptions SHALL execute and return the documented result:
- `change`, `change_pct` and `time_shift(…, -1)` with a `date_range` compare the first in-range bucket against the bucket before the range, and `time_shift(…, 1)` compares the last in-range bucket against the bucket after it;
- `time_shift(…, -1, 'year')` compares each bucket against the same bucket a year earlier;
- an aggregation with `window=` includes rows before the range in the first in-range buckets;
- `cumsum` with a `date_range` accumulates from the range start only;
- `ntile(x, n=4)` puts the lowest values in bucket 1;
- `agg(x) FILTER (WHERE c)` aggregates only the rows where `c` holds: `count(orders.*) FILTER (WHERE orders.status = 'paid')` counts the paid rows;
- a filter `<joined dataset>.<key> is null` keeps exactly the rows with no joined row;
- a query rooted at a model keeps every row of that model, counting 0 where no joined rows exist;
- ordering by an undisplayed aggregate with a `limit` returns the top rows by that aggregate without projecting it;
- a filter on `rank(..., partition_by=customers.regions.name, direction='desc') <= N` keeps the top N per region;
- `x / x(partition_by=customers.regions.name)` and `x / x(partition_by=[])` give the share of region and of grand total;
- a joined model's aggregate sliced by a non-attributable dimension splits per group by association by default, and with `partition_by=[]` repeats its total on every group;
- a nested aggregation averages the inner per-group values within the outer group;
- a granularity call as a dimension (`date_trunc('month', orders.created_at)`) and the time-dimension string `"date_trunc('month', orders.created_at)"` bucket like the equivalent `{"field": ..., "granularity": "month"}` time dimension;
- a computed dimension `iif(sum(orders.amount, partition_by=customers.id) > 150, 'high', 'low')` labels each row by its customer's total;
- an outer stage reads an inner dotted dimension by its flattened `__` name (`customers.regions.name` as `<stage>.customers__regions__name`);
- a monthly time dimension on `time_spine.timestamp` with a `date_range` returns every month of the range, months without rows included (count 0, other aggregates NULL);
- rooted at customers, `sum(orders.amount)` and `sum(returns.amount)` in one query each equal the sum over that child model's own rows;
- rooted at customers and grouped by a parent dimension, a child row without a parent counts in that dimension's NULL row when the population has one and in no row otherwise, and an ungrouped aggregate includes it;
- rooted at customers, `sum(orders.amount) / count(customers.*)` divides the orders' total by the number of customers;
- rooted at customers, `count(events.id) / count(customers.*)` with `events` joined to nothing divides the events' total by the number of customers and warns `unrelated_dataset`;
- a filter on a transform's value (`rank(...) <= N`, `cumsum(...) > X`) applies after the transform, which is computed over every row the other filters keep;
- a time filter on the bucketed time column restricts `cumsum`, `lag`, `lead`, `consecutive_periods`, `rank` / `ntile` over buckets and `partition_by` totals to the kept buckets, while `change`, `change_pct`, `time_shift` and `window=` read past it;
- a filter on a dimension restricts `rank`, `dense_rank`, `ntile`, `partition_by` shares and totals and nested aggregates to the kept peers; a filter on a non-time column restricts the rows `window=` reads, before the range as well;
- an inner stage computing a rank over all rows and an outer stage filtering one row returns that row's rank among all rows;
- a time-point filter is a half-open period: `orders.order_date >= '2025-Q1'` keeps rows from the start of the quarter, `orders.order_date <= '2025-Q1'` rows through its end, and `orders.order_date in '2025-Q1'` rows inside it;
- `items.code ~ '^A[0-9]{3}'` keeps the rows whose value matches the pattern, its braces read as regex text;
- `items.flag IS NOT TRUE` keeps the rows where `flag` is false or NULL, and `items.status IS DISTINCT FROM 'a'` keeps the rows where `status` is NULL or another value;
- over a list column, `contains(items.tags, 'x')` and `items.tags.value = 'x'` keep the rows whose list holds `x`, `contains_any(items.tags, [...])` the rows whose list shares an element with the literal, and `contains_all(items.tags, [...])` the rows whose list holds every element of it;
- a variable inside quotes (`items.status = '{s}'`) binds its text as one string literal; a list variable as the whole right-hand side of `IN` (`items.status in ({statuses})`) keeps the rows whose value is in the list, and an empty list there fails with an error naming the variable, while an empty list as a list-function argument (`contains_all(items.tags, {wanted})`) binds as an empty list.

#### Scenario: Lookback before date_range
- **WHEN** a monthly query with `date_range` `"2025"` measures `change(sum(orders.amount))`
- **THEN** January 2025's change is computed against December 2024

#### Scenario: Year-over-year time shift
- **WHEN** a monthly query with `date_range` `"2025"` measures `time_shift(sum(orders.amount), -1, 'year')`
- **THEN** each month carries the amount of the same month of 2024

#### Scenario: Running total starts at the range
- **WHEN** a monthly query with `date_range` `"2025"` measures `cumsum(sum(orders.amount))`
- **THEN** January 2025's value excludes 2024 amounts

#### Scenario: ntile puts the lowest values first
- **WHEN** a query grouped by customer name measures `ntile(sum(orders.amount), n=4)`
- **THEN** the customers with the lowest totals are in bucket 1 and those with the highest in bucket 4

#### Scenario: A FILTER clause restricts the aggregated rows
- **WHEN** a query on orders measures `count(orders.*) FILTER (WHERE orders.status = 'paid')` and `count(orders.*)`
- **THEN** the first value counts only the paid orders and the second every order

#### Scenario: Anti-join
- **WHEN** a query rooted at customers filters `orders.id is null`
- **THEN** exactly the customers with no orders are returned

#### Scenario: Cross-model aggregate broadcast vs associate
- **WHEN** a query rooted at orders, grouped by order status, measures `sum(customers.credit)`
- **THEN** by default each row carries the credit of the distinct customers with orders in that status with an associated-cells warning, and `sum(customers.credit, partition_by=[])` carries the total credit of all customers on each row with no warning

#### Scenario: Granularity calls bucket like a time dimension
- **WHEN** one query groups by the dimension `date_trunc('month', orders.created_at)`, another by the time dimension `"date_trunc('month', orders.created_at)"`, and a third by `{"field": "orders.created_at", "granularity": "month"}`, each measuring `count(orders.*)`
- **THEN** all three return the same monthly buckets and counts

#### Scenario: A computed dimension over a fixed-grain aggregate
- **WHEN** a query on orders groups by the computed dimension `iif(sum(orders.amount, partition_by=customers.id) > 150, 'high', 'low')` named `spend_band` and measures `count(orders.*)`
- **THEN** each order is counted in the band of its customer's total amount

#### Scenario: Time spine returns empty buckets
- **WHEN** a query buckets `time_spine.timestamp` by month with `date_range` `"2025"` and measures `sum(orders.amount)` and `count(orders.id)`, and one month of 2025 has no orders
- **THEN** twelve rows are returned, the month without orders carrying count 0 and sum NULL

#### Scenario: Several child models in one query
- **WHEN** a query rooted at customers, grouped by customer name, measures `sum(orders.amount)` and `sum(returns.amount)`
- **THEN** each customer's values equal the sums over that customer's own orders and own returns, unmultiplied by the other child model's rows

#### Scenario: Parentless child rows land in the parent dimension's NULL row
- **WHEN** a query rooted at customers, grouped by `regions.name`, measures `sum(orders.amount)`, one customer has no region and some orders have no (or a non-existent) customer
- **THEN** the NULL-region row holds the region-less customer's orders plus the parentless orders, and the same aggregate without dimensions includes the parentless orders

#### Scenario: Parentless child rows without a NULL population row
- **WHEN** the same query runs on data where every customer has a region
- **THEN** no NULL-region row is returned and the parentless orders count in no row

#### Scenario: Cross-model ratio
- **WHEN** a query rooted at customers measures `sum(orders.amount) / count(customers.*)`
- **THEN** the value equals the orders' total divided by the number of customers

#### Scenario: A filter on a transform's value applies after it
- **WHEN** a monthly query over all months measures `cumsum(sum(orders.amount))` and filters on that running total exceeding a threshold
- **THEN** the kept months carry the running total accumulated over every month, not only the kept ones

#### Scenario: A time filter restricts cross-row values except calendar look-backs
- **WHEN** a monthly query filters the bucketed time column to two months and measures one cross-row value
- **THEN** `cumsum`, `lag`, `lead`, `consecutive_periods`, `rank` / `ntile` over months and `sum(orders.amount, partition_by=[])` are computed over the two kept months only, while `change`, `change_pct`, `time_shift` and `window=` equal their values in the unfiltered query

#### Scenario: A dimension filter restricts peer values
- **WHEN** a query grouped by region name and customer name filters one customer and measures a rank, dense rank, ntile, share of region, grand total or nested average
- **THEN** the value is computed over the kept customer only

#### Scenario: Rank among all rows needs a stage
- **WHEN** an inner stage ranks every customer by total amount and an outer stage keeps one customer
- **THEN** that customer carries its rank among all customers

#### Scenario: Unrelated-model ratio
- **WHEN** a query rooted at customers measures `count(events.id) / count(customers.*)` and no join path connects `events` to `customers`
- **THEN** the value equals the number of events divided by the number of customers, and the response carries an `unrelated_dataset` warning

#### Scenario: Time points are half-open periods
- **WHEN** queries on orders measure `count(orders.*)` with the filters `orders.order_date >= '2025-Q1'`, `orders.order_date <= '2025-Q1'` and `orders.order_date in '2025-Q1'` in turn
- **THEN** the first counts the orders from 2025-01-01 on, the second the orders up to and including 2025-03-31, and the third the orders inside the first quarter of 2025

#### Scenario: Regex braces are pattern text
- **WHEN** a query filters `items.code ~ '^A[0-9]{3}'` with no variables supplied
- **THEN** it keeps exactly the rows whose code starts with `A` and three digits

#### Scenario: Null-aware boolean and distinctness tests
- **WHEN** a query filters `items.flag IS NOT TRUE`, and another filters `items.status IS DISTINCT FROM 'paid'`
- **THEN** the first keeps the rows whose `flag` is false or NULL, and the second the rows whose status is NULL or not `paid`

#### Scenario: List-column filters
- **WHEN** queries over `items`, whose list column is `tags`, filter `contains(items.tags, 'x')`, `items.tags.value = 'x'`, `contains_any(items.tags, ['x', 'y'])` and `contains_all(items.tags, ['x', 'y'])` in turn
- **THEN** the first two keep the rows whose list holds `x`, the third the rows whose list holds `x` or `y`, and the fourth the rows whose list holds both

#### Scenario: A quoted variable binds one string literal
- **WHEN** a filter is `items.status = '{s}'` with `variables={"s": "paid"}`
- **THEN** it keeps the rows whose status is `paid`

#### Scenario: A list variable fills a whole IN list
- **WHEN** a filter is `items.status in ({statuses})` with `variables={"statuses": ["paid", "shipped"]}`
- **THEN** it keeps the rows whose status is `paid` or `shipped`

#### Scenario: An empty list variable binds only as a list-function argument
- **WHEN** a filter is `items.status in ({statuses})` with `statuses` set to `[]`, and another is `contains_all(items.tags, {wanted})` with `wanted` set to `[]`
- **THEN** the first fails with an error naming `statuses` as an empty list, and the second keeps every row whose `tags` is not NULL

### Requirement: Help topics hold the long-form reference and are reachable
The server SHALL seed the help topics `help.intro`, `help.datasets`, `help.workflow`, `help.aggregations`, `help.transforms`, `help.time`, `help.relationships` and `help.queries`. Each topic SHALL be self-contained and carry a one-line description stating when to read it. Every `memory:help.<topic>` reference in the tool descriptions, input schemas or instructions SHALL name a seeded topic. Seeding a store that holds earlier bodies under these ids SHALL replace them with the current bodies, and re-seeding SHALL keep them.

#### Scenario: Every help reference resolves
- **WHEN** all `memory:help.<topic>` references are collected from the tool descriptions, input schemas and instructions
- **THEN** each names a topic present after seeding

#### Scenario: A previously retired topic id is reused
- **WHEN** a store holding stale bodies under `help.aggregations`, `help.transforms`, `help.time`, `help.relationships` and `help.queries` is seeded, then seeded again
- **THEN** each id holds the current body after the first seed and still holds it after the second

### Requirement: Help topics are returned in full by inspect
Inspecting a memory whose id starts with `help.` SHALL return its full body whatever the `compact` setting, on every inspect surface (MCP tool, REST endpoint, CLI, Python client), in markdown and json, for a single reference and in a batch. An explicit `descriptions_max_chars` SHALL still truncate. Memories whose id does not start with `help.` SHALL keep the compact preview behaviour.

#### Scenario: Default inspect of a help topic returns its body
- **WHEN** `inspect` is called with `reference="memory:help.intro"`, `entity_type="memory"` and no `compact`
- **THEN** the full topic body is returned, not only its one-line description

#### Scenario: Host help topics are included
- **WHEN** a host-namespaced topic such as `help.motley.x` is inspected with `compact=True`
- **THEN** its full body is returned

#### Scenario: Other memories stay compact
- **WHEN** a user memory is inspected with the default `compact`
- **THEN** only its one-line description is returned

#### Scenario: Explicit truncation still applies
- **WHEN** a help topic is inspected with `descriptions_max_chars` set
- **THEN** the body is truncated to that length

### Requirement: Optional always-load marking of the query tool
The server SHALL offer an option, off by default, that marks the `query` tool with the `anthropic/alwaysLoad` meta flag so Claude Code keeps it loaded instead of deferring it behind tool search. The option SHALL be settable for both the standalone MCP server and the MCP server embedded in the REST app, by command-line flag and by the `SLAYER_MCP_ALWAYS_LOAD_QUERY` environment variable. No other tool SHALL carry the flag.

#### Scenario: Off by default
- **WHEN** the server is started without the option
- **THEN** no tool carries `anthropic/alwaysLoad`

#### Scenario: Enabled for the embedded server
- **WHEN** the REST app is started with `--always-load-query`
- **THEN** its embedded MCP server's `query` tool carries `anthropic/alwaysLoad` set to true and no other tool does

### Requirement: Cypher filters are time-bounded
A search `cypher_filter` SHALL execute under a fixed time limit and without blocking other requests served by
the same process while it runs. A filter exceeding the limit SHALL fail with an error stating that the
`cypher_filter` timed out and naming the limit. Concurrent filters SHALL never share one in-flight database
connection, and rebuilding the search graph SHALL wait for an in-flight filter to finish.

#### Scenario: A runaway variable-length traversal times out
- **WHEN** a search runs `MATCH (a)-[:RELATIONSHIPS*]->(b) RETURN DISTINCT b.id AS id` over a model graph with cycles
  that would take longer than the limit
- **THEN** the search fails within the limit with the timed-out error

#### Scenario: Other requests proceed during a slow filter
- **WHEN** a slow `cypher_filter` is running and another request is served by the same process
- **THEN** the other request completes without waiting for the filter

#### Scenario: Concurrent filters each return their own result
- **WHEN** two searches with different `cypher_filter` values run concurrently
- **THEN** each returns exactly the ids its own filter selects

### Requirement: The order column's string form is advertised
The query input schema SHALL advertise an order entry's `field` as either a string (a column name or
an expression) or a column-reference object, and SHALL NOT publish the internal `raw_expression` field of
an order entry. Input carrying `raw_expression` — such as a stored query — SHALL still be accepted and
persisted unchanged.

#### Scenario: Schema shows both column forms
- **WHEN** a client lists the server's tools and reads the `query` tool's order entry schema
- **THEN** `field` accepts a string or a column-reference object, and no `raw_expression` property appears

#### Scenario: Stored order expressions still load
- **WHEN** a saved query whose order entry carries `raw_expression` is loaded and executed
- **THEN** it sorts by that expression exactly as before

### Requirement: Database failures are tool errors
When a database failure stops the `query`, `create_dataset`, `validate_datasets` or `ingest_datasource` tool from completing, the tool call SHALL fail as an MCP tool error (`isError: true`) whose text carries the friendly database-error message ("Database error: …"), never return that message as a successful result. A database rejection the tool reports as one of its own findings is not such a failure.

#### Scenario: Query hits a database failure
- **WHEN** a `query` call fails because the database rejects the generated SQL
- **THEN** the result has `isError: true` and its text holds the "Database error:" message

#### Scenario: Model creation hits a database failure
- **WHEN** `create_dataset`, `validate_datasets` or `ingest_datasource` fails because the database is unreachable
- **THEN** the result has `isError: true` and its text holds the "Database error:" message

#### Scenario: Rejected model SQL is a validation finding
- **WHEN** `validate_datasets` runs against a reachable database that rejects a SQL model's own SQL
- **THEN** the result is not an error and reports that model with an `invalid_sql` finding

### Requirement: Documented examples are written qualified
Every query, reference and expression example in the tool descriptions, their input schemas, the server instructions and the help topics SHALL be written in the qualified syntax of `queries/references`: every field and dataset metric written as `<dataset>.<name>` (or with an explicit route), every row count as `count(<dataset>.*)`, every later-stage reference as `<stage>.<column>`, and no bare name other than an alias declared by the same example.

#### Scenario: No example uses a pre-2.0 spelling
- **WHEN** every example query in the `query` tool description, its input schema, the server instructions and every help topic body is executed against a datasource seeded with the datasets it names
- **THEN** none emits a `deprecated_input` warning

#### Scenario: Unqualified row counts are not documented
- **WHEN** the same texts are collected
- **THEN** no example contains `count(*)`

### Requirement: create_datasource auto-ingest runs the idempotent ingest
When the `create_datasource` tool auto-ingests, it SHALL ingest the datasource exactly as `ingest_datasource` would with the same scope: it SHALL create missing datasets and additively merge existing ones, never overwriting a stored dataset's user metadata. Its result SHALL be the datasource line ("Datasource '<name>' created" or "replaced", with the connection test message) followed by the same rendering `ingest_datasource` returns, failed key reads included, plus the explore hint when datasets were created. A database failure during the ingest SHALL be reported as "Auto-ingestion failed: <friendly database error>" after the datasource line.

#### Scenario: Fresh create
- **WHEN** `create_datasource` creates a SQLite datasource over a database with tables `a` and `b`
- **THEN** datasets `a` and `b` are stored, and the result names both under the new-datasets section

#### Scenario: Re-create keeps dataset edits
- **WHEN** dataset `a` of datasource `app` is stored with a user-written description, and `create_datasource` is called again for `app`
- **THEN** the result says the datasource was replaced, and dataset `a` keeps its user-written description

#### Scenario: Database failure during ingest
- **WHEN** the ingest fails with a database error after the datasource is saved
- **THEN** the result contains "Auto-ingestion failed:" with the friendly database error, and the datasource is stored

### Requirement: The semantic-model tools fit the surface rules
The tools `create_semantic_model`, `edit_semantic_model` and `delete_semantic_model` SHALL obey every rule of this capability: their descriptions SHALL fit the client description budget, and every parameter SHALL be described in the input schema. `edit_dataset` and `create_dataset` SHALL describe their `semantic_model` behaviour, and `delete_semantic_model` SHALL state that it deletes the document's datasets.

#### Scenario: The new tools fit the budget
- **WHEN** the MCP server lists its tools
- **THEN** each semantic-model tool's description fits the budget and every parameter carries a description

### Requirement: Query guidance names bare model-scoped metrics and AGG
The query tool's schema guidance SHALL state, once, on the field that owns metric references, that a bare name is a query alias or a model-scoped metric and that `AGG(dataset.metric)` evaluates a dataset metric; a help topic SHALL hold the long-form reference for model-scoped metrics, reachable from the server instructions.

#### Scenario: A documented bare-metric query works as documented
- **WHEN** the documented example query naming a model-scoped metric by bare name runs on the demo
- **THEN** it returns the documented result

### Requirement: Startup preparation never delays the handshake
`slayer mcp` SHALL answer the MCP handshake (`initialize` and `tools/list`) without waiting for startup
preparation. Startup preparation is the demo setup requested by `--demo` followed by the startup ingest
requested by `--ingest-on-startup` or `SLAYER_INGEST_ON_STARTUP`; it SHALL run in the background once the
server starts. Every tool call SHALL wait until preparation has finished and then run normally. While a call
waits, the server SHALL send it progress notifications when the request carries a progress token. A
cancelled call SHALL NOT stop preparation for other calls. When the demo setup fails, the server SHALL keep
running and every tool call SHALL return the error `Failed to set up the Jaffle Shop demo: <reason>`, which
is also written to stderr. A startup-ingest failure — of a datasource or of individual tables — SHALL be
reported on stderr and SHALL NOT fail tool calls. Preparation SHALL write nothing to stdout. Without
startup preparation, tool calls SHALL NOT wait. `slayer serve` SHALL keep finishing its preparation before
it starts serving.

#### Scenario: The handshake answers during the demo build
- **WHEN** `slayer mcp --demo` starts on empty storage and the client sends `initialize` and `tools/list`
  while the demo is still being built
- **THEN** both answer without waiting for the build

#### Scenario: A tool call waits for the demo
- **WHEN** the client calls `query` on the demo while the demo is still being built
- **THEN** the call returns the query's rows once the build has finished

#### Scenario: Progress while waiting
- **WHEN** a tool call carrying a progress token waits for preparation
- **THEN** the client receives progress notifications for that call until it runs

#### Scenario: A cancelled call does not stop preparation
- **WHEN** one of two waiting tool calls is cancelled
- **THEN** preparation continues and the other call returns its result

#### Scenario: A failed demo setup becomes a tool error
- **WHEN** the demo setup fails
- **THEN** the server keeps running, every tool call returns `Failed to set up the Jaffle Shop demo:`
  followed by the reason, and stderr holds the reason

#### Scenario: Startup ingest runs behind the gate
- **WHEN** `slayer mcp --ingest-on-startup` starts and one configured datasource fails to ingest
- **THEN** the handshake answers without waiting for the ingest, stderr reports the failed datasource,
  and tool calls succeed once the ingest has finished

### Requirement: The ontology tools fit the surface rules
The tools `create_ontology`, `edit_ontology`, `delete_ontology`, `create_ontology_mapping`, `edit_ontology_mapping` and `delete_ontology_mapping` SHALL obey every rule of this capability: their descriptions SHALL fit the client description budget, and every parameter SHALL be described in the input schema. `inspect`'s `entity_type` description SHALL list `ontology` and `ontology_mapping`.

#### Scenario: The ontology tools fit the budget
- **WHEN** the MCP server lists its tools
- **THEN** each ontology tool's description fits the budget, every parameter carries a description, and `inspect`'s `entity_type` description names `ontology` and `ontology_mapping`

### Requirement: Search and validate_datasets describe their ontology use within the budget
The `search` tool's description of `entities` SHALL name the `concept:<Concept>` and `concept:<Ontology>.<Concept>` references, and the `validate_datasets` description SHALL state that it also reports ontology findings (identifier, multiplicity, constraint and required-link checks, and what is not checked), each within the client description budget.

#### Scenario: The descriptions fit and name the ontology use
- **WHEN** the MCP server lists its tools
- **THEN** `search`'s `entities` description names `concept:<Concept>`, `validate_datasets`' description names ontology findings, and both fit the budget

### Requirement: Every tool carries a title and behaviour annotations
Every tool in `tools/list` SHALL carry a non-empty human-readable `title` and annotations with all four hints
(`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) set explicitly, none left to the protocol
default. The hints SHALL follow these criteria, where *stored definitions* are the datasources, datasets (their
cached sampled values excluded), semantic models, ontologies, ontology mappings, memories and the datasource
priority list, and derived state (cached field samples, embeddings, search indexes) is not part of them:

- `readOnlyHint` is true iff the tool changes no stored definition;
- `destructiveHint` is true iff the tool can remove, replace or re-type existing stored content, a same-name
  replace included; a read-only tool sets it false;
- `idempotentHint` is true iff a repeat call with the same arguments leaves the stored definitions as the first
  call left them, a refusal or "not found" on the repeat included; a read-only tool sets it true;
- `openWorldHint` is true iff the tool can connect to a datasource's database; a call to an embedding provider
  does not count.

The tools, their titles and hints (R = read-only, D = destructive, I = idempotent, O = open-world; `-` = false)
SHALL be exactly:

| Tool | Title | R | D | I | O |
|------|-------|---|---|---|---|
| `query` | Query data | R | - | I | O |
| `inspect` | Inspect entity | R | - | I | O |
| `search` | Search memories and entities | R | - | I | O |
| `describe_datasource` | Describe datasource | R | - | I | O |
| `validate_datasets` | Validate datasets | R | - | I | O |
| `datasets_summary` | Summarize datasets | R | - | I | - |
| `list_datasources` | List datasources | R | - | I | - |
| `get_datasource_priority` | Get datasource priority | R | - | I | - |
| `recommend_population` | Recommend population | R | - | I | - |
| `create_dataset` | Create dataset | - | D | I | O |
| `edit_dataset` | Edit dataset | - | D | I | O |
| `create_datasource` | Create datasource | - | D | I | O |
| `ingest_datasource` | Ingest datasource | - | D | I | O |
| `edit_datasource` | Edit datasource | - | D | I | - |
| `set_datasource_priority` | Set datasource priority | - | D | I | - |
| `delete_dataset` | Delete dataset | - | D | I | - |
| `delete_datasource` | Delete datasource | - | D | I | - |
| `create_semantic_model` | Create semantic model | - | D | I | - |
| `edit_semantic_model` | Edit semantic model | - | D | I | - |
| `delete_semantic_model` | Delete semantic model | - | D | I | - |
| `create_ontology` | Create ontology | - | D | I | - |
| `edit_ontology` | Edit ontology | - | D | I | - |
| `delete_ontology` | Delete ontology | - | D | I | - |
| `create_ontology_mapping` | Create ontology mapping | - | D | I | - |
| `edit_ontology_mapping` | Edit ontology mapping | - | D | I | - |
| `delete_ontology_mapping` | Delete ontology mapping | - | D | I | - |
| `forget_memory` | Forget memory | - | D | I | - |
| `save_memory` | Save memory | - | D | - | - |

A tool registered without a title or without its four hints SHALL fail the server build.

#### Scenario: Titles and hints match the table
- **WHEN** a client lists the tools
- **THEN** the set of tool names equals the table's, and each tool's title and four hints equal its row, none of them null

#### Scenario: A tool without metadata cannot be registered
- **WHEN** the server registers a tool without a title or without its hints
- **THEN** building the server fails

### Requirement: Read-only tools leave stored definitions unchanged
A tool whose `readOnlyHint` is true SHALL leave every stored definition (as defined by the annotation criteria)
unchanged; it MAY refresh derived state such as cached field samples, embeddings and search indexes.

#### Scenario: Read-only calls on seeded storage
- **WHEN** each read-only tool is called with valid arguments against storage seeded with a datasource, table-backed and query-backed datasets, a memory and a datasource priority list
- **THEN** a snapshot of the stored definitions, taken without cached sampled values, is identical before and after the call

### Requirement: Closed-world tools open no database connection
A tool whose `openWorldHint` is false SHALL complete without connecting to any datasource's database.

#### Scenario: Closed-world calls with an unreachable database
- **WHEN** each closed-world tool is called with valid arguments against storage seeded with table-backed and query-backed datasets, and every attempt to obtain a database client fails
- **THEN** no database client is requested and the call does not fail for want of one

### Requirement: Tool output schemas stay the wrapped result string
Every tool SHALL advertise the output schema of a single required string property `result`, and no other output
schema.

#### Scenario: Output schemas
- **WHEN** a client lists the tools
- **THEN** every tool's output schema is an object whose only property is the required string `result`
