## MODIFIED Requirements

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
- `measures`: aggregation call form; arithmetic combining aggregates over different models, with a cross-model ratio example rooted at customers; `partition_by=` with keys spelled like the query's dimensions (dotted paths), a share-of-group example and `partition_by=[]` for the grand total; `window=` with compact durations (`'90d'`, `'3m'`); nested aggregation; one line per transform (`cumsum`, `change` / `change_pct`, `time_shift`, `lag` / `lead`, `rank` / `dense_rank` with required direction, `ntile`, `consecutive_periods`); a joined model's aggregate is computed over that model's own rows, broadcast across dimensions it cannot be attributed to unless `to_many_handling` is `associate`; aggregates over several child models in one query (orders and returns from customers) are each computed over their own rows; a child row without a parent counts in the NULL row of a parent dimension when that row exists and in an ungrouped total, and is never added again.
- `dimensions`: every result row needs a naturally readable label — group by a name, never by an id alone unless ids are asked for, and by the id and the name when a name may repeat.
- `filters`: routing to row / aggregate / post-aggregation filtering; transforms in filters (top N per group via a rank); the anti-join (a joined model's key `is null`); the time-point forms.
- `order`: sorting by any aggregate or transform, displayed or not, with `limit` for top N; returning only the columns the question asks for, never adding the sort value to the measures.
- `time_dimensions`: buckets without rows are absent; a time dimension on `time_spine.timestamp` with a lower `date_range` bound returns every bucket, empty ones included, and lines several models up on one time axis.
- a time dimension's `date_range`: its forms; that `change`, `time_shift` and `window=` read rows before the range so the range is not widened; that `cumsum` starts at the range start.
- `source_model`: rooting at the model whose every row must appear when the question asks for every such row, with a named conditional aggregate; otherwise rooting where the facts are, so rows without facts drop out, or omitting it to infer the smallest model determining every dimension and row-level condition column.
- `name` (stages): what needs no stage (top N per group, averaging or bucketing per-entity totals, shares and totals); that a filter on a transform's value applies after it, over all rows; that a stage is needed only to drop rows by a dimension or time condition that a cross-row value must still see, or to join or reuse a whole result, because a row filter or `date_range` applies before a running total, `lag` / `lead`, rank, `ntile`, `consecutive_periods` or `partition_by` total while `change`, `time_shift` and `window=` read past `date_range` or a filter on their own time dimension only; and that an outer stage references inner dotted columns by their flattened `__` names.

The one-query rule SHALL appear only in the server `instructions` and the `query` tool description. The gap-filling, several-child-models, parentless-child-row, cross-model-ratio, projection, conditional-rooting and stage guidance SHALL also appear in no help topic and not in the server `instructions`; the instructions' fan-out reason SHALL appear only in the instructions. No description, the instructions or a help topic SHALL tell agents to use a stage to filter after a rank or window, to re-query a whole result, or to answer with one query only "whenever" it can.

Fields that reuse another field's syntax (a refinement's clauses, a measure's `formula`) SHALL point to the owning field instead of repeating its guidance. Guidance too long for its field SHALL be cited by the exact `inspect` call of the help topic that holds it.

#### Scenario: Each guidance marker has one owner
- **WHEN** the `query` tool description and every description in its input schema are collected
- **THEN** each guidance marker (for example the grand-total `partition_by=[]` example, the "rows before the range" rule, the flattened `__` stage-column rule, the stage rule, the readable-label rule) occurs in its owning field's description and nowhere else

#### Scenario: Moved guidance has no second copy
- **WHEN** the `query` tool description, every description in its input schema, the server instructions and every help topic body are collected
- **THEN** each of the gap-filling (`time_spine.timestamp`), several-child-models, parentless-child-row, cross-model-ratio, projection, conditional-rooting and stage-rule markers occurs only in its owning field's description, the fan-out-reason marker only in the instructions, and the one-query marker only in the instructions and the `query` description

#### Scenario: No retired stage or one-query wording remains
- **WHEN** the same texts are collected
- **THEN** none tells agents to use stages to filter after a window or rank or to re-query a whole result, none says to answer with one query "whenever" it can, and no help topic keeps a "One query first" section or a "Typical uses" list of stages

#### Scenario: Examples group by readable columns
- **WHEN** the same texts are collected
- **THEN** no example groups, partitions or flattens by a column whose name ends in `_id`

#### Scenario: A refinement clause points to the query field
- **WHEN** a client reads the `refine` argument's `measures` description
- **THEN** it refers to the query object's `measures` instead of repeating its guidance

#### Scenario: date_range is described
- **WHEN** a client reads a time dimension's `date_range` description
- **THEN** it states the accepted forms, that `change` / `time_shift` / `window=` read rows before the range, and that `cumsum` starts at the range start

### Requirement: Documented query idioms behave as documented
Every query idiom given as an example in the tool or schema descriptions SHALL execute and return the documented result:
- `change`, `change_pct` and `time_shift` with a `date_range` compare the first in-range bucket against the bucket before the range;
- an aggregation with `window=` includes rows before the range in the first in-range buckets;
- `cumsum` with a `date_range` accumulates from the range start only;
- a filter `<joined model>.<key> is null` keeps exactly the rows with no joined row;
- a query rooted at a model keeps every row of that model, counting 0 where no joined rows exist;
- ordering by an undisplayed aggregate with a `limit` returns the top rows by that aggregate without projecting it;
- a filter on `rank(..., partition_by=customers.regions.name, direction='desc') <= N` keeps the top N per region;
- `x / x(partition_by=customers.regions.name)` and `x / x(partition_by=[])` give the share of region and of grand total;
- a joined model's aggregate sliced by a non-attributable dimension broadcasts its total by default and splits per group under `associate`;
- a nested aggregation averages the inner per-group values within the outer group;
- an outer stage reads an inner dotted dimension by its flattened `__` name (`customers.regions.name` as `customers__regions__name`);
- a monthly time dimension on `time_spine.timestamp` with a `date_range` returns every month of the range, months without rows included (count 0, other aggregates NULL);
- rooted at customers, `sum(orders.amount)` and `sum(returns.amount)` in one query each equal the sum over that child model's own rows;
- rooted at customers and grouped by a parent dimension, a child row without a parent counts in that dimension's NULL row when the population has one and in no row otherwise, and an ungrouped aggregate includes it;
- rooted at customers, `sum(orders.amount) / count(*)` divides the orders' total by the number of customers;
- a filter on a transform's value (`rank(...) <= N`, `cumsum(...) > X`) applies after the transform, computed over all rows;
- a time filter on the bucketed time column restricts `cumsum`, `lag`, `lead`, `consecutive_periods`, `rank` / `ntile` over buckets and `partition_by` totals to the kept buckets, while `change`, `time_shift` and `window=` read past it;
- a filter on a dimension restricts `rank`, `dense_rank`, `ntile`, `partition_by` shares and totals and nested aggregates to the kept peers; a filter on a non-time column restricts the rows `window=` reads, before the range as well;
- an inner stage computing a rank over all rows and an outer stage filtering one row returns that row's rank among all rows.

#### Scenario: Lookback before date_range
- **WHEN** a monthly query with `date_range` `"2025"` measures `change(sum(amount))`
- **THEN** January 2025's change is computed against December 2024

#### Scenario: Running total starts at the range
- **WHEN** a monthly query with `date_range` `"2025"` measures `cumsum(sum(amount))`
- **THEN** January 2025's value excludes 2024 amounts

#### Scenario: Anti-join
- **WHEN** a query rooted at customers filters `orders.id is null`
- **THEN** exactly the customers with no orders are returned

#### Scenario: Cross-model aggregate broadcast vs associate
- **WHEN** a query rooted at orders, grouped by order status, measures `sum(customers.credit)`
- **THEN** by default each row carries the total credit of all customers with a broadcast warning, and with `to_many_handling` `associate` each row carries the credit of the distinct customers with orders in that status

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
- **WHEN** a query rooted at customers measures `sum(orders.amount) / count(*)`
- **THEN** the value equals the orders' total divided by the number of customers

#### Scenario: A filter on a transform's value applies after it
- **WHEN** a monthly query over all months measures `cumsum(sum(amount))` and filters on that running total exceeding a threshold
- **THEN** the kept months carry the running total accumulated over every month, not only the kept ones

#### Scenario: A time filter restricts cross-row values except calendar look-backs
- **WHEN** a monthly query filters the bucketed time column to two months and measures one cross-row value
- **THEN** `cumsum`, `lag`, `lead`, `consecutive_periods`, `rank` / `ntile` over months and `sum(amount, partition_by=[])` are computed over the two kept months only, while `change`, `time_shift` and `window=` equal their values in the unfiltered query

#### Scenario: A dimension filter restricts peer values
- **WHEN** a query grouped by region name and customer name filters one customer and measures a rank, dense rank, ntile, share of region, grand total or nested average
- **THEN** the value is computed over the kept customer only

#### Scenario: Rank among all rows needs a stage
- **WHEN** an inner stage ranks every customer by total amount and an outer stage keeps one customer
- **THEN** that customer carries its rank among all customers
