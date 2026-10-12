## MODIFIED Requirements

### Requirement: Server instructions fit the budget and name the help call
The server `instructions` SHALL be at most 2048 characters, SHALL state that one query can compute shares, ranks, trailing windows, period-over-period changes and cross-model aggregates (so answers are not assembled from several queries), SHALL give the reason in one plain sentence — aggregates over several joined models in one query are each computed over their own rows, so one query cannot fan out or double count, and a query is split only when a stage must re-query a whole result — and SHALL give the exact `inspect` call that fetches the help topics. Building the server with over-budget instructions SHALL fail.

#### Scenario: Instructions within budget and pointing at help
- **WHEN** a client initializes a session
- **THEN** the instructions are at most 2048 characters and contain an `inspect` call whose references are `memory:help.*` topics

#### Scenario: Instructions give the fan-out reason
- **WHEN** a client initializes a session
- **THEN** the instructions state that aggregates over several joined models in one query are each computed over their own rows, so one query cannot fan out or double count

### Requirement: Query-language guidance lives on the owning schema field, once
Each piece of query-language guidance SHALL appear in exactly one place across the `query` tool description and every description in its input schema (field- and definition-level), namely the field where an agent writes that value:
- `measures`: aggregation call form; arithmetic combining aggregates over different models, with a cross-model ratio example rooted at customers; `partition_by=` with keys spelled like the query's dimensions (dotted paths), a share-of-group example and `partition_by=[]` for the grand total; `window=` with compact durations (`'90d'`, `'3m'`); nested aggregation; one line per transform (`cumsum`, `change` / `change_pct`, `time_shift`, `lag` / `lead`, `rank` / `dense_rank` with required direction, `ntile`, `consecutive_periods`); a joined model's aggregate is computed over that model's own rows, broadcast across dimensions it cannot be attributed to unless `to_many_handling` is `associate`; aggregates over several child models in one query (orders and returns from customers) are each computed over their own rows; a child row without a parent counts in the NULL row of a parent dimension when that row exists and in an ungrouped total, and is never added again.
- `filters`: routing to row / aggregate / post-aggregation filtering; transforms in filters (top N per group via a rank); the anti-join (a joined model's key `is null`); the time-point forms.
- `order`: sorting by any aggregate or transform, displayed or not, with `limit` for top N; returning only the columns the question asks for, never adding the sort value to the measures.
- `time_dimensions`: buckets without rows are absent; a time dimension on `time_spine.timestamp` with a lower `date_range` bound returns every bucket, empty ones included, and lines several models up on one time axis.
- a time dimension's `date_range`: its forms; that `change`, `time_shift` and `window=` read rows before the range so the range is not widened; that `cumsum` starts at the range start.
- `source_model`: rooting at the model whose every row must appear when the question asks for every such row, with a named conditional aggregate; otherwise rooting where the facts are, or omitting it, so rows without facts drop out.
- `name` (stages): when stages are needed, and that an outer stage references inner dotted columns by their flattened `__` names.

The gap-filling, several-child-models, parentless-child-row, cross-model-ratio, projection and conditional-rooting guidance SHALL also appear in no help topic and not in the server `instructions`; the instructions' fan-out reason SHALL appear only in the instructions.

Fields that reuse another field's syntax (a refinement's clauses, a measure's `formula`) SHALL point to the owning field instead of repeating its guidance. Guidance too long for its field SHALL be cited by the exact `inspect` call of the help topic that holds it.

#### Scenario: Each guidance marker has one owner
- **WHEN** the `query` tool description and every description in its input schema are collected
- **THEN** each guidance marker (for example the grand-total `partition_by=[]` example, the "rows before the range" rule, the flattened `__` stage-column rule) occurs in its owning field's description and nowhere else

#### Scenario: Moved guidance has no second copy
- **WHEN** the `query` tool description, every description in its input schema, the server instructions and every help topic body are collected
- **THEN** each of the gap-filling (`time_spine.timestamp`), several-child-models, parentless-child-row, cross-model-ratio, projection and conditional-rooting markers occurs only in its owning field's description, and the fan-out-reason marker only in the instructions

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
- a filter on `rank(..., partition_by=<dimension path>, direction='desc') <= N` keeps the top N per group;
- `x / x(partition_by=<dimension path>)` and `x / x(partition_by=[])` give the share of group and of grand total;
- a joined model's aggregate sliced by a non-attributable dimension broadcasts its total by default and splits per group under `associate`;
- a nested aggregation averages the inner per-group values within the outer group;
- an outer stage reads an inner dotted dimension by its flattened `__` name;
- a monthly time dimension on `time_spine.timestamp` with a `date_range` returns every month of the range, months without rows included (count 0, other aggregates NULL);
- rooted at customers, `sum(orders.amount)` and `sum(returns.amount)` in one query each equal the sum over that child model's own rows;
- rooted at customers and grouped by a parent dimension, a child row without a parent counts in that dimension's NULL row when the population has one and in no row otherwise, and an ungrouped aggregate includes it;
- rooted at customers, `sum(orders.amount) / count(*)` divides the orders' total by the number of customers.

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
