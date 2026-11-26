# queries/ossie-l3-binding Specification

## Purpose
The strict Ossie Layer 3 binding: a query run with `conformance="ossie-l3"` is accepted only in the shape of Ossie's wide-table query interface (`wide_table_semantics.md` §3.7), refused with Ossie's §8 error codes in their order, and answered with Ossie's rows and output names, on every query surface.

## Requirements

### Requirement: The conformance is chosen beside the query
Every query surface SHALL take a conformance outside the query object, `slayer` (the default) or `ossie-l3`: the engine's `execute` argument `conformance`, the client's `query`, `sql` and `explain` (and their sync twins) argument `conformance`, the REST query route's URL parameter `conformance`, the MCP `query` tool's wrapper argument `conformance` (`mcp/query-tool`) and `slayer query --conformance` (`cli/commands`). Under `slayer` a query SHALL behave as it does without the argument. Any other value SHALL be refused with an error naming the two values, before the query is read. The value names no Ossie version: it tracks the newest Layer 3 version SLayer supports.

#### Scenario: Every surface reaches the same binding
- **WHEN** `{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue", "tickets.ticket_count"]}` runs on the Appendix B retail model with `ossie-l3` through the engine, the embedded client, the remote client, REST, MCP and the CLI
- **THEN** each refuses it with `E_PARTIAL_DIMENSION_REACHABILITY`

#### Scenario: REST takes the conformance in the URL
- **WHEN** the body `{"metrics": ["orders.total_revenue"]}` is posted to `/query?conformance=ossie-l3`
- **THEN** the query runs under the strict binding and the body is read as the query alone

#### Scenario: An unknown conformance is refused
- **WHEN** a query runs with `conformance="ossie"`
- **THEN** it fails with an error naming `slayer` and `ossie-l3`, and no query is read

### Requirement: Only Ossie's query shape is admitted
Under `ossie-l3` the query as submitted, before any conversion, normalization or migration, SHALL be an object with only the parts `dimensions`, `metrics` and `filter`, each optional: `dimensions` a list whose entries are reference strings or objects with exactly `field` and an optional `alias`, `metrics` a list whose entries are reference strings or objects with exactly `metric` and an optional `alias`, and `filter` one expression string. A reference SHALL be a dotted chain of identifiers; an alias SHALL be one identifier, regular or double-quoted. Anything else SHALL raise `E_INVALID_QUERY`: another key (every SLayer-only part, `population`, `time_dimensions`, `order`, `limit`, `offset`, `extensions`, `variables`, `to_many_handling`, `distinct_dimension_values`, `whole_periods_only`, `main_time_dimension`, `name`, included), a `null` anywhere, a list of queries, a part of the wrong type, an entry with another property, a reference that is an expression, call or literal, an alias that is not an identifier, a list of filters, and a filter that does not parse or is not an Ossie expression (Requirement: The filter is an Ossie expression). A query object built in Python SHALL be judged by the parts and entry properties it sets.

#### Scenario: An unsupported part is malformed
- **WHEN** `{"metrics": ["orders.total_revenue"], "limit": 10}` runs under `ossie-l3`
- **THEN** it fails with `E_INVALID_QUERY`, while under `slayer` it answers

#### Scenario: A null value is malformed
- **WHEN** `{"metrics": ["orders.total_revenue"], "filter": null}` runs under `ossie-l3`
- **THEN** it fails with `E_INVALID_QUERY`, not with a validation error of the native query model

#### Scenario: An expression in metric position is malformed
- **WHEN** `{"metrics": ["sum(orders.amount)"]}` runs under `ossie-l3`
- **THEN** it fails with `E_INVALID_QUERY`

#### Scenario: A list of queries or of filters is malformed
- **WHEN** a list of two query objects, or `{"metrics": ["orders.total_revenue"], "filter": ["orders.amount > 100"]}`, runs under `ossie-l3`
- **THEN** each fails with `E_INVALID_QUERY`

#### Scenario: An alias must be an identifier
- **WHEN** `{"metrics": [{"metric": "orders.total_revenue", "alias": "order amount"}]}` runs under `ossie-l3`
- **THEN** it fails with `E_INVALID_QUERY`; with the alias `"\"order amount\""` it answers, its column named `order amount`

### Requirement: Errors are Ossie's, in Ossie's order
Under `ossie-l3` a query meeting the condition of one or more rows below SHALL raise the code of the first such row, judging each row across the whole query: 1 `E_INVALID_QUERY` (Requirement: Only Ossie's query shape is admitted); 2 `E_EMPTY_AGGREGATION_QUERY`, neither dimensions nor metrics, absent and empty lists alike; 3 `E_NAME_NOT_FOUND`, a dimension, metric or filter reference that resolves to nothing in the model; 4 `E_METRIC_IN_DIMENSIONS`, a dimension naming a metric; 5 `E_FIELD_IN_METRICS`, a metric entry naming a field; 6 `E_AGGREGATE_IN_WHERE`, a filter with an aggregate or a metric reference; 7 `E_WINDOW_IN_WHERE`, a filter with a window function; 8 `E_CROSS_DATASET_FILTER`, a filter reading fields of more than one dataset; 9 `E_NAME_COLLISION` (Requirement: Result columns carry Ossie output names); 10 `E_UNAGGREGATED_FINER_GRAIN_REFERENCE`, 11 `E_DEFERRED_WINDOWED_METRIC` and 12 `E_AMBIGUOUS_MEASURE_GRAIN` (Requirement: Model-scoped metrics are judged by their components); 13 `E_NO_PATH`, 14 `E_PARTIAL_DIMENSION_REACHABILITY`, 15 `E_NO_ANCHOR`, 16 `E_UNSUPPORTED_FILTER_PATH` and 17 `E_AMBIGUOUS_PATH` (Requirement: Paths decide reachability and ambiguity). A reference SHALL resolve only as `dataset.field`, `dataset.metric` or a bare model-scoped metric name of the model, names matching as everywhere else (`queries/identifier-case`); a path of three or more segments and a bare field name resolve to nothing. The model SHALL be identified after rows 1 and 2: the query's `data_source`, else the datasource the native resolution picks, an ambiguous pick raising the native error.

#### Scenario: Malformed before unknown names
- **WHEN** `{"dimensions": ["customers.no_such_field"], "limit": 10}` runs under `ossie-l3`
- **THEN** it fails with `E_INVALID_QUERY`

#### Scenario: Unknown names before filter errors
- **WHEN** `{"dimensions": ["customers.no_such_field"], "filter": "orders.amount > 100 AND customers.region = 'EAST'"}` runs under `ossie-l3`
- **THEN** it fails with `E_NAME_NOT_FOUND`

#### Scenario: Unknown names before a window in the filter
- **WHEN** `{"dimensions": ["orders.no_such_field"], "filter": "ROW_NUMBER() OVER (ORDER BY orders.amount) = 1"}` runs under `ossie-l3`
- **THEN** it fails with `E_NAME_NOT_FOUND`; with `orders.order_id` in place of the unknown dimension it fails with `E_WINDOW_IN_WHERE`

#### Scenario: Each code on its own
- **WHEN** each of upstream's error cases `e03` to `e19` and `e22` to `e25` runs under `ossie-l3` on its fixture
- **THEN** it raises the code its `expected.txt` names

#### Scenario: Later rows yield to earlier ones
- **WHEN** a query meets two conditions among an aggregate in the filter and a cross-dataset filter, a name collision and a dimension no metric reaches, a dimension no metric reaches and one only some reach, or a filter path the anchor does not reach and an ambiguous path
- **THEN** it raises the earlier row's code of each pair: `E_AGGREGATE_IN_WHERE`, `E_NAME_COLLISION`, `E_NO_PATH`, `E_UNSUPPORTED_FILTER_PATH`

### Requirement: Reachability errors name what fails to reach
An `E_NO_PATH` or `E_PARTIAL_DIMENSION_REACHABILITY` error SHALL name the first dimension in query order whose dataset the condition holds for and the first metric in query order that does not reach it; an `E_UNSUPPORTED_FILTER_PATH` error SHALL name the filter and the first metric in query order, or the anchor, that does not reach the filter's dataset; an `E_AMBIGUOUS_PATH` error SHALL name the two datasets and the paths between them, or the datasets that qualify as the anchor. Each error SHALL suggest a remedy where one exists. The names SHALL also be exposed as the error's `dimension`, `filter`, `metric` and `anchor` attributes.

#### Scenario: Partial reachability names the dimension and the metric
- **WHEN** `{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue", "tickets.ticket_count"]}` runs under `ossie-l3`
- **THEN** the error names `customers.region` and `tickets.ticket_count`

#### Scenario: A filter path names the anchor
- **WHEN** `{"dimensions": ["customers.region"], "filter": "orders.amount > 100"}` runs under `ossie-l3`
- **THEN** `E_UNSUPPORTED_FILTER_PATH` names the filter and the anchor `customers`

### Requirement: The strict binding reads the model as an Ossie model
The model SHALL be the identified datasource with everything it holds, SLayer-only content included: fields, dataset metrics, datasets of any source, and relationships' join types and cardinalities, which the model requests explicitly through its `SLAYER` extensions (Ossie §9). A field SHALL belong to the dataset declaring it; a dataset-scoped metric SHALL be one component homed on its dataset, whatever its expression. An **Ossie step** SHALL cross a relationship from its many side to its one side: a relationship of a semantic-model document in its `from` → `to` direction, whatever its `SLAYER` entry declares; any other relationship as Ossie export orients it — a many-to-one or one-to-one relationship forward, a one-to-many relationship backward, one of undetermined cardinality in the direction its declared keys prove, otherwise no step. A many-to-many relationship and a relationship of a dataset to itself SHALL give no step. A **path** SHALL be a sequence of Ossie steps with no repeated dataset; the path from a dataset to itself SHALL be empty.

#### Scenario: A one-to-many relationship is a backward step
- **WHEN** standalone `customers` declares a one-to-many relationship to `orders` and `{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}` runs under `ossie-l3`
- **THEN** it answers, `orders` reaching `customers` by one step

#### Scenario: A many-to-many relationship gives no path
- **WHEN** the only relationship between `orders` and `customers` is many-to-many and the same query runs under `ossie-l3`
- **THEN** it fails with `E_NO_PATH`

#### Scenario: A document relationship keeps its direction
- **WHEN** a document's `orders_to_customers` (from `orders` to `customers`) carries a `SLAYER` entry declaring it one-to-many, and the same query runs under `ossie-l3`
- **THEN** `orders` still reaches `customers` by one step

#### Scenario: SLayer-only content is honoured
- **WHEN** `orders` declares a SLayer-only field `customer_region` defined as `customers.region` and a SLayer-only dataset metric `revenue_share` defined with a transform, and `{"dimensions": ["orders.customer_region"], "metrics": ["orders.revenue_share"]}` runs under `ossie-l3`
- **THEN** `customer_region` is a field of `orders`, `revenue_share` one component homed on `orders`, and the query answers as it does under `slayer`, keyed `customer_region`, `revenue_share`

### Requirement: The filter is an Ossie expression
Under `ossie-l3` the filter SHALL be an OSSIE_SQL_2026 expression (Ossie Layer 3 §1.4). A construct SHALL be admitted only when Ossie Layer 1 defines it and SLayer gives it Ossie's meaning; every other construct SHALL raise `E_INVALID_QUERY`: SLayer's list functions and list children, the regex operator `~`, the JSON operator `->>`, a single-string `IN '…'`, deprecated spellings, transforms, a `{variable}` placeholder left unbound, and a string literal compared with a temporal field that is not an ISO-8601 date or timestamp (period and relative tokens such as `'2025-Q1'` and `'last month'`). A scalar function SHALL be admitted only at an argument count Ossie Layer 1 defines for it (`LOG(base, x)` but not `LOG(x)`, `ROUND(x, d)` but not `ROUND(x)`), and a call at an argument count SLayer does not accept SHALL raise `E_INVALID_QUERY`. `CAST` SHALL be admitted to every target type Ossie Layer 1 names, `TIMESTAMP_NTZ` included. An aggregate SHALL count as an Ossie construct only when Ossie Layer 1 defines its spelling and shape and SLayer evaluates it. An aggregate whose arguments are not Ossie's (one value, `COUNT(*)`, two for `APPROX_PERCENTILE`, `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)`) SHALL raise `E_INVALID_QUERY`, windowed or not. An Ossie construct SLayer does not support yet SHALL raise `E_INVALID_QUERY` as a property the engine does not support (§3.5). Aggregates and metric references SHALL raise `E_AGGREGATE_IN_WHERE` and window functions `E_WINDOW_IN_WHERE`, at their rows. A window function SHALL be read as Ossie Layer 1 writes it, `function(args) OVER ([PARTITION BY …] [ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]] [ROWS|RANGE frame])`, so the rest of the filter and everything inside the window are judged at their own rows before row 7; a malformed window, a function that is not one of Ossie's ranking, offset or aggregate functions (or takes other arguments than Ossie's), a window inside a window and a frame whose bounds are out of ANSI order SHALL raise `E_INVALID_QUERY`. A windowed aggregate SHALL raise `E_WINDOW_IN_WHERE`, not `E_AGGREGATE_IN_WHERE`. A filter with no field references SHALL have no dataset and SHALL apply in every branch. The native conformance SHALL evaluate a window in a filter (`queries/window-functions`); judging a filter under `ossie-l3` SHALL NOT change what the same filter does under `slayer`.

#### Scenario: SLayer-only constructs are refused
- **WHEN** a strict query's filter is `orders.status ~ '^c'`, `contains(orders.tags, 'x')`, `orders.order_date >= '2025-Q1'` or `orders.order_date >= 'last month'`
- **THEN** each fails with `E_INVALID_QUERY`, and each answers under `slayer`

#### Scenario: Ossie constructs are admitted
- **WHEN** a strict query's filter uses comparison, `BETWEEN`, `IN (…)`, `LIKE`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `CASE WHEN`, arithmetic, `CAST`, an ISO date literal compared with a date field, or an Ossie scalar function SLayer implements
- **THEN** it answers with the rows Ossie defines

#### Scenario: A filter without fields reaches unconnected metrics
- **WHEN** `{"metrics": ["orders.total_revenue", "tickets.ticket_count"], "filter": "FALSE"}` runs under `ossie-l3` on the retail model
- **THEN** it returns one row with `total_revenue` NULL and `ticket_count` 0

#### Scenario: Ossie's argument counts decide admission
- **WHEN** a strict query's filter is `LOG(10, orders.amount) > 2`, and others are `LOG(orders.amount) > 2`, `ROUND(orders.amount / 120.0) = 1`, `TRUNC(orders.amount / 120.0) = 0`, `SUBSTRING(orders.status, 1) = 'complete'` and `REGEXP_LIKE(orders.status, '^C', 'i')`
- **THEN** the first answers with the rows Ossie defines, and each other fails with `E_INVALID_QUERY` and answers under `slayer`

#### Scenario: A TIMESTAMP_NTZ cast is admitted
- **WHEN** a strict query's filter is `orders.order_date > CAST('2025-01-01 00:00:00' AS TIMESTAMP_NTZ)`
- **THEN** it answers with the rows Ossie defines, as under `slayer`

#### Scenario: Aggregates SLayer cannot evaluate are refused
- **WHEN** a strict query's filter is `APPROX_PERCENTILE(orders.amount, 0.5) > 1` or `percentile_disc(orders.amount, 0.5) > 1`, and another's is `MEDIAN(orders.amount) > 1`
- **THEN** the first two fail with `E_INVALID_QUERY`, and the third with `E_AGGREGATE_IN_WHERE`

#### Scenario: A wrong argument count is refused
- **WHEN** a strict query's filter is `ABS(orders.amount, 1) > 0` or `UPPER(orders.status, 1) = 'X'`
- **THEN** each fails with `E_INVALID_QUERY`, never with an uncoded error

#### Scenario: A malformed window is malformed
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `ROW_NUMBER() OVER (`, `FOO() OVER ()`, `ROW_NUMBER(orders.amount) OVER ()`, `SUM(ROW_NUMBER() OVER ()) OVER ()`, `ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) = 1` or `ROW_NUMBER() OVER (ORDER BY orders.amount GROUPS UNBOUNDED PRECEDING) = 1`
- **THEN** each fails with `E_INVALID_QUERY`

#### Scenario: Names inside a window resolve before the window
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `ROW_NUMBER() OVER (ORDER BY orders.no_such_field) = 1`
- **THEN** it fails with `E_NAME_NOT_FOUND`; with `SUM(orders.amount) OVER (PARTITION BY orders.total_revenue) > 10` it fails with `E_AGGREGATE_IN_WHERE`

#### Scenario: The rest of a windowed filter is judged
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `orders.status ~ '^c' AND ROW_NUMBER() OVER (ORDER BY orders.amount) = 1`
- **THEN** it fails with `E_INVALID_QUERY`

#### Scenario: A complete window is a window in the filter
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `SUM(orders.amount) OVER (PARTITION BY orders.status) > 10`, `AVG(orders.amount) OVER (ORDER BY orders.amount DESC NULLS LAST ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) > 1`, or `ROW_NUMBER() OVER (ORDER BY orders.amount) = {n}` with the variable `n` bound to 1
- **THEN** each fails with `E_WINDOW_IN_WHERE`

#### Scenario: An aggregate takes Ossie's arguments
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `SUM(orders.amount, orders.amount) > 1` or `SUM(orders.amount, orders.amount) OVER () > 1`
- **THEN** each fails with `E_INVALID_QUERY`; `COUNT(*) > 1` fails with `E_AGGREGATE_IN_WHERE`

#### Scenario: The native binding evaluates a window in the filter
- **WHEN** a query with the dimension `orders.order_id` and the filter `ROW_NUMBER() OVER (ORDER BY orders.amount) = 1` runs under `slayer`, before or after the same filter is judged under `ossie-l3`
- **THEN** under `slayer` it returns the one order with the smallest amount both times, and under `ossie-l3` it fails with `E_WINDOW_IN_WHERE`

### Requirement: Model-scoped metrics are judged by their components
Each aggregate call of a model-scoped metric SHALL be a component homed on the one dataset its arguments read, an `AGG(D.m)` call a component homed on `D`, and the metric's datasets its components' homes; this analysis SHALL cover every model-scoped metric of the model, those of `SLAYER` entries included. Under `ossie-l3` a query naming a model-scoped metric SHALL raise `E_UNAGGREGATED_FINER_GRAIN_REFERENCE` when its expression reads a field outside every aggregate, `E_DEFERRED_WINDOWED_METRIC` when it contains a window function, and `E_AMBIGUOUS_MEASURE_GRAIN` when an aggregate's arguments read several datasets or none, or its `FILTER` reads another dataset than its arguments — never the native error a query reading the metric would otherwise raise. A windowed field and a dataset-scoped windowed metric SHALL be evaluated under `ossie-l3` as under `slayer`.

#### Scenario: The model-scoped restrictions
- **WHEN** `amount_less_order`, `running_revenue`, `order_less_return` and `row_count` of the retail model are each queried under `ossie-l3`
- **THEN** they raise `E_UNAGGREGATED_FINER_GRAIN_REFERENCE`, `E_DEFERRED_WINDOWED_METRIC`, `E_AMBIGUOUS_MEASURE_GRAIN` and `E_AMBIGUOUS_MEASURE_GRAIN`

#### Scenario: Components aggregate their own datasets
- **WHEN** `{"metrics": ["revenue_per_customer"]}` runs under `ossie-l3` on the retail model
- **THEN** it returns 162.5 (650 / 4), the customer count including the customer without orders

#### Scenario: A dataset-scoped windowed metric answers under strict
- **WHEN** dataset `orders` of the retail model gains the metric `revenue_rank` with expression `RANK() OVER (ORDER BY SUM(amount) DESC)`, and `{"dimensions": ["orders.customer_id"], "metrics": ["orders.revenue_rank"]}` runs under `ossie-l3` and under `slayer`
- **THEN** both return the same rows, and `{"metrics": ["running_revenue"]}` still raises `E_DEFERRED_WINDOWED_METRIC` under `ossie-l3` while answering under `slayer`

### Requirement: Paths decide reachability and ambiguity
Under `ossie-l3` a metric SHALL reach a dataset only when a path leads to it from every one of the metric's datasets. With metrics, the query SHALL raise `E_NO_PATH` when some dimension's dataset no metric reaches, `E_PARTIAL_DIMENSION_REACHABILITY` when some dimension's dataset some but not all metrics reach, and `E_UNSUPPORTED_FILTER_PATH` when the filter has a dataset some metric does not reach. Without metrics, the **anchor** SHALL be the dataset of a dimension from which a path leads to every other dimension's dataset; none SHALL raise `E_NO_ANCHOR`, several `E_AMBIGUOUS_PATH`, and a filter dataset the anchor does not reach `E_UNSUPPORTED_FILTER_PATH`. Two or more paths from a metric's dataset, or from the anchor, to a dimension's or the filter's dataset SHALL raise `E_AMBIGUOUS_PATH`, even when one path is shorter; paths between datasets the query does not join SHALL not matter.

#### Scenario: One-to-many paths are refused
- **WHEN** `{"dimensions": ["orders.order_id"], "metrics": ["customers.customer_count"]}` runs under `ossie-l3`
- **THEN** it fails with `E_NO_PATH` naming `orders.order_id` and `customers.customer_count`, while under `slayer` it answers with a warning

#### Scenario: A shorter path does not settle ambiguity
- **WHEN** `{"dimensions": ["date_dim.year"], "metrics": ["store_sales.total_sales"]}` runs under `ossie-l3` on the `stores_ambiguous` model
- **THEN** it fails with `E_AMBIGUOUS_PATH`, and the same query on `stores` answers

#### Scenario: A second path elsewhere does not matter
- **WHEN** `{"dimensions": ["date_dim.year"], "metrics": ["store_returns.total_returns"]}` runs under `ossie-l3` on `stores_ambiguous`
- **THEN** it answers

#### Scenario: Unconnected metrics without dimensions
- **WHEN** `{"metrics": ["orders.total_revenue", "tickets.ticket_count"]}` runs under `ossie-l3` on the retail model
- **THEN** it returns one row, `total_revenue` 650 and `ticket_count` 0

### Requirement: Admitted queries return Ossie's rows
A query the strict binding admits SHALL be answered by the native evaluation of the same query without a population (`queries/population` › Rows of an inferred query are the row supply), which SHALL return the rows of Ossie's lowering (§5): one row per distinct dimension tuple of the branches, NULLs equal, a missing branch row taking each component's empty-input value, a filter applied in every branch. A native error raised after admission SHALL propagate unchanged.

#### Scenario: Upstream's row cases
- **WHEN** each of upstream's row cases runs under `ossie-l3` on its fixture
- **THEN** it returns exactly its expected rows, `a16_shared_dimensions_stores` excepted until DEV-2388

#### Scenario: Three branches give each key once
- **WHEN** `{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue", "returns.total_returns", "visits.visit_count"]}` runs under `ossie-l3` on the retail model
- **THEN** it returns `EAST 350 25 0`, `WEST 300 NULL 1`, `NORTH NULL 10 2`

### Requirement: Result columns carry Ossie output names
Under `ossie-l3` each entry's **output name** SHALL be its alias, its quotes removed, else the model's spelling of the last segment of its parsed reference (a quoted name containing a dot keeps its whole name). Two entries whose output names are equal after Layer 1 identifier normalization — a regular identifier upper-cased, a quoted one taken exactly — SHALL raise `E_NAME_COLLISION` before the query runs. A column SHALL be labelled by its output name, except that where two distinct output names would give one label, a regular identifier's column SHALL be labelled by its normalized spelling. The result's rows, columns and attribute metadata SHALL be keyed by the output names, dimensions first, then metrics, each in query order, on every surface. Under `slayer` result keys SHALL stay the native ones.

#### Scenario: Aliases name the columns
- **WHEN** `{"dimensions": [{"field": "customers.region", "alias": "area"}], "metrics": [{"metric": "orders.total_revenue", "alias": "revenue"}]}` runs under `ossie-l3`
- **THEN** the columns are `area`, `revenue`

#### Scenario: Last segments collide
- **WHEN** `orders` and `customers` each declare a field `id`, and `{"dimensions": ["orders.id", "customers.id"]}` runs under `ossie-l3`
- **THEN** it fails with `E_NAME_COLLISION` and runs no SQL, while under `slayer` it returns two columns

#### Scenario: Normalization decides a collision
- **WHEN** a strict query has entries aliased `foo` and `"FOO"`, or `foo` and `"foo"`
- **THEN** the first pair fails with `E_NAME_COLLISION`; the second answers, the regular identifier's column labelled by its normalized spelling `FOO` beside `foo`, since two columns never share a label

### Requirement: Saved queries run by name under the strict binding
A saved query run by name under `ossie-l3` SHALL be judged and answered by these rules on its stored stages after any `refine` merge: it SHALL have exactly one stage; the stored `name`, `version` and a `population` marked `population_inferred` SHALL be ignored, and a stored filter holding one condition SHALL count as the filter string; any other stored part, an explicit `population` included, SHALL raise `E_INVALID_QUERY`.

#### Scenario: A saved Ossie-shaped query runs strictly
- **WHEN** a saved query storing `{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}` runs by name under `ossie-l3`, alone and with `refine={"metrics": ["returns.total_returns"]}`
- **THEN** it returns the Ossie rows keyed `region`, `total_revenue` (and `total_returns`)

#### Scenario: A saved query with an explicit population is refused
- **WHEN** a saved query naming `population: "customers"` runs by name under `ossie-l3`
- **THEN** it fails with `E_INVALID_QUERY`

### Requirement: Variables bind before the filter is judged
Under `ossie-l3` the run's `variables` SHALL substitute the filter's placeholders as literals before the filter is judged; a query-level `variables` part SHALL raise `E_INVALID_QUERY`.

#### Scenario: A placeholder bound by the run's variables
- **WHEN** `{"metrics": ["orders.total_revenue"], "filter": "customers.region = '{r}'"}` runs under `ossie-l3` with `variables={"r": "EAST"}`
- **THEN** it returns 350

### Requirement: Explain and dry runs judge first
Under `ossie-l3` a dry run and an explain SHALL raise what the run would raise; for an admitted query the explain SHALL expose its lowering.

#### Scenario: A refused query is refused by explain
- **WHEN** `{"metrics": ["orders.total_revenue"], "limit": 10}` is explained under `ossie-l3`
- **THEN** it fails with `E_INVALID_QUERY`

### Requirement: Strict refusals are one typed error on every surface
A strict refusal SHALL be an `OssieQueryError`, a `SlayerError`, exposing `code` (one of the 17 §8 codes) and the attributes of Requirement: Reachability errors name what fails to reach, with the first line `OssieQueryError: <code>: <summary>`. REST SHALL answer it with status 400 and a `detail` object holding `code`, `message` and those attributes; the remote client SHALL raise it again as an `OssieQueryError` with the same code and attributes; MCP and the CLI SHALL report its text.

#### Scenario: The REST body carries the code
- **WHEN** `{"metrics": ["orders.no_such_metric"]}` is posted to `/query?conformance=ossie-l3`
- **THEN** the response is 400 with `detail.code` `E_NAME_NOT_FOUND`, and the remote client raises `OssieQueryError` with that code
