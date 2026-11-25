## MODIFIED Requirements

### Requirement: Ambiguous first and last names dispatch by argument shape
`first` and `last` SHALL always be aggregations — there is no `first` / `last` transform.
The call SHALL parse to one node whose home is decided by the bound shape of its first
argument, which binds exactly like any aggregation source: an argument with a row-level
leaf (a column, a literal, an expression over columns, or a mixed expression whose
attached values are broadcast onto the rows) SHALL rank its home's rows; an argument with
no row-level leaf — an aggregation, a saved measure, or a grained transform, alone or
composed through arithmetic and scalar calls — SHALL be the second-order ranked
aggregation over that operand dataset's cells (per `queries/semantics` › first and last
over an aggregate rank their operand's cells). The dispatch SHALL NOT depend on the
spelling of the argument, and SHALL NOT pre-empt transform-input validation: an
ill-formed transform inside the argument fails with that transform's own error. A
dataset-defined custom aggregation named `first` or `last` SHALL shadow the built-in.

#### Scenario: Aggregation reading
- **WHEN** a measure is written `last(balance)` or `last(balance, updated_at)`
- **THEN** it is the `last` aggregation — the latest `balance` ranked by the time
  axis, or by `updated_at`

#### Scenario: Transform reading
- **WHEN** a query over a month time dimension has a measure written `last(sum(revenue))`
- **THEN** it is the second-order `last` over the `sum(revenue)` cells, ranked by the
  month bucket, with the values the former `last` transform returned

#### Scenario: Saved-measure operand reads as the transform
- **WHEN** the model declares a measure `rev` with formula `sum(revenue)` and a query over
  a month time dimension has a measure written `first(rev)` or `first(rev * 2)`
- **THEN** it is the second-order `first` over the saved measure's cells — the same plan
  and executed values as `first(sum(revenue))` / `first(sum(revenue) * 2)` on SQLite and
  DuckDB — never an unknown-reference error

#### Scenario: Row-grain composite keeps the aggregation reading
- **WHEN** a measure is written `first(quantity * avg(unit_price, partition_by=product))`
  or `first(1)`
- **THEN** it is the `first` aggregation over the home's rows, picking the row-level value
  at the ranked row, by executed values — never the second-order reading

#### Scenario: Dispatch does not pre-empt transform-input validation
- **WHEN** a measure is written `first(cumsum(weight))` with `weight` an unprojected
  row-level column
- **THEN** it fails at plan time with the grain-refining row-level-leaf error naming
  `cumsum` — never a ranked pick

#### Scenario: A custom first or last shadows the built-in
- **WHEN** a dataset defines the custom aggregation `last` with formula `MAX({value})` and
  a query on it selects `last(balance)`
- **THEN** each cell carries its maximum `balance`, the custom formula rendered — never
  the built-in ranked pick

### Requirement: Positional parameters fold onto declared parameter order
An aggregation call MAY pass declared parameters positionally after its source
(or, for a re-aggregation, after its operand): positional values SHALL bind to
the aggregation's declared parameter order — the built-in registry
(`percentile` → `p`; `weighted_avg` → `weight`; `corr`/`covar_samp`/`covar_pop`
→ `other`) or a custom aggregation's `params` declaration order — yielding the
identical aggregation identity, SQL, results, and result keys as the named
spelling. Passing a parameter both positionally and by name, or more positional
values than declared parameters — any positional value at all on an aggregation
that declares none — SHALL fail with a clear error naming the rule. Ranked
`first`/`last` declare no parameters — they take at most one positional value,
their ranking key (resolved per › Default ranking key when omitted), which, when
given, SHALL be determined by the aggregation's home per › An expression ranking key is
judged by its leaves — never a literal-only expression. After binding an
attached (aggregate- or transform-valued) parameter is therefore always a named
parameter.

#### Scenario: Positional percentile equals named
- **WHEN** a measure is written `percentile(price, 0.9)`
- **THEN** SQL, results, and result keys are identical to `percentile(price, p=0.9)`

#### Scenario: Positional parameter on a re-aggregation outer
- **WHEN** a measure is written
  `percentile(sum(amount, partition_by=[city, region]), 0.9)`
- **THEN** it equals the `p=0.9` spelling by executed values

#### Scenario: Custom aggregation binds positionals by declared order
- **WHEN** a model declares `wavg(weight)` and a measure is written `wavg(amount, id)`
- **THEN** it equals `wavg(amount, weight=id)` by executed values

#### Scenario: Duplicate and excess positional parameters error
- **WHEN** `percentile(price, 0.9, p=0.5)` or `percentile(price, 0.9, 0.5)` is submitted
- **THEN** each fails with a clear error naming the duplicated parameter or the
  declared-parameter count

#### Scenario: Positional value on a parameterless aggregation errors
- **WHEN** `sum(amount, 1)` or `sum(customers.spend, sum(customers.spend, partition_by=status))`
  is submitted, under any `to_many_handling` mode
- **THEN** each fails at bind with a clear error naming the aggregation and that it
  takes no parameters — never an executed value

#### Scenario: Invalid first/last ranking key errors
- **WHEN** `last(amount, 1)`, `last(sum(amount), 1)` or `last(amount, id, amount)` is
  submitted
- **THEN** each fails at bind with a clear error naming the ranking-key rule

#### Scenario: Expression ranking key
- **WHEN** a measure is written `last(balance, coalesce(updated_at, created_at))`
- **THEN** it is the `balance` of the row latest by the coalesced timestamp, by
  executed values on SQLite and DuckDB

#### Scenario: Positional transform parameter equals named
- **WHEN** a measure is written
  `weighted_avg(customers.spend, rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
  rooted at `orders`
- **THEN** it binds to the identical aggregation identity as the `weight=rank(..., direction='desc')`
  spelling and returns identical result keys and values

### Requirement: Default ranking key
A ranked `first` / `last` aggregation over rows without an explicit ranking key SHALL
rank its home's rows by,
in order: the first entry of the query's `dimensions` (in projection order) that is
row-level and types DATE or TIMESTAMP — a column or a computed dimension, never a
granularity bucket, which ties every row of a cell; else the first time dimension's un-truncated source, column or
expression; else the model's default time dimension; else it SHALL fail with the
existing ranking-key resolution error. The query is the one the aggregation is a
measure of — each stage its own — whatever the aggregation's `partition_by=`,
`window=`, `time_shift` or nesting inside another aggregation. A cross-model
aggregation SHALL read each candidate in its home's coordinates, skipping one its home
does not determine, and fall back to its home model's default time dimension. A ranked
aggregation over an aggregate operand without an explicit ranking key SHALL rank the
operand's cells by the query's active time bucket (each stage its own); when that bucket
is not a member of the operand's grain, or the query has no time dimension, it SHALL
fail at plan time with a `TimeAxisError`. A defaulted key is dropped from the grain like
an explicit one (`queries/semantics` › A ranked aggregation's grain drops its ranking
key).

#### Scenario: A temporal computed dimension is the default ranking key
- **WHEN** a query groups by the computed dimension `coalesce(shipped_at, created_at)`
  (typed TIMESTAMP) and a month time dimension on `created_at`, and selects
  `last(amount)`
- **THEN** the key is a query dimension and is dropped from the grain: each month's
  value is the `amount` of its row latest by the coalesced timestamp, broadcast onto
  every coalesced-timestamp row of that month with no warning, by executed values on
  SQLite and DuckDB

#### Scenario: An expression time dimension's source is the default ranking key
- **WHEN** a query's only temporal dimension is the time dimension
  `coalesce(shipped_at, created_at)` at `month` and it selects `last(amount)`
- **THEN** each month's value is the `amount` of its row latest by the un-truncated
  coalesced timestamp

#### Scenario: A partition coarser than the query reads the query's temporal dimension
- **WHEN** a query on `orders` groups by `region` and the computed dimension
  `e = coalesce(shipped_at, created_at)` with a month time dimension on `created_at`,
  and selects `last(amount, partition_by=region)`
- **THEN** US is 60 and EU is 50 (the rows latest by `e`, the non-temporal `region`
  listed first notwithstanding), never US 40 by the model default `created_at`, by
  executed values on SQLite and DuckDB

#### Scenario: A partition coarser than the query reads the time dimension's source
- **WHEN** a query on `orders` (default time dimension `created_at`) groups by `region`
  with a month time dimension on `shipped_at`, and selects
  `last(amount, partition_by=region)`
- **THEN** the bucket is skipped and its source ranks: US is 60 and EU is 50 by
  `shipped_at`, never US 40 by `created_at`

#### Scenario: No model default with a partition coarser than the query
- **WHEN** `orders` declares no default time dimension and a query groups by `region`
  with a month time dimension on `created_at`, selecting
  `last(amount, partition_by=region)`
- **THEN** it ranks by `created_at`: US is 40 and EU is 50, instead of failing with the
  ranking-key resolution error

#### Scenario: A ranked aggregation nested in a second-order aggregation
- **WHEN** the query of the coarser-partition scenario selects
  `avg(last(amount, partition_by=[region, product]), partition_by=region)`
- **THEN** the inner `last` ranks by `e`: US is 45 and EU is 35, never US 35 by
  `created_at`

#### Scenario: A windowed partitioned ranked aggregation
- **WHEN** the query of the coarser-partition scenario selects
  `last(amount, window='90d', partition_by=region)`
- **THEN** each interval's rows rank by `e`: the US February cells are 60, never 40 by
  the window axis `created_at`

#### Scenario: A shifted partitioned ranked aggregation
- **WHEN** `orders` declares the default time dimension `shipped_at` and a query groups
  by `region` with a year time dimension on `created_at`, selecting
  `time_shift(first(amount, partition_by=[region, created_at]), 1)`
- **THEN** the US 2024 cell is 30 — the US 2025 value ranked by `created_at` — never 40 by
  the model default `shipped_at`

#### Scenario: Each stage reads its own dimensions
- **WHEN** a first stage on `orders` groups by `id`, `region`, `e` and `created_at` and
  selects `last(amount, partition_by=region)`, and a second stage over it groups by its
  `region` and `created_at` and selects `last(id, partition_by=region)`
- **THEN** the first stage's US value is 60 (by `e`) and the second stage's US value is
  4 (by its own `created_at`), never 6 by the first stage's `e`

#### Scenario: A cross-model aggregation reads a query dimension its home determines
- **WHEN** a query on `orders` groups by `customers.region` with a month time dimension
  on `customers.updated_at`, and selects
  `last(customers.spend, partition_by=customers.region)` (`customers` declares no
  default time dimension)
- **THEN** North is 100 and South is 200, ranked by `customers.updated_at`, instead of
  failing with the ranking-key resolution error

#### Scenario: A cross-model aggregation reads a computed query dimension
- **WHEN** a query on `orders` groups by `customers.region` and the computed dimension
  `coalesce(customers.updated_at, customers.signup_ts)`, selecting
  `last(customers.spend, partition_by=customers.region)`
- **THEN** North is 300 and South is 200, ranked by the coalesced timestamp

#### Scenario: A cross-model aggregation skips a query dimension its home does not determine
- **WHEN** `customers` declares the default time dimension `signup_ts`, and a query on
  `orders` groups by `customers.region`, `orders.created_at` and `customers.updated_at`
  in that order, selecting `last(customers.spend, partition_by=customers.region)`
- **THEN** `orders.created_at` is skipped and `customers.updated_at` ranks: North is
  100 and South is 200, never North 300 by the default `signup_ts`

#### Scenario: A cross-model aggregation falls back to its home's default
- **WHEN** `customers` declares the default time dimension `signup_ts`, and a query on
  `orders` groups by `customers.region` with a month time dimension on
  `orders.created_at`, selecting `last(customers.spend, partition_by=customers.region)`
- **THEN** North is 300 and South is NULL, ranked by `signup_ts`

#### Scenario: A defaulted query dimension crossing a fanning hop fails closed
- **WHEN** a query on `accounts` (`accounts → orders` one-to-many) groups by
  `accounts.id` and `orders.created_at`, selecting
  `last(accounts.spend, partition_by=accounts.id)`
- **THEN** it fails at plan time with the ranked-input safety error naming `created_at`
  and the hop to `orders`, as the same query without `partition_by=` does

#### Scenario: An aggregate operand ranks by the active time bucket
- **WHEN** on the snapshot fixture of `queries/partitioned-aggregates` › Re-aggregation over
  ranked and windowed operands, a query by `customer_id` with a month time dimension on
  `snapshot_date` selects `last(sum(balance))`
- **THEN** every row of customer 100 reads 160 and every row of customer 200 reads NULL —
  its February cell, whose value is NULL, is picked — never 70, by executed values on
  SQLite and DuckDB, with no broadcast warning

#### Scenario: An operand without the active time bucket fails
- **WHEN** the same query selects `last(sum(balance, partition_by=customer_id))`, and a
  query by `customer_id` with no time dimension selects `last(sum(balance))`
- **THEN** each fails at plan time with a `TimeAxisError`, never a value

### Requirement: An expression ranking key is judged by its leaves
An explicit or defaulted ranking key SHALL be determined by the aggregation's home
and never widen it (semantics Axiom 2.4) — one predicate for both forms — judged over
every leaf the expression reads and every dependency of a derived column it names,
determination being closed under row-level combination. Over rows the key SHALL be
a column of the home, a field reached from it over provably to-one hops, or an
attached value (an aggregate or a transform) whose grain the home determines, broadcast
onto the home's rows before the ranking exactly as a mixed-source constituent is. Over
an aggregate operand the key SHALL be determined by the operand's grain (`queries/semantics`
Axiom 1): a member of the grain, a field the grain determines over an entity key or provably
to-one hops, or an attached value whose grain the operand's grain determines. A literal-only
key stays refused (› Positional
parameters fold onto declared parameter order). For local and cross-model
(target-rooted) producers alike, a key reading a leaf across a fanning or unproven
hop from the home SHALL fail with the existing ranked-input safety error naming that
leaf and the hop, and any other key the home does not determine SHALL fail at plan
time with the same typed error naming the key. Two ranked aggregations differing only in their ranking key SHALL
stay separate producers; equal keys, however spelled, SHALL share one.

#### Scenario: Fanning leaf inside an expression ranking key
- **WHEN** a query rooted at `customers` selects
  `last(spend, coalesce(orders.created_at, signup_ts))`, `customers → orders`
  one-to-many
- **THEN** it fails at plan time with the ranked-input safety error naming
  `orders.created_at` and the hop, never a value

#### Scenario: Cross-model ranked aggregate with an expression key
- **WHEN** a query rooted at `orders` selects
  `last(customers.spend, coalesce(customers.updated_at, customers.signup_ts), partition_by=region)`
- **THEN** it executes ranked inside the `customers`-rooted producer, by executed values

#### Scenario: An attached aggregate ranks the rows
- **WHEN** on the Appendix B seed of `queries/population` › Rows of an inferred query are
  the row supply, a query groups by `customers.segment` and selects
  `last(orders.amount, orders.amount - avg(orders.amount, partition_by=customers.region))`
- **THEN** retail reads 200 and corporate 50 — the amount of the order furthest above its
  region's average — equal to the hand-computed oracle on SQLite and DuckDB

#### Scenario: An attached transform ranks the rows
- **WHEN** on the Appendix B seed a query groups by `customers.segment` and selects
  `last(orders.amount, rank(sum(orders.amount, partition_by=orders.id), direction='desc'))`
- **THEN** retail reads 100 and corporate 50 — the amount of the order ranked last — equal
  to the hand-computed oracle on SQLite and DuckDB, never a refusal

#### Scenario: An aggregate ranks an aggregate's cells
- **WHEN** `orders` holds the rows (EAST, P1, revenue 100, units 5), (EAST, P2, 40, 9),
  (WEST, P1, 70, 2) and (WEST, P3, 10, 1), and a query by `region` and `product` selects
  `last(sum(revenue), sum(units), partition_by=region)`
- **THEN** both EAST rows read 40 and both WEST rows 70 — the revenue of the region's
  product with the most units — with no broadcast warning, on SQLite and DuckDB

#### Scenario: A grain member ranks an aggregate's cells
- **WHEN** on the snapshot fixture of `queries/partitioned-aggregates` › Re-aggregation
  over ranked and windowed operands, a query by a month time dimension on `snapshot_date`
  selects `first(sum(balance, partition_by=[customer_id, snapshot_date]), customer_id)`
- **THEN** January reads 370 and February 160 — customer 100's monthly total — by executed
  values on SQLite and DuckDB

#### Scenario: A field the operand grain determines ranks its cells
- **WHEN** a query by `customers.region` selects
  `last(sum(orders.amount, partition_by=customers.id), customers.signup_ts, partition_by=customers.region)`
- **THEN** each region's value is the sum of its customer latest by `signup_ts`, identical
  to ranking by `max(customers.signup_ts, partition_by=customers.id)`; the same key over
  an operand grained by `customers.region` alone fails with the ranking-key safety error

#### Scenario: A key the operand does not determine fails closed
- **WHEN** the same query selects
  `last(sum(balance, partition_by=[customer_id, snapshot_date]), account_id)` or
  `last(sum(balance, partition_by=customer_id), sum(balance, partition_by=account_id))`
- **THEN** each fails at plan time with the ranked-input safety error naming the key,
  never a value

#### Scenario: An attached key the row home does not determine fails closed
- **WHEN** on the Appendix B seed a query groups by `customers.region` and selects
  `last(customers.segment, count(orders.*, partition_by=orders.status))`
- **THEN** it fails at plan time with the ranked-input safety error naming the key — the
  `customers` home does not determine `orders.status` — never a value

### Requirement: first and last never pick a NULL ranking key
A ranked `first` / `last` aggregation (plain, partitioned, windowed, cross-model, or over an aggregate operand, whose cells it ranks as rows) SHALL pick only among the rows whose ranking key is non-NULL: `first` the row with the smallest key, `last` the row with the largest, on every dialect and independent of its native NULL ordering or session settings. A cell whose rows all have a NULL ranking key SHALL take the empty value, NULL. A NULL value under a non-NULL key SHALL be picked like any other value. Over an aggregate operand, a cell in the NULL time bucket has a NULL ranking key.

#### Scenario: NULL keys are skipped in both directions
- **WHEN** a customer's rows are `(updated_at 2024-01-01, status 'a')`, `(NULL, 'b')`, `(2024-03-01, 'c')` and a query selects `first(status, updated_at)` and `last(status, updated_at)`
- **THEN** they are `'a'` and `'c'` on SQLite, DuckDB, Postgres and every other Tier-1 dialect

#### Scenario: All keys NULL
- **WHEN** a customer's rows all have a NULL `updated_at`
- **THEN** `first(status, updated_at)` and `last(status, updated_at)` are NULL for that customer

#### Scenario: NULL value under a non-NULL key
- **WHEN** the latest-by-`updated_at` row's `status` is NULL
- **THEN** `last(status, updated_at)` is NULL, not an earlier row's status

#### Scenario: Partitioned and cross-model
- **WHEN** a query rooted at `orders` selects `last(customers.status, customers.updated_at, partition_by=region)` over customers some of whose `updated_at` are NULL
- **THEN** each region's value is the status of its latest non-NULL-keyed customer, by executed values on SQLite and DuckDB

#### Scenario: A NULL time bucket is never picked over an aggregate
- **WHEN** a query rooted at `customers` selects `last(sum(customers.spend))` by month of
  `orders.ordered_at`, a customer without orders (spend 55) falling in the NULL bucket
  beside monthly sums 100, 150, 100, 110 for 2024-01 to 2024-04
- **THEN** every row reads 110 on SQLite and DuckDB, never 55
