## ADDED Requirements

### Requirement: Rank-family ordering direction

`rank` and `dense_rank` SHALL take a required keyword argument `direction` whose
value is a string literal `asc`, `desc`, `ascending` or `descending` (any case,
surrounding whitespace ignored), normalised to `asc` / `desc`, and SHALL order
their window by the inner value ascending for `asc` and descending for `desc`.
`direction` SHALL combine with `partition_by=` and SHALL accept any orderable
inner, numeric or not. `ntile` and `percent_rank` SHALL reject `direction` and
SHALL always order ascending, so `ntile` bucket 1 holds the lowest values and a
higher value never gets a lower `percent_rank`. A missing, unrecognised,
non-literal or forbidden `direction` SHALL fail before any SQL runs with a
`TransformArgumentError` (a `QueryTypeError`), in every position (measure,
filter, order, computed dimension, aggregation parameter, saved `ModelMeasure`);
the missing-direction message SHALL show both
`direction='asc'` (lowest first) and `direction='desc'` (highest first). The
importer formula validator SHALL apply the identical rule with the identical
error. `rank(x, direction='asc')` and `rank(x, direction='desc')` SHALL be
distinct values that never deduplicate into one.

Values below use the DEV-1847 `sales` fixture, whose region totals are North
90, South 140, East 180, Gap 20 and Void NULL.

#### Scenario: Ascending rank puts the lowest value first

- **WHEN** a query over `[region]` selects `rank(sum(amount), direction='asc')`
- **THEN** the window orders the inner ascending and the ranks are Gap 1, North 2,
  South 3, East 4, Void NULL on SQLite and DuckDB

#### Scenario: Descending rank puts the highest value first

- **WHEN** a query over `[region]` selects `rank(sum(amount), direction='desc')`
- **THEN** the window orders the inner descending and the ranks are East 1, South 2,
  North 3, Gap 4, Void NULL

#### Scenario: dense_rank takes the same direction

- **WHEN** a query over `[region]` selects `dense_rank(sum(amount), direction='asc')`
- **THEN** the ranks are Gap 1, North 2, South 3, East 4, Void NULL

#### Scenario: Direction synonyms normalise

- **WHEN** a query selects `rank(sum(amount), direction=' Descending ')`
- **THEN** it binds to the identical value as `rank(sum(amount), direction='desc')`

#### Scenario: Non-numeric inner ranks ascending

- **WHEN** a query over `[region]` selects `rank(min(city), direction='asc')`
- **THEN** the ranks are North 1 and South 1 (both `Alpha`), East 3 (`Delta`), Gap 4
  (`Kappa`), Void 5 (`Xi`)

#### Scenario: Direction combines with partition_by

- **WHEN** a query over `[region, city]` selects
  `rank(sum(amount), partition_by=region, direction='asc')`
- **THEN** each city ranks lowest-first within its region: East Delta 1, Epsilon 1,
  Zeta 3; North Alpha 1, Beta 2; South Alpha 1, Gamma 2; Gap Kappa 1, the NULL city
  2; Void Xi NULL

#### Scenario: Both directions in one query stay distinct

- **WHEN** one query selects both `rank(sum(amount), direction='asc')` and
  `rank(sum(amount), direction='desc')` unnamed
- **THEN** the result carries two columns with the ascending and descending ranks
  above, never one deduplicated column

#### Scenario: Missing direction fails naming both spellings

- **WHEN** a query selects, filters on, or orders by `rank(sum(amount))` or
  `dense_rank(sum(amount), partition_by=region)`, or queries a saved
  `ModelMeasure` whose formula is `rank(sum(amount))`
- **THEN** it fails with a `TransformArgumentError` naming the transform and showing
  `direction='asc'` (lowest first) and `direction='desc'` (highest first), and no SQL
  runs

#### Scenario: Unrecognised or non-literal direction fails

- **WHEN** a query selects `rank(sum(amount), direction='up')` or
  `rank(sum(amount), direction=region)`
- **THEN** it fails with a `TransformArgumentError` listing the accepted values

#### Scenario: ntile and percent_rank reject direction

- **WHEN** a query selects `ntile(sum(amount), n=2, direction='desc')` or
  `percent_rank(sum(amount), direction='asc')`
- **THEN** it fails with a `TransformArgumentError` stating that the transform always
  orders ascending and takes no `direction`

#### Scenario: ntile and percent_rank order ascending

- **WHEN** a query over `[region]` selects `ntile(sum(amount), n=2)` and
  `percent_rank(sum(amount))`
- **THEN** `ntile` is Gap 1, North 1, South 2, East 2, Void NULL and `percent_rank` is
  Gap 0, North 1/3, South 2/3, East 1, Void NULL

#### Scenario: Importer validation shares the rule

- **WHEN** the importer formula validator parses `rank(sum(amount))`,
  `rank(sum(amount), direction='sideways')` or `ntile(sum(amount), n=4, direction='asc')`
- **THEN** each fails with the same `TransformArgumentError` the query binder raises
  for the same formula

#### Scenario: Window ordering is pinned across dialects

- **WHEN** `rank` with each direction, `dense_rank`, `ntile` and `percent_rank` are
  rendered for postgres, sqlite, duckdb, tsql and bigquery
- **THEN** each window orders the inner by the stated direction (`ASC` for `ntile` /
  `percent_rank`) and the generated SQL matches recorded golden baselines

### Requirement: Rank-family NULL inputs rank NULL

For `rank`, `dense_rank`, `ntile` and `percent_rank`, a row whose inner value is
NULL SHALL get a NULL result. NULL rows SHALL NOT take a rank position or an
`ntile` bucket, nor count in `percent_rank`'s denominator, within each partition;
the non-NULL rows SHALL rank exactly as if the NULL rows were absent. The result
SHALL be identical on every supported dialect, independent of the dialect's
native NULL ordering.

#### Scenario: Mixed NULL and non-NULL inners

- **WHEN** a query over `[region]` selects `rank(sum(amount), direction='desc')`,
  `percent_rank(sum(amount))` and `ntile(sum(amount), n=2)` on the `sales` fixture
- **THEN** Void (NULL total) gets NULL for all three, and the other regions get the
  values of the ordering-direction scenarios above, `percent_rank`'s denominator
  counting four rows, on SQLite and DuckDB

#### Scenario: An all-NULL partition ranks NULL without disturbing others

- **WHEN** a query over `[region, city]` selects
  `dense_rank(sum(amount), partition_by=region, direction='desc')`
- **THEN** Void Xi (NULL total) is NULL, and every other region's cities rank as they
  would without Void

#### Scenario: A NULL row inside a partition takes no position

- **WHEN** a query over `[region, city]` selects
  `rank(city, partition_by=region, direction='asc')`
- **THEN** Gap's NULL city is NULL and Kappa is 1; East Delta 1, Epsilon 2, Zeta 3

#### Scenario: A filter on rank drops NULL-ranked rows

- **WHEN** a query over `[region]` filters `rank(sum(amount), direction='asc') <= 5`
- **THEN** East, Gap, North and South survive and Void does not

#### Scenario: NULL handling does not depend on the dialect's NULL ordering

- **WHEN** the rank family is rendered for tsql, whose native ordering puts NULLs
  first on `ASC`
- **THEN** the emitted SQL nulls the result for a NULL inner and keeps NULL rows out of
  the non-NULL rows' window, exactly as on postgres

### Requirement: Stored rank calls without a direction load as descending

A persisted model, query or memory whose stored schema version predates this
change SHALL load with every `rank(` / `dense_rank(` call lacking a top-level
`direction=` in its Mode-B fields rewritten to carry `direction='desc'`,
preserving its pre-change ordering. The Mode-B fields are `ModelMeasure.formula`
and a query's `measures`, `filters`, `dimensions`, `time_dimensions`, `order` and
`main_time_dimension`, including queries nested in `source_queries`, an inline
query `source_model`, and `Memory.query`. Mode-A SQL (`Column.sql`, model
`filters`, `Column.filter`, aggregation templates) and `ntile` / `percent_rank`
calls SHALL never be rewritten. The rewrite SHALL apply only to a payload read
from storage or one that declares an explicit schema version older than the
current one; a payload without a version, or at the current version, SHALL be
left as written, so a bare call in it fails with the missing-direction error. The
rewrite SHALL be idempotent, SHALL leave a formula it cannot tokenise
byte-identical, and a migrated model SHALL be persisted back at the current
version on first load.

#### Scenario: A stored model measure keeps its descending meaning

- **WHEN** a model stored at the previous version holds the measure
  `rank(sum(amount))` and is loaded
- **THEN** the measure reads `rank(sum(amount), direction='desc')`, the stored document
  is rewritten at the current version, and querying it returns the descending ranks

#### Scenario: A stored query's every Mode-B field is rewritten

- **WHEN** a stored query-backed model's `source_queries` entry holds bare
  `rank(` / `dense_rank(` calls in a measure, a filter, an order item and a computed
  dimension expression, nested inside other calls and colon syntax
- **THEN** every call gains `direction='desc'` and nothing else in the formulas changes

#### Scenario: Unversioned legacy documents are rewritten

- **WHEN** a stored model with no `version`, whose nested source query also has no
  `version`, or a stored memory (YAML and SQLite) with no `version` or with an
  unversioned `query`, holds bare `rank(` calls
- **THEN** they load with `direction='desc'` filled in

#### Scenario: A fresh payload is never filled in

- **WHEN** a query or model with no `version`, or at the current version, is
  submitted through the API, MCP or Python with a bare `rank(sum(amount))`
- **THEN** it fails with the missing-direction `TransformArgumentError`

#### Scenario: A payload declaring an old version is treated as legacy

- **WHEN** a query submitted with an explicit older `version` holds `rank(sum(amount))`
- **THEN** it is filled in with `direction='desc'`, exactly as a stored document would be

#### Scenario: Calls that need no rewrite are untouched

- **WHEN** a stored document holds `rank(sum(amount), direction='asc')`, the text
  `rank(` inside a string literal, an attribute call `x.rank(`, `ntile(sum(amount), n=4)`,
  `percent_rank(sum(amount))`, or `dense_rank() over (order by id)` in a `Column.sql`
- **THEN** each is left byte-identical

#### Scenario: The rewrite is idempotent

- **WHEN** an already-migrated document is migrated again
- **THEN** it is unchanged

#### Scenario: An untokenisable formula still loads

- **WHEN** a stored model's measure formula cannot be tokenised
- **THEN** the formula is left byte-identical and the model still loads

## MODIFIED Requirements

### Requirement: Transforms reject grain-refining row-level leaves

A transform other than the aggregation-dispatched `first` / `last`, used in
measure, filter, or order position, or as a constituent of an aggregation
source, SHALL reject with a typed plan-time error — before any SQL is
generated — any row-level (non-aggregate) leaf in its input that refines the
consumer grain, that is, a leaf that is not itself a projected query
dimension. The error SHALL name the transform, the offending leaf, and the
remedies (aggregate the leaf, e.g. `cumsum(weight:sum)`; project it as a query
dimension; or compute it in an earlier `source_queries` stage), and cite no
tracking issue. The rule applies uniformly to every such transform op — the
rank family and the shift family (`time_shift`, `change`, `change_pct`)
included. Leaves that are projected grain keys — plain or computed dimensions
— remain legal, evaluated at the query grain. The raw source column of a
bucketed time dimension is not a projected grain key (it refines the bucket).
`first` / `last` keep their aggregation dispatch, and the stricter
dimension-position rules are unchanged.

#### Scenario: Bare grain-refining leaf rejected
- **WHEN** a query with a month time dimension and no `weight` dimension selects
  the measure `cumsum(weight)`
- **THEN** it fails at plan time with the typed error naming the remedy — never
  SQL whose base grain is inflated to one row per (bucket, weight-value)

#### Scenario: Rank family is covered
- **WHEN** a query over `[store]` with a month time dimension selects the
  measure `rank(qty, direction='desc')`
- **THEN** it fails with the same typed error, never a result carrying one row
  per (store, month, qty-value)

#### Scenario: Shift family is covered
- **WHEN** a query with a month time dimension and no `weight` dimension
  selects `time_shift(weight, -1)`, `change(weight)` or `change_pct(weight)`,
  or `time_shift(hi_rev, -1)` over the unprojected derived column `hi_rev`
- **THEN** each fails at plan time with the same typed error naming the
  transform, the leaf and the remedies — never a result whose row count differs
  from the same query without the measure

#### Scenario: Predicate over an unprojected row column rejected
- **WHEN** a query selects the measure `consecutive_periods(weight > 0)` with
  `weight` not a query dimension
- **THEN** it fails with the same typed error naming the aggregate-the-leaf
  remedy

#### Scenario: A projected grain key stays legal
- **WHEN** a query projects `weight` as a dimension and selects the measure
  `rank(weight, direction='desc')`
- **THEN** it compiles at the query grain and executes with correct values —
  no error, no extra result rows

#### Scenario: A projected grain key stays legal under the shift family
- **WHEN** a query over `[store]` with a month time dimension selects
  `time_shift(store, -1)`
- **THEN** it compiles at the query grain and executes with the operand's own
  value per row — no error, no extra result rows

#### Scenario: Attached values do not launder a row leaf
- **WHEN** a query selects the measure
  `cumsum(weight * avg(unit_price, partition_by=product))` with `weight` not
  projected
- **THEN** it fails with the same typed error — a transform does not collapse
  row grain, unlike an aggregation

#### Scenario: Row leaf under a transform inside an aggregation source rejected
- **WHEN** a query selects the measure `sum(cumsum(weight) - 1)` with `weight` not a
  query dimension
- **THEN** it fails at plan time with the same typed error naming the transform and
  the aggregate-the-leaf remedy — an enclosing aggregation does not launder the
  transform's row leaf

#### Scenario: Projected grain key under a transform inside a source stays legal
- **WHEN** a query over `[region]` selects the measure `sum(rank(region, direction='desc'))`
- **THEN** it compiles: the transform types at the query grain and the aggregation is
  the degenerate identity with the degenerate-re-aggregation warning, never an error

### Requirement: Rank-family partition keys are operand-grain members
A rank-family transform (`rank`, `dense_rank`, `percent_rank`, `ntile`) partitions its
operand's cells, so every key in its own `partition_by=` SHALL be a member of the
transform's operand grain, in every position. The operand grain is the union over the
transform's input: an aggregate contributes its explicit `partition_by=` keys, else the
query grain (its dimensions and time buckets), plus the query's active time bucket when
it is windowed; a nested transform contributes its own operand grain, minus its time
axis when it is `first` or `last`; a composite contributes the union of its operands,
with a projected row-level leaf contributing itself; an input with no aggregate (only
row-level leaves or literals) is the query grain, since it is evaluated once per
query-grain cell. A non-member key SHALL fail at plan time with an error naming
the transform, the key, the operand grain and the remedy (add the key to the inner
aggregate's `partition_by=`, or partition by a member). A key that is no query dimension
at all keeps the existing "not a query dimension" error, and an ungrained inner aggregate
in dimension position keeps the existing grain-self-containment error; both take
precedence over the membership error.

#### Scenario: Non-member query dimension as a measure
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=product, direction='desc')`
- **THEN** planning fails with an error naming `rank`, `product`, the operand grain `city, region` and the `partition_by=` remedy — it never executes by widening the grain

#### Scenario: Non-member query dimension in a filter
- **WHEN** the same query filters `rank(sum(amount, partition_by=[city, region]), partition_by=product, direction='desc') <= 2`
- **THEN** planning fails with the same error

#### Scenario: Non-member key in dimension position, plain column
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount, partition_by=[city, product]), partition_by=region, direction='desc')`
- **THEN** planning fails with the same error naming `region` and the grain `city, product`, never with an internal producer-slot error

#### Scenario: Non-member key in dimension position, computed-dimension name
- **WHEN** a query declares `ureg` = `upper(region)` and the dimension `rank(sum(amount, partition_by=[city, region]), partition_by=ureg, direction='desc')`
- **THEN** planning fails with the same error naming the key and the grain `city, region`, never with an internal error

#### Scenario: Member key executes
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=region, direction='desc')`
- **THEN** each row carries its (city, region) total's rank within the region: East Zeta 1, Delta 2, Epsilon 2; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap the NULL city 1, Kappa 2; Void Xi NULL (its total is NULL)

#### Scenario: Ungrained inner keeps the query-dimension rule
- **WHEN** a query over the banded dimension alone selects `rank(sum(amount), partition_by=region, direction='desc')`
- **THEN** planning fails with the existing "partition_by column 'region' is not a query dimension" error listing the available dimensions; over `[region, band]` the same measure executes because the ungrained inner is grained at the query grain and `region` is a member

#### Scenario: Windowed inner admits the active bucket
- **WHEN** a monthly query selects `rank(sum(amount, window='1y', partition_by=customers.regions.name), partition_by=ordered_at, direction='desc')`
- **THEN** the partition key passes the operand-grain rule as the query's month bucket, a member contributed by the windowed inner

#### Scenario: Nested collapsing transform drops its axis
- **WHEN** a monthly query selects `rank(last(sum(amount, partition_by=[customers.regions.name, ordered_at])), partition_by=ordered_at, direction='desc')`
- **THEN** planning fails with the operand-grain error naming `ordered_at` and the grain `customers.regions.name`; with `partition_by=customers.regions.name` the key passes the rule

#### Scenario: Aggregate-free input takes the query grain
- **WHEN** a query over `[city, region, product]` selects `rank(city, partition_by=region, direction='desc')`
- **THEN** it executes, ranking each row's city value descending within its region: East Zeta 1, Epsilon 2, Delta 3; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap Kappa 1, the NULL city NULL; Void Xi 1

#### Scenario: Residue error precedes the membership rule
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount), partition_by=region, direction='desc')`
- **THEN** planning fails with the existing grain-self-containment error ("must declare partition_by= explicitly"), not the operand-grain error
