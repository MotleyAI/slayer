# queries/time-dimensions Specification

## Purpose
The `TimeDimension` input contract: which keys may name the time column, and how the
object is serialized and advertised, independent of any one query surface.

## Requirements

### Requirement: column is an accepted alias of dimension

A time dimension SHALL accept its column under either the key `dimension` or the key
`column`, with identical coercion (a bare string or a column-reference object).
Serialization SHALL always emit `dimension`, and the JSON schema SHALL advertise
`dimension` as the property.

#### Scenario: column key validates and dumps as dimension

- **WHEN** a query is constructed with
  `time_dimensions` = `[{"column": "created_at", "granularity": "month"}]`
- **THEN** the time dimension's column is `created_at`, and dumping the query emits
  `{"dimension": ..., "granularity": "month"}` with no `column` key

#### Scenario: dimension key still validates

- **WHEN** a query is constructed with
  `time_dimensions` = `[{"dimension": "created_at", "granularity": "month"}]`
- **THEN** it validates exactly as before, and constructing the object with the
  `dimension` keyword still works

#### Scenario: Schema advertises dimension

- **WHEN** the query's JSON schema is generated
- **THEN** the time-dimension object's property is `dimension` and no `column` property is
  listed

### Requirement: Functional granularity form in dimensions

A string entry in `SlayerQuery.dimensions` that is a single call `gran(col)` — where `gran` case-insensitively matches a `TimeGranularity` value (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`) and `col` is a bare or dotted column reference — SHALL be accepted at query construction as exactly equivalent to an explicit `TimeDimension(dimension=col, granularity=gran)`: rewritten entries leave `dimensions` and append to `time_dimensions` in order of appearance, after any explicit entries, and every downstream observable (generated SQL, result rows, result keys, projection order, grain, transform axes, `whole_periods_only`, `main_time_dimension` matching, canonical serialization) SHALL be identical to the explicitly written form.

#### Scenario: month(created_at) groups by month

- WHEN a query has `dimensions=["month(created_at)"]` and a count measure
- THEN it produces the same SQL and results as the query with `time_dimensions=[{"dimension": "created_at", "granularity": "month"}]`, with result key `<model>.created_at`

#### Scenario: every granularity value works

- WHEN a dimension entry uses any of the nine `TimeGranularity` values as the callee
- THEN the entry is rewritten to a time dimension with that granularity

#### Scenario: case-insensitive callee and dotted join path

- WHEN a query has `dimensions=["MONTH(customers.created_at)"]`
- THEN it is equivalent to a time dimension on `customers.created_at` at `month` granularity

#### Scenario: rewritten entries append after explicit time dimensions

- WHEN a query has both explicit `time_dimensions` and functional entries in `dimensions`
- THEN the canonical query lists the explicit time dimensions first, then the rewritten ones in their order of appearance in `dimensions`

#### Scenario: canonical serialization

- WHEN a query with a functional dimension entry is serialized
- THEN the dump shows the entry as a `TimeDimension` under `time_dimensions`, not under `dimensions`

#### Scenario: legacy-version query input

- WHEN a query document at an older schema version contains a functional dimension entry
- THEN schema migrations run first and the entry is still rewritten

### Requirement: Functional granularity form in time_dimensions entries

A string entry in `SlayerQuery.time_dimensions` of the same `gran(col)` shape SHALL coerce to the equivalent `TimeDimension`. Any other string entry SHALL be rejected at construction with an error naming the functional form and the valid granularities; no default granularity is invented. The string form SHALL be accepted wherever queries enter the system (REST API, MCP `query` tool, stored queries).

#### Scenario: functional string entry coerces

- WHEN a query has `time_dimensions=["month(created_at)"]`
- THEN it is equivalent to `time_dimensions=[{"dimension": "created_at", "granularity": "month"}]`

#### Scenario: bare column string rejected with remedy

- WHEN a query has `time_dimensions=["created_at"]`
- THEN construction fails with an error naming the `gran(col)` form and the valid granularities

#### Scenario: string entries accepted at the API surfaces

- WHEN a query with a string `time_dimensions` entry arrives via the REST API or the MCP `query` tool
- THEN it is accepted and behaves identically to the core form

### Requirement: Granularity error surface in dimensions

A dimension entry whose callee is a granularity but whose shape is not a single bare or dotted column reference (zero, extra, nested-expression, `*`, or keyword arguments) SHALL fail at construction with a typed error naming the required shape and the valid granularities. A dimension entry of shape `name(col)` — a single bare or dotted column argument — whose callee is not a granularity, not an allowlisted scalar function, not a transform, and not a builtin aggregation SHALL fail at construction with a typed error naming the valid granularities and stating that a custom aggregation used as a dimension must carry `partition_by=`. All other dimension expressions SHALL keep their existing behaviour and error paths, including scalar-function and `partition_by=` aggregate computed dimensions remaining legal and bare builtin aggregates keeping their binding-time `partition_by=` error.

#### Scenario: wrong-shape granularity call

- WHEN a query has a dimension entry `month()`, `month(a, b)`, `month(upper(x))`, or `month(*)`
- THEN construction fails with an error naming the required `gran(col)` shape and the valid granularities

#### Scenario: unknown single-column call names granularities

- WHEN a query has a dimension entry `mnth(created_at)`
- THEN construction fails with an error naming the nine valid granularities and the `partition_by=` requirement for custom aggregations in dimensions

#### Scenario: legal computed dimensions unaffected

- WHEN a query has a dimension entry `upper(region)` or an aggregate expression carrying `partition_by=`
- THEN it binds and executes exactly as before this change

#### Scenario: bare builtin aggregate keeps its error

- WHEN a query has a dimension entry `sum(price)`
- THEN it fails at binding with the existing error stating aggregates in dimension expressions must declare `partition_by=`

### Requirement: Functional granularity form as an order key

An order entry `gran(col)` SHALL sort by the bucketed value of the query's time dimension on `col` at granularity `gran` when that time dimension is projected. When no time dimension on `col` is projected, or the projected granularity differs, the query SHALL fail with an error naming the remedy. Granularity calls inside Mode-B filter expressions are out of scope and SHALL keep their existing unknown-function behaviour.

#### Scenario: order by projected bucket

- WHEN a query projects `month(created_at)` (functional or explicit) and orders by `"month(created_at)"`
- THEN rows sort by the month bucket, identically to ordering by the time dimension's column name

#### Scenario: order without matching time dimension

- WHEN a query orders by `"month(created_at)"` but projects no time dimension on `created_at`, or projects it at a different granularity
- THEN the query fails with an error naming the missing or mismatched time dimension and the remedy

#### Scenario: granularity call in a filter is not recognised

- WHEN a query filter contains `month(created_at) >= '2024-01-01'`
- THEN the query fails with the existing unknown-function error for `month`

### Requirement: Result keys disambiguate same-column time dimensions

When two or more projected time dimensions share the same source column at different granularities, each of their result keys SHALL be the usual column key with the granularity appended (`orders.created_at.month`, `orders.created_at.year`). A time dimension whose column no other projected time dimension shares SHALL keep its existing granularity-free key. Exact-duplicate time dimensions (same column, granularity, date range, and label) SHALL be deduplicated at construction; time dimensions sharing column and granularity but differing in date range or label SHALL be rejected at construction. Time dimensions whose columns are spelled differently but resolve to the same bucket (the same bound time-truncation identity), disagreeing in date range or label, SHALL be rejected at binding — backstopping the construction-time text check for spellings that only prove equivalent once resolved.

#### Scenario: two granularities of one column

- WHEN a query projects `month(created_at)` and `year(created_at)`
- THEN the result has two columns keyed `<model>.created_at.month` and `<model>.created_at.year` with correctly bucketed values

#### Scenario: single time dimension keeps its key

- WHEN a query projects a single time dimension on `created_at`
- THEN its result key stays `<model>.created_at` with no granularity suffix

#### Scenario: exact duplicates dedupe

- WHEN a query lists `month(created_at)` twice (in either form)
- THEN the query behaves as if it were listed once

#### Scenario: same column and granularity with differing metadata rejected

- WHEN a query lists two time dimensions on the same column and granularity that differ in date range or label
- THEN construction fails with an error naming the collision

#### Scenario: equivalent spellings with differing metadata rejected at binding

- WHEN two projected time dimensions name one column via different spellings that resolve to the same bucket but differ in date range or label
- THEN the query fails with an error naming the conflict

### Requirement: main_time_dimension over same-column buckets fails closed

When several projected time dimensions share a column at different granularities, a bare-column `main_time_dimension` naming that column cannot pick a bucket and SHALL be rejected as ambiguous, naming the per-granularity candidates (`month(created_at)`, `year(created_at)`). A `main_time_dimension` that resolves to exactly one projected time dimension (a lone time dimension, a distinct column, or a full-name match) SHALL continue to select it.

#### Scenario: ambiguous bare-column main_time_dimension rejected

- WHEN a query projects `month(created_at)` and `year(created_at)`, sets `main_time_dimension="created_at"`, and carries a time-ordered transform
- THEN the query fails with an ambiguity error naming `month(created_at)` and `year(created_at)`

#### Scenario: main_time_dimension on a distinct column still resolves

- WHEN a query projects time dimensions on two different columns and `main_time_dimension` names one of them
- THEN that time dimension is selected as the transform axis

### Requirement: Functional form is advertised

The MCP `query` tool documentation SHALL name every `TimeGranularity` value and the functional `gran(col)` form for dimensions and time dimensions.

#### Scenario: tool docs list all granularities

- WHEN the MCP `query` tool's argument documentation is inspected
- THEN it names all nine granularity values and the functional form

### Requirement: Time dimensions bind on stage datasets

A `TimeDimension` in a downstream stage of a multi-stage query SHALL bind against any column of its upstream stage's schema whose type is DATE or TIMESTAMP, exactly as it binds against a model column: the stage column is bucketed at the requested granularity, the result key is the stage-prefixed column name, and the bucket takes part in the stage's grain alongside its plain dimensions. A column the upstream stage already bucketed SHALL re-bucket at the same granularity (idempotent) or at a coarser granularity the upstream one nests into. A column the upstream stage did not bucket — a raw temporal column projected as a plain dimension, or an aggregate output such as the `max` of a timestamp, including the auto-named `partition_by` form — SHALL accept every granularity. The binding SHALL work at any depth of the stage chain and for multi-hop flat names.

#### Scenario: Same granularity re-binds a multi-hop flat name

- **WHEN** an inner stage projects `customers.regions.last_activity_at` at `month` and the outer stage declares a time dimension on the flat name `customers__regions__last_activity_at` at `month`
- **THEN** the outer stage groups by that column's month bucket, its result key is the stage-prefixed flat name, and the executed values equal the upstream buckets

#### Scenario: Coarser granularity re-buckets

- **WHEN** an inner stage buckets `created_at` at `month` with a revenue sum, and the outer stage declares a time dimension on `created_at` at `year` with the sum of the inner sums
- **THEN** the outer stage returns one row per year whose value is the sum of that year's monthly sums, by executed values on SQLite and DuckDB

#### Scenario: Raw temporal column projected by the inner stage

- **WHEN** an inner stage projects `created_at` as a plain dimension and the outer stage declares a time dimension on `created_at` at `month`
- **THEN** the outer stage groups by month with correct executed values

#### Scenario: Aggregate-output timestamp with the partition_by auto-name and a same-column filter

- **WHEN** an inner stage grouped by `customer_id` selects `ordered_at:max(partition_by=customer_id)` (auto-named `ordered_at_max_partition_by_customer_id`), and the outer stage declares a time dimension on that column at `month`, a filter `ordered_at_max_partition_by_customer_id >= '<date>'` and a count
- **THEN** the query executes on SQLite and DuckDB, returns one row per cohort month with the count of customers whose last order falls in that month and after the filter date, and the same query binds at every granularity, `day` included, with no re-bucketing error

#### Scenario: Three-stage chain

- **WHEN** stage one buckets `created_at` at `month`, stage two declares a time dimension on `created_at` at `month` over stage one, and stage three declares one at `year` over stage two
- **THEN** stage three returns one row per year with correct executed values

#### Scenario: Stage time dimension joins the stage grain

- **WHEN** an outer stage declares a plain stage dimension and a stage time dimension together
- **THEN** the result has one row per (dimension value, bucket) with correct executed values

### Requirement: Re-bucketing a stage column is typed

Planning SHALL fail with a typed error (a `ValueError` subclass naming the time dimension), raised by the checker before any SQL is generated, when a stage time dimension's column is not DATE / TIMESTAMP, when the column has no recorded type, or when the requested granularity does not equal the upstream bucket's granularity and the upstream granularity does not nest into it. Nesting is: `second → minute → hour → day`, `day → week`, `day → week_sunday`, `day → month → quarter → year`, closed under transitivity; every other pair (finer, or non-nesting such as `week` against `month` in either direction, or `week` against `week_sunday`) is rejected. The re-bucketing error MUST name both granularities and the remedy (request the same or a nesting-coarser granularity, or bucket the raw column instead), and MUST be the same message whether the bucketed column belongs to a stage or to a model — it does not refer to an upstream stage. An unknown stage column SHALL raise the existing unknown-reference error listing the stage's columns, and a dotted name against a stage SHALL keep the existing illegal-scope error.

#### Scenario: Finer granularity rejected

- **WHEN** an inner stage buckets `created_at` at `month` and the outer stage declares a time dimension on `created_at` at `day`
- **THEN** planning fails with the typed error naming `month`, `day` and the remedy, and no SQL is generated

#### Scenario: Non-nesting granularities rejected in both directions

- **WHEN** an inner stage buckets `created_at` at `month` and the outer stage requests `week`, or the inner stage buckets at `week` and the outer requests `month`
- **THEN** planning fails with the same typed error naming both granularities

#### Scenario: Non-temporal stage column rejected

- **WHEN** the outer stage declares a time dimension on a TEXT column of the inner stage
- **THEN** planning fails with the typed error stating the column must be temporal (DATE / TIMESTAMP) and naming the observed type

#### Scenario: Untyped stage column fails closed

- **WHEN** the outer stage declares a time dimension on an inner-stage column whose type is not recorded
- **THEN** planning fails with the temporal-column typed error rather than binding

#### Scenario: Unknown and dotted names keep their binder errors

- **WHEN** the outer stage declares a time dimension on a name the inner stage does not project, or on a dotted name
- **THEN** planning fails with the unknown-reference error listing the stage's columns, or the illegal-scope error, respectively

#### Scenario: One message for stage and model columns

- **WHEN** a finer time dimension is requested over a stage column bucketed at `month` and over a model column carrying granularity `month`
- **THEN** both fail with the same typed error text, which names both granularities and the remedy and does not mention an upstream stage

### Requirement: Functional surfaces apply to stage time dimensions

A `gran(col)` order entry over a stage time dimension SHALL sort by that bucket, and two stage time dimensions on one stage column at different granularities SHALL carry granularity-suffixed result keys, identically to the model-scope behaviour.

#### Scenario: Order by the stage bucket

- **WHEN** an outer stage declares a time dimension on `created_at` at `month` and orders by `"month(created_at)"` descending
- **THEN** rows sort by the month bucket descending, identically to ordering by `created_at`

#### Scenario: Two granularities of one stage column

- **WHEN** an outer stage declares time dimensions on `created_at` at `month` and at `year`
- **THEN** the result has keys `<stage>.created_at.month` and `<stage>.created_at.year` with correctly bucketed values

### Requirement: Re-bucketing a bucketed model column is typed

A time dimension over a model column that carries a granularity — a query-backed model's cached bucketed column, or a table-backed column declaring one — SHALL follow the same rule as a stage column: the same granularity re-binds, a granularity the recorded one nests into re-buckets, and a finer or non-nesting granularity is the typed re-bucketing error raised before any SQL is generated. The rule SHALL apply however the column is reached — bare, dotted through a join path, or through a nested query-backed model.

#### Scenario: Finer granularity over a query-backed model column rejected

- **WHEN** a query-backed model `monthly` is saved from a query bucketing `created_at` at `month` with a revenue sum, and a query on `monthly` requests a time dimension on `created_at` at `day`
- **THEN** planning fails with the typed re-bucketing error naming `month`, `day` and the remedy, and no SQL is generated, on SQLite and DuckDB

#### Scenario: Same granularity over a query-backed model column executes

- **WHEN** a query on `monthly` requests a time dimension on `created_at` at `month` with the sum of `rev`
- **THEN** it executes on SQLite and DuckDB and returns the same buckets and values as `monthly` itself

#### Scenario: Coarser granularity over a query-backed model column executes

- **WHEN** a query on `monthly` requests a time dimension on `created_at` at `year` with the sum of `rev`
- **THEN** it executes on SQLite and DuckDB and returns one row per year whose value is the sum of that year's monthly sums

### Requirement: Sibling-stage stand-ins carry the bucket

When a stage of a multi-stage query binds against a sibling stage through a model-shaped scope — a `ModelExtension` over a named sibling, or a join or dotted reference to a sibling — the sibling's bucketed columns SHALL carry their granularity into that scope, so the re-bucketing rule applies exactly as it does for the flat stage-schema binding.

#### Scenario: ModelExtension over a bucketed sibling rejects finer

- **WHEN** a named stage buckets `created_at` at `month`, and a later stage whose source is a `ModelExtension` over that sibling declares a time dimension on `created_at` at `day`
- **THEN** planning fails with the typed re-bucketing error, and the same shape at `year` executes with correct values

#### Scenario: Stage join to a bucketed sibling rejects finer

- **WHEN** a named stage buckets `created_at` at `month`, and a later stage joins it and declares a time dimension on the dotted sibling column at `day`
- **THEN** planning fails with the typed re-bucketing error, and the same shape at `month` binds
