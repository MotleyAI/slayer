# queries/time-dimensions Delta

## Purpose

Defines the time-dimension input surface: the functional granularity form `gran(col)` in `dimensions`, `time_dimensions`, and order keys, its equivalence with the explicit `TimeDimension` form, its error surface, and result-key disambiguation when several time dimensions share a column.

## ADDED Requirements

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
