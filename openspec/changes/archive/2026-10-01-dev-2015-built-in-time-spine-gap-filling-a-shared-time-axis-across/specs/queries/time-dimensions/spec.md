## MODIFIED Requirements

### Requirement: Functional granularity form in dimensions

A string entry in `SlayerQuery.dimensions` that is a single call `gran(col)` — where `gran` case-insensitively matches a built-in granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`) or a custom granularity defined on the query's datasource (`queries/custom-granularities`), and `col` is a bare or dotted column reference — SHALL be exactly equivalent to an explicit `TimeDimension(dimension=col, granularity=gran)`: rewritten entries leave `dimensions` and append to `time_dimensions` in order of appearance, after any explicit entries, and every downstream observable (generated SQL, result rows, result keys, projection order, grain, transform axes, `whole_periods_only`, `main_time_dimension` matching, canonical serialization) SHALL be identical to the explicitly written form. A built-in callee SHALL be rewritten at query construction; a callee that is not built-in SHALL be resolved at binding against the datasource's granularities and, when it names one, SHALL behave identically to the explicit form.

#### Scenario: month(created_at) groups by month

- WHEN a query has `dimensions=["month(created_at)"]` and a count measure
- THEN it produces the same SQL and results as the query with `time_dimensions=[{"dimension": "created_at", "granularity": "month"}]`, with result key `<model>.created_at`

#### Scenario: every granularity value works

- WHEN a dimension entry uses any of the nine built-in granularity values as the callee
- THEN the entry is rewritten to a time dimension with that granularity

#### Scenario: datasource granularity callee

- WHEN a query against a datasource defining `fiscal_year` has `dimensions=["fiscal_year(created_at)"]` and a count measure
- THEN it produces the same SQL and results as the query with `time_dimensions=[{"dimension": "created_at", "granularity": "fiscal_year"}]`

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

A string entry in `SlayerQuery.time_dimensions` of the same `gran(col)` shape SHALL coerce to the equivalent `TimeDimension`, whose granularity is resolved at binding when it is not built-in. A string entry that is not of the `gran(col)` shape SHALL be rejected at construction with an error naming the functional form and the valid built-in granularities; a well-shaped entry whose callee names no built-in or datasource granularity SHALL fail at binding with the unknown-granularity error listing the built-in and the datasource's granularities; no default granularity is invented. The string form SHALL be accepted wherever queries enter the system (REST API, MCP `query` tool, stored queries).

#### Scenario: functional string entry coerces

- WHEN a query has `time_dimensions=["month(created_at)"]`
- THEN it is equivalent to `time_dimensions=[{"dimension": "created_at", "granularity": "month"}]`

#### Scenario: bare column string rejected with remedy

- WHEN a query has `time_dimensions=["created_at"]`
- THEN construction fails with an error naming the `gran(col)` form and the valid granularities

#### Scenario: unknown callee fails at binding

- WHEN a query has `time_dimensions=["mnth(created_at)"]` against a datasource defining `fiscal_year`
- THEN planning fails with the unknown-granularity error listing the nine built-in granularities and `fiscal_year`

#### Scenario: string entries accepted at the API surfaces

- WHEN a query with a string `time_dimensions` entry arrives via the REST API or the MCP `query` tool
- THEN it is accepted and behaves identically to the core form

### Requirement: Granularity error surface in dimensions

A dimension entry whose callee is a built-in granularity but whose shape is not a single bare or dotted column reference (zero, extra, nested-expression, `*`, or keyword arguments) SHALL fail at construction with a typed error naming the required shape and the valid granularities; a datasource granularity callee of such a shape SHALL fail with the same error at binding. A dimension entry of shape `name(col)` — a single bare or dotted column argument — whose callee is not a built-in or datasource granularity, not an allowlisted scalar function, not a transform, and not a builtin aggregation SHALL fail at binding with a typed error naming the valid built-in and datasource granularities and stating that a custom aggregation used as a dimension must carry `partition_by=`. All other dimension expressions SHALL keep their existing behaviour and error paths, including scalar-function and `partition_by=` aggregate computed dimensions remaining legal and bare builtin aggregates keeping their binding-time `partition_by=` error.

#### Scenario: wrong-shape granularity call

- WHEN a query has a dimension entry `month()`, `month(a, b)`, `month(upper(x))`, or `month(*)`
- THEN construction fails with an error naming the required `gran(col)` shape and the valid granularities

#### Scenario: unknown single-column call names granularities

- WHEN a query has a dimension entry `mnth(created_at)`
- THEN planning fails with an error naming the nine built-in granularities, any datasource granularities, and the `partition_by=` requirement for custom aggregations in dimensions

#### Scenario: legal computed dimensions unaffected

- WHEN a query has a dimension entry `upper(region)` or an aggregate expression carrying `partition_by=`
- THEN it binds and executes exactly as before this change

#### Scenario: bare builtin aggregate keeps its error

- WHEN a query has a dimension entry `sum(price)`
- THEN it fails at binding with the existing error stating aggregates in dimension expressions must declare `partition_by=`
