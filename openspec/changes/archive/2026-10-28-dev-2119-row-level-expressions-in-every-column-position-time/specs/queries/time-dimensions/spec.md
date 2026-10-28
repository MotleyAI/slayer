## ADDED Requirements

### Requirement: Time dimensions accept row-level expressions

A time dimension's `dimension` SHALL be a row-level Mode-B expression — a bare or dotted column reference being the one-leaf case — bucketed at the requested granularity exactly as a column is. The expression SHALL be homed per `queries/semantics` › Home dataset (Axiom 2.2): its leaves MAY be host-model columns, columns reached over provably to-one hops, or columns of the current stage's schema; an expression whose leaves no candidate home determines SHALL fail with the existing input-safety error naming the leaf and the hop. The expression SHALL type DATE or TIMESTAMP; any other type, or a type that cannot be determined, SHALL fail before any SQL is generated with the existing temporal-column typed error naming the time dimension and the observed type, and, for an undetermined type, the remedy of wrapping the expression in `CAST(… AS TIMESTAMP)` or `CAST(… AS DATE)`. An aggregate, transform or attached value anywhere in the expression SHALL be rejected with a typed error stating that a time dimension is row-level. The population inference of `queries/population` SHALL read an expression time dimension's leaves as it reads a column's.

#### Scenario: Coalesced timestamp groups by month
- **WHEN** a query on `orders` has `time_dimensions` = `[{"dimension": "coalesce(shipped_at, created_at)", "granularity": "month"}]` and a count measure
- **THEN** each row is one month of the per-row coalesced timestamp with the count of orders in it, by hand-computed executed values on SQLite and DuckDB

#### Scenario: Cast text to a date
- **WHEN** a query buckets `CAST(created_txt AS DATE)` at `month`, `created_txt` a TEXT column holding ISO dates
- **THEN** it groups by the month of the cast value, by executed values on SQLite and DuckDB

#### Scenario: Cross-model expression over a to-one hop
- **WHEN** a query rooted at `orders` buckets `coalesce(customers.signup_ts, created_at)` at `month`, `orders → customers` provably to-one
- **THEN** it executes over the `orders` rows, each order reading its own customer's value, by executed values

#### Scenario: Expression leaf across a fanning hop fails closed
- **WHEN** a query rooted at `customers` buckets `coalesce(orders.shipped_at, signup_ts)` at `month`, `customers → orders` one-to-many
- **THEN** planning fails with the input-safety error naming `orders.shipped_at` and the hop, never a multiplied row set

#### Scenario: Non-temporal expression rejected
- **WHEN** a query buckets `upper(status)` at `month`
- **THEN** planning fails with the temporal-column typed error naming the time dimension and the type TEXT, and no SQL is generated

#### Scenario: Undetermined type fails closed with the cast remedy
- **WHEN** a query buckets an expression whose type cannot be determined at `month`
- **THEN** planning fails with the temporal-column typed error naming the remedy of an explicit `CAST`

#### Scenario: Aggregate inside a time dimension rejected
- **WHEN** a query buckets `max(created_at, partition_by=status)` at `month`
- **THEN** planning fails with the typed error stating that a time dimension is row-level

#### Scenario: Stage-scope expression
- **WHEN** an outer stage buckets `coalesce(shipped_at, created_at)` over an inner stage that projects both columns as plain dimensions
- **THEN** it groups by the month of the coalesced stage value, by executed values

### Requirement: An expression time dimension behaves as a column in every time position

Every rule stated for a time dimension's column SHALL apply to an expression time dimension's per-row value: its `date_range` SHALL restrict the rows whose value falls in the range and SHALL take date bounds when the expression types DATE and instant bounds when it types TIMESTAMP; `whole_periods_only` SHALL snap its bounds by the same rule; a frame bound written as a comparison of the expression (or `gran(expression)`) with a time point SHALL behave as a frame bound on that time dimension; `window=` aggregations, time-ordered transforms and the `first` / `last` collapse SHALL use it as their time axis; and a downstream stage SHALL see it as a temporal column carrying the bucket granularity, re-bucketable under the existing nesting rule. Its values, row counts and companion measures SHALL equal those of the same query over a column holding the expression's per-row values.

#### Scenario: Date range on an expression time dimension
- **WHEN** a query buckets `coalesce(shipped_at, created_at)` at `month` with `date_range` `["2025-01-01", "2025-03-31"]`
- **THEN** only orders whose coalesced timestamp falls in the range are counted, and the result has the January–March buckets, by executed values

#### Scenario: DATE-typed expression takes date bounds
- **WHEN** the time dimension is `CAST(created_at AS DATE)` with `date_range` `["2025-01", "2025-02"]` and `whole_periods_only`
- **THEN** the bounds are compared as dates and snap to whole periods exactly as for a DATE column, by executed values

#### Scenario: Rolling window and change over an expression axis
- **WHEN** a query buckets `coalesce(shipped_at, created_at)` at `month` and selects `sum(amount, window='3m')` and `change(sum(amount))`
- **THEN** both read the months of the coalesced timestamp, including rows before the visible range for the window, by hand-computed executed values

#### Scenario: Two-stage re-bucketing of an expression time dimension
- **WHEN** an inner stage buckets `coalesce(shipped_at, created_at)` at `month` with a revenue sum, and the outer stage buckets that output column at `year` and at `day`
- **THEN** the `year` query returns one row per year summing the monthly sums, and the `day` query fails with the typed re-bucketing error naming `month` and `day`

#### Scenario: Equal to a column holding the per-row values
- **WHEN** a model declares a derived column `ts` with SQL `COALESCE(shipped_at, created_at)` and two queries bucket `ts` and `coalesce(shipped_at, created_at)` at `month` with the same measures
- **THEN** both return identical rows and values

### Requirement: Time dimension names

A time dimension SHALL accept an optional `name`. An unnamed column time dimension SHALL keep its existing result key. An unnamed expression time dimension SHALL be named by the same sanitising rule as an unnamed computed dimension, and its result key SHALL be that name prefixed by the query's model, extended with the granularity exactly when another projected time dimension shares its bound source. A named time dimension's result key SHALL be its name prefixed by the query's model, never extended with the granularity. Names SHALL be unique across a query's dimensions and time dimensions; a collision SHALL fail at construction naming it. A time dimension MAY be referred to by its name from `main_time_dimension` and from `partition_by=`, exactly as a computed dimension is by its name.

#### Scenario: Unnamed expression time dimension's key
- **WHEN** a query on `orders` buckets `coalesce(shipped_at, created_at)` at `month`
- **THEN** its result key is `orders.coalesce_shipped_at_created_at`

#### Scenario: Named time dimension
- **WHEN** the same time dimension carries `"name": "effective_at"`
- **THEN** its result key is `orders.effective_at`, and `main_time_dimension` `"effective_at"` selects it as the axis of a `cumsum` measure in a query that also buckets `created_at`

#### Scenario: Named column time dimension
- **WHEN** a time dimension on `created_at` at `month` carries `"name": "m"`
- **THEN** its result key is `orders.m` and its values equal the unnamed form's

#### Scenario: Partition by a time dimension's name
- **WHEN** a query buckets `coalesce(shipped_at, created_at)` at `month` named `effective_at`, groups by `region`, and selects `sum(amount, partition_by=effective_at)`
- **THEN** each row carries its month's total across regions, by executed values

#### Scenario: Name collision rejected
- **WHEN** a query declares a computed dimension and a time dimension both named `m`
- **THEN** construction fails with an error naming `m`

### Requirement: Re-bucketing is judged per bucketed leaf

A time dimension over an expression SHALL obey the re-bucketing rule for every bucketed temporal column the expression reads — a column carrying a granularity (a query-backed model's cached bucket, a table-backed column declaring one, a sibling stage's stand-in) or a stage column its upstream stage bucketed: the requested granularity SHALL equal that leaf's granularity or be one it nests into, else planning SHALL fail with the existing typed re-bucketing error naming that leaf, both granularities and the remedy. Leaves carrying no granularity and non-temporal leaves SHALL impose nothing.

#### Scenario: Cast of a month-bucketed column at day rejected
- **WHEN** a query on the query-backed `monthly` model (bucketing `created_at` at `month`) buckets `CAST(created_at AS DATE)` at `day`
- **THEN** planning fails with the typed re-bucketing error naming `created_at`, `month` and `day`, and no SQL is generated

#### Scenario: Same and coarser granularities bind
- **WHEN** the same expression is bucketed at `month` and at `year`
- **THEN** each executes on SQLite and DuckDB with the monthly and yearly values

#### Scenario: A bucketed leaf beside an unbucketed one still binds the rule
- **WHEN** a query buckets `coalesce(created_at, refunded_at)` at `day`, `created_at` bucketed at `month` and `refunded_at` unbucketed
- **THEN** planning fails with the typed re-bucketing error naming `created_at`

## MODIFIED Requirements

### Requirement: column is an accepted alias of dimension

A time dimension SHALL accept its dimension under either the key `dimension` or the key
`column`, with identical coercion: a string holding a Mode-B row-level expression (a bare
or dotted column reference being the one-leaf case), or the legacy column-reference object
`{"name": …, "model": …}`, which SHALL be read as the dotted text `model.name`. Serialization
SHALL always emit `dimension` as a string, and the JSON schema SHALL advertise `dimension` as
a string property.

#### Scenario: column key validates and dumps as dimension

- **WHEN** a query is constructed with
  `time_dimensions` = `[{"column": "created_at", "granularity": "month"}]`
- **THEN** the time dimension's dimension is `created_at`, and dumping the query emits
  `{"dimension": "created_at", "granularity": "month"}` with no `column` key

#### Scenario: dimension key still validates

- **WHEN** a query is constructed with
  `time_dimensions` = `[{"dimension": "created_at", "granularity": "month"}]`
- **THEN** it validates exactly as before, and constructing the object with the
  `dimension` keyword still works

#### Scenario: Legacy object form is read as text

- **WHEN** a query is constructed with
  `time_dimensions` = `[{"dimension": {"name": "created_at", "model": "customers"}, "granularity": "month"}]`
- **THEN** it equals the query with `"dimension": "customers.created_at"`, and dumps
  `dimension` as the string `customers.created_at`

#### Scenario: Schema advertises dimension

- **WHEN** the query's JSON schema is generated
- **THEN** the time-dimension object's property is `dimension`, typed as a string, and no
  `column` property is listed

### Requirement: Functional granularity form in dimensions

A string entry in `SlayerQuery.dimensions` that is a single call `gran(expr)` — where `gran` case-insensitively matches a built-in granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`) or a custom granularity defined on the query's datasource (`queries/custom-granularities`), and `expr` is a row-level expression (a bare or dotted column reference being the one-leaf case) — SHALL be exactly equivalent to an explicit `TimeDimension(dimension=expr, granularity=gran)`: rewritten entries leave `dimensions` and append to `time_dimensions` in order of appearance, after any explicit entries, and every downstream observable (generated SQL, result rows, result keys, projection order, grain, transform axes, `whole_periods_only`, `main_time_dimension` matching, canonical serialization) SHALL be identical to the explicitly written form. A built-in callee SHALL be rewritten at query construction; a callee that is not built-in SHALL be resolved at binding against the datasource's granularities and, when it names one, SHALL behave identically to the explicit form.

#### Scenario: month(created_at) groups by month

- WHEN a query has `dimensions=["month(created_at)"]` and a count measure
- THEN it produces the same SQL and results as the query with `time_dimensions=[{"dimension": "created_at", "granularity": "month"}]`, with result key `<model>.created_at`

#### Scenario: Functional form over an expression

- WHEN a query has `dimensions=["month(coalesce(shipped_at, created_at))"]` and a count measure
- THEN it produces the same SQL and results as the query with `time_dimensions=[{"dimension": "coalesce(shipped_at, created_at)", "granularity": "month"}]`

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

A string entry in `SlayerQuery.time_dimensions` of the same `gran(expr)` shape SHALL coerce to the equivalent `TimeDimension`, whose granularity is resolved at binding when it is not built-in. A string entry that is not of the `gran(expr)` shape SHALL be rejected at construction with an error naming the functional form and the valid built-in granularities; a well-shaped entry whose callee names no built-in or datasource granularity SHALL fail at binding with the unknown-granularity error listing the built-in and the datasource's granularities; no default granularity is invented. The string form SHALL be accepted wherever queries enter the system (REST API, MCP `query` tool, stored queries).

#### Scenario: functional string entry coerces

- WHEN a query has `time_dimensions=["month(created_at)"]`
- THEN it is equivalent to `time_dimensions=[{"dimension": "created_at", "granularity": "month"}]`

#### Scenario: functional string entry over an expression coerces

- WHEN a query has `time_dimensions=["month(CAST(created_txt AS DATE))"]`
- THEN it is equivalent to `time_dimensions=[{"dimension": "CAST(created_txt AS DATE)", "granularity": "month"}]`

#### Scenario: bare column string rejected with remedy

- WHEN a query has `time_dimensions=["created_at"]`
- THEN construction fails with an error naming the `gran(expr)` form and the valid granularities

#### Scenario: unknown callee fails at binding

- WHEN a query has `time_dimensions=["mnth(created_at)"]` against a datasource defining `fiscal_year`
- THEN planning fails with the unknown-granularity error listing the nine built-in granularities and `fiscal_year`

#### Scenario: string entries accepted at the API surfaces

- WHEN a query with a string `time_dimensions` entry arrives via the REST API or the MCP `query` tool
- THEN it is accepted and behaves identically to the core form

### Requirement: Granularity error surface in dimensions

A dimension entry whose callee is a built-in granularity but whose shape is not a single row-level argument (zero or extra arguments, `*`, or keyword arguments) SHALL fail at construction with a typed error naming the required shape and the valid granularities; a datasource granularity callee of such a shape SHALL fail with the same error at binding. A single-argument entry whose argument does not type DATE or TIMESTAMP SHALL fail per › Time dimensions accept row-level expressions. A dimension entry of shape `name(col)` — a single bare or dotted column argument — whose callee is not a built-in or datasource granularity, not an allowlisted scalar function, not a transform, and not a builtin aggregation SHALL fail at binding with a typed error naming the valid built-in and datasource granularities and stating that a custom aggregation used as a dimension must carry `partition_by=`. All other dimension expressions SHALL keep their existing behaviour and error paths, including scalar-function and `partition_by=` aggregate computed dimensions remaining legal and bare builtin aggregates keeping their binding-time `partition_by=` error.

#### Scenario: wrong-shape granularity call

- WHEN a query has a dimension entry `month()`, `month(a, b)` or `month(*)`
- THEN construction fails with an error naming the required `gran(expr)` shape and the valid granularities

#### Scenario: non-temporal argument fails as a time dimension

- WHEN a query has a dimension entry `month(upper(x))`
- THEN planning fails with the temporal-column typed error naming the type TEXT, not a shape error

#### Scenario: unknown single-column call names granularities

- WHEN a query has a dimension entry `mnth(created_at)`
- THEN planning fails with an error naming the nine built-in granularities, any datasource granularities, and the `partition_by=` requirement for custom aggregations in dimensions

#### Scenario: legal computed dimensions unaffected

- WHEN a query has a dimension entry `upper(region)` or an aggregate expression carrying `partition_by=`
- THEN it binds and executes exactly as before this change

#### Scenario: bare builtin aggregate keeps its error

- WHEN a query has a dimension entry `sum(price)`
- THEN it fails at binding with the existing error stating aggregates in dimension expressions must declare `partition_by=`

### Requirement: Result keys disambiguate same-column time dimensions

When two or more projected unnamed time dimensions share the same bound source — a column or an expression — at different granularities, each of their result keys SHALL be the usual key with the granularity appended (`orders.created_at.month`, `orders.created_at.year`). An unnamed time dimension whose source no other projected time dimension shares SHALL keep its granularity-free key, and a named one SHALL never carry the suffix (› Time dimension names). Exact-duplicate time dimensions (same dimension text, granularity, date range, name and label) SHALL be deduplicated at construction; time dimensions sharing dimension text and granularity but differing in date range, name or label SHALL be rejected at construction. Time dimensions whose dimensions are spelled differently but resolve to the same bucket (the same bound time-truncation identity), disagreeing in date range, name or label, SHALL be rejected at binding — backstopping the construction-time text check for spellings that only prove equivalent once resolved. Such spellings that share their name (an unnamed one's derived from its template) and agree in date range and label SHALL be deduplicated at binding into one projected time dimension, as exact duplicates are at construction.

#### Scenario: two granularities of one column

- WHEN a query projects `month(created_at)` and `year(created_at)`
- THEN the result has two columns keyed `<model>.created_at.month` and `<model>.created_at.year` with correctly bucketed values

#### Scenario: two granularities of one expression

- WHEN a query projects `month(coalesce(shipped_at, created_at))` and `year(coalesce(shipped_at, created_at))`
- THEN the result has two columns keyed `<model>.coalesce_shipped_at_created_at.month` and `<model>.coalesce_shipped_at_created_at.year`

#### Scenario: single time dimension keeps its key

- WHEN a query projects a single time dimension on `created_at`
- THEN its result key stays `<model>.created_at` with no granularity suffix

#### Scenario: exact duplicates dedupe

- WHEN a query lists `month(created_at)` twice (in either form)
- THEN the query behaves as if it were listed once

#### Scenario: same column and granularity with differing metadata rejected

- WHEN a query lists two time dimensions on the same dimension text and granularity that differ in date range or label
- THEN construction fails with an error naming the collision

#### Scenario: equivalent spellings with differing metadata rejected at binding

- WHEN two projected time dimensions name one source via different spellings (e.g. `coalesce(shipped_at, created_at)` and `COALESCE(shipped_at,created_at)`) that resolve to the same bucket but differ in date range or label
- THEN the query fails with an error naming the conflict

#### Scenario: equivalent spellings with agreeing metadata dedupe at binding

- WHEN two projected time dimensions name one source via different spellings (e.g. `coalesce(shipped_at, created_at)` and `COALESCE(shipped_at,created_at)`) at one granularity, agreeing in date range, name and label
- THEN the query behaves as if it were listed once, by executed values on SQLite and DuckDB

### Requirement: main_time_dimension over same-column buckets fails closed

`main_time_dimension` SHALL select a projected time dimension by its name (› Time dimension names), or, for a column time dimension, by the column's full or leaf name. When several projected time dimensions share a source at different granularities, a reference that matches them all cannot pick a bucket and SHALL be rejected as ambiguous, naming the per-granularity candidates (`month(created_at)`, `year(created_at)`). A `main_time_dimension` that resolves to exactly one projected time dimension (a lone time dimension, a distinct source, a full-name match, or a name match) SHALL continue to select it.

#### Scenario: ambiguous bare-column main_time_dimension rejected

- WHEN a query projects `month(created_at)` and `year(created_at)`, sets `main_time_dimension="created_at"`, and carries a time-ordered transform
- THEN the query fails with an ambiguity error naming `month(created_at)` and `year(created_at)`

#### Scenario: main_time_dimension on a distinct column still resolves

- WHEN a query projects time dimensions on two different columns and `main_time_dimension` names one of them
- THEN that time dimension is selected as the transform axis

#### Scenario: main_time_dimension names an expression time dimension

- WHEN a query projects `month(created_at)` and the time dimension `coalesce(shipped_at, created_at)` at `month` named `effective_at`, sets `main_time_dimension="effective_at"`, and selects `cumsum(count(*))`
- THEN the running count accumulates along the `effective_at` months, by executed values

### Requirement: Functional granularity order key

An order entry `gran(expr)` SHALL sort by the bucketed value of the query's time dimension on `expr` at granularity `gran` when that time dimension is projected, `expr` matching the time dimension's source by its bound identity. When no time dimension on `expr` is projected, or the projected granularity differs, the query SHALL fail with an error naming the remedy. A granularity call used inside a filter or another expression is a row-level expression (see `queries/time-points`).

#### Scenario: order by projected bucket

- WHEN a query projects `month(created_at)` (functional or explicit) and orders by `"month(created_at)"`
- THEN rows sort by the month bucket, identically to ordering by the time dimension's column name

#### Scenario: order by a projected expression bucket

- WHEN a query projects `month(coalesce(shipped_at, created_at))` and orders by `"month(coalesce(shipped_at, created_at))"` descending
- THEN rows sort by that month bucket descending

#### Scenario: order without matching time dimension

- WHEN a query orders by `"month(created_at)"` but projects no time dimension on `created_at`, or projects it at a different granularity
- THEN the query fails with an error naming the missing or mismatched time dimension and the remedy

#### Scenario: granularity call in a filter is recognised

- WHEN a query filter contains `month(created_at) >= '2024-01-01'`
- THEN the query executes and restricts rows to `created_at >= 2024-01-01`
