# queries/time-dimensions Specification

## Purpose
The `TimeDimension` input contract: which keys may name the time column, and how the
object is serialized and advertised, independent of any one query surface.

## Requirements

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

A string entry in `SlayerQuery.dimensions` that is a single call `date_trunc('<unit>', expr)` — where `<unit>` is a string literal case-insensitively matching a built-in granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`) or a custom granularity defined on the query's datasource (`queries/custom-granularities`), and `expr` is a row-level expression (a bare or dotted column reference being the one-leaf case) — SHALL be exactly equivalent to an explicit `TimeDimension(dimension=expr, granularity=<unit>)`: rewritten entries leave `dimensions` and append to `time_dimensions` in order of appearance, after any explicit entries, and every downstream observable (generated SQL, result rows, result keys, projection order, grain, transform axes, `whole_periods_only`, `main_time_dimension` matching, canonical serialization) SHALL be identical to the explicitly written form. A built-in unit SHALL be rewritten at query construction; a unit that is not built-in SHALL be resolved at binding against the datasource's granularities and, when it names one, SHALL behave identically to the explicit form. A `dimensions` entry that is a date-part call (`year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, any case) SHALL NOT be rewritten: it is an ordinary expression dimension holding the integer extraction (`queries/date-functions`), with no warning.

#### Scenario: month(created_at) groups by month

- WHEN a query has `dimensions=["date_trunc('month', created_at)"]` and a count measure
- THEN it produces the same SQL and results as the query with `time_dimensions=[{"dimension": "created_at", "granularity": "month"}]`, with result key `<model>.created_at`

#### Scenario: Functional form over an expression

- WHEN a query has `dimensions=["date_trunc('month', coalesce(shipped_at, created_at))"]` and a count measure
- THEN it produces the same SQL and results as the query with `time_dimensions=[{"dimension": "coalesce(shipped_at, created_at)", "granularity": "month"}]`

#### Scenario: every granularity value works

- WHEN a dimension entry uses any of the nine built-in granularity values as the `date_trunc` unit
- THEN the entry is rewritten to a time dimension with that granularity

#### Scenario: datasource granularity callee

- WHEN a query against a datasource defining `fiscal_year` has `dimensions=["date_trunc('fiscal_year', created_at)"]` and a count measure
- THEN it produces the same SQL and results as the query with `time_dimensions=[{"dimension": "created_at", "granularity": "fiscal_year"}]`

#### Scenario: case-insensitive callee and dotted join path

- WHEN a query has `dimensions=["DATE_TRUNC('MONTH', customers.created_at)"]`
- THEN it is equivalent to a time dimension on `customers.created_at` at `month` granularity

#### Scenario: rewritten entries append after explicit time dimensions

- WHEN a query has both explicit `time_dimensions` and functional entries in `dimensions`
- THEN the canonical query lists the explicit time dimensions first, then the rewritten ones in their order of appearance in `dimensions`

#### Scenario: canonical serialization

- WHEN a query with a functional dimension entry is serialized
- THEN the dump shows the entry as a `TimeDimension` under `time_dimensions`, not under `dimensions`

#### Scenario: legacy-version query input

- WHEN a query document at an older schema version contains the functional dimension entry `month(created_at)`
- THEN schema migrations run first, upgrading it to `date_trunc('month', created_at)`, and the entry is still rewritten to the month time dimension on `created_at`

#### Scenario: Extraction call is an ordinary dimension

- WHEN a query has `dimensions=["month(created_at)"]` and a count measure
- THEN the entry stays under `dimensions` as an INT expression dimension holding the month of year (1–12), rows of every year's same month share one group, no time dimension is added, and no warning is raised

### Requirement: Functional granularity form in time_dimensions entries

A string entry in `SlayerQuery.time_dimensions` SHALL be a `date_trunc('<unit>', expr)` call of the same shape, and SHALL coerce to the equivalent `TimeDimension`, whose granularity is resolved at binding when it is not built-in. Any other string entry — a bare column, a date-part extraction such as `month(created_at)`, or a call named after a granularity such as `week_sunday(created_at)` or `fiscal_year(created_at)` — SHALL be rejected at construction with a typed error naming the `date_trunc('<unit>', expr)` form and the valid built-in granularities; when the entry is a one-argument call, the error SHALL give the remedy `date_trunc('<callee>', <argument>)` (e.g. `date_trunc('month', created_at)`, `date_trunc('fiscal_year', created_at)`). A well-shaped entry whose unit names no built-in or datasource granularity SHALL fail at binding with the unknown-granularity error listing the built-in and the datasource's granularities; no default granularity is invented. The string form SHALL be accepted wherever queries enter the system (REST API, MCP `query` tool, stored queries).

#### Scenario: functional string entry coerces

- WHEN a query has `time_dimensions=["date_trunc('month', created_at)"]`
- THEN it is equivalent to `time_dimensions=[{"dimension": "created_at", "granularity": "month"}]`

#### Scenario: functional string entry over an expression coerces

- WHEN a query has `time_dimensions=["date_trunc('month', CAST(created_txt AS DATE))"]`
- THEN it is equivalent to `time_dimensions=[{"dimension": "CAST(created_txt AS DATE)", "granularity": "month"}]`

#### Scenario: bare column string rejected with remedy

- WHEN a query has `time_dimensions=["created_at"]`
- THEN construction fails with an error naming the `date_trunc('<unit>', expr)` form and the valid granularities

#### Scenario: unknown callee fails at binding

- WHEN a query has `time_dimensions=["date_trunc('mnth', created_at)"]` against a datasource defining `fiscal_year`
- THEN planning fails with the unknown-granularity error listing the nine built-in granularities and `fiscal_year`

#### Scenario: string entries accepted at the API surfaces

- WHEN a query with a string `time_dimensions` entry arrives via the REST API or the MCP `query` tool
- THEN it is accepted and behaves identically to the core form

#### Scenario: Extraction string entry rejected with the exact remedy

- WHEN a query has `time_dimensions=["month(created_at)"]` or `time_dimensions=["week_sunday(created_at)"]`
- THEN construction fails with the typed error naming `date_trunc('month', created_at)`, respectively `date_trunc('week_sunday', created_at)`

#### Scenario: Custom-granularity call string rejected

- WHEN a query against a datasource defining `fiscal_year` has `time_dimensions=["fiscal_year(created_at)"]`
- THEN construction fails with the typed error naming the `date_trunc('<unit>', expr)` form

### Requirement: Granularity error surface in dimensions

A `date_trunc` dimension entry whose shape is not exactly a string-literal unit followed by one row-level argument (no or extra arguments, a non-literal unit, `*`, or keyword arguments) SHALL fail at construction with a typed error naming the required `date_trunc('<unit>', expr)` shape and the valid granularities. A well-shaped entry whose argument does not type DATE or TIMESTAMP SHALL fail per › Time dimensions accept row-level expressions. A dimension entry `g(expr)` whose callee `g` is `week_sunday` or a custom granularity of the query's datasource, and which resolves to no aggregation, SHALL fail before any SQL is generated with a typed error whose remedy is `date_trunc('<g>', <expr>)`; when the model defines a custom aggregation named `g`, the call SHALL resolve as that aggregation. A dimension entry of shape `name(col)` — a single bare or dotted column argument — whose callee is not a granularity, not an allowlisted scalar function, not a transform, and not a builtin aggregation SHALL fail at binding with a typed error naming the `date_trunc('<unit>', expr)` bucket form, the valid built-in and datasource granularities, and stating that a custom aggregation used as a dimension must carry `partition_by=`. All other dimension expressions SHALL keep their existing behaviour and error paths, including date-part extractions, scalar-function and `partition_by=` aggregate computed dimensions remaining legal and bare builtin aggregates keeping their binding-time `partition_by=` error.

#### Scenario: wrong-shape granularity call

- WHEN a query has a dimension entry `date_trunc('month')`, `date_trunc('month', a, b)`, `date_trunc('month', *)` or `date_trunc(unit_col, created_at)`
- THEN construction fails with an error naming the required `date_trunc('<unit>', expr)` shape and the valid granularities

#### Scenario: non-temporal argument fails as a time dimension

- WHEN a query has a dimension entry `date_trunc('month', upper(x))`
- THEN planning fails with the temporal-column typed error naming the type TEXT, not a shape error

#### Scenario: unknown single-column call names granularities

- WHEN a query has a dimension entry `mnth(created_at)`
- THEN planning fails with an error naming the `date_trunc('<unit>', expr)` form, the nine built-in granularities, any datasource granularities, and the `partition_by=` requirement for custom aggregations in dimensions

#### Scenario: legal computed dimensions unaffected

- WHEN a query has a dimension entry `upper(region)` or an aggregate expression carrying `partition_by=`
- THEN it binds and executes exactly as before this change

#### Scenario: bare builtin aggregate keeps its error

- WHEN a query has a dimension entry `sum(price)`
- THEN it fails at binding with the existing error stating aggregates in dimension expressions must declare `partition_by=`

#### Scenario: Granularity-named call has no call form

- WHEN a query against a datasource defining `fiscal_year` (and no aggregation of that name) has a dimension entry `week_sunday(created_at)` or `fiscal_year(created_at)`
- THEN planning fails with the typed error naming `date_trunc('week_sunday', created_at)`, respectively `date_trunc('fiscal_year', created_at)`, and no SQL is generated

### Requirement: Result keys disambiguate same-column time dimensions

When two or more projected unnamed time dimensions share the same bound source — a column or an expression — at different granularities, each of their result keys SHALL be the usual key with the granularity appended (`orders.created_at.month`, `orders.created_at.year`). An unnamed time dimension whose source no other projected time dimension shares SHALL keep its granularity-free key, and a named one SHALL never carry the suffix (› Time dimension names). Exact-duplicate time dimensions (the same dimension, granularity, date range, name and label) SHALL be deduplicated at construction; time dimensions sharing dimension and granularity but differing in date range, name or label SHALL be rejected at construction. At construction two dimensions are the same when their parsed expressions are equal once a leading root qualifier is dropped (`created_at` ≡ `orders.created_at` from root `orders`), never on their text, so text inside a string literal or quoted identifier is compared exactly and never read as a qualifier. Time dimensions whose dimensions are spelled differently but resolve to the same bucket (the same bound time-truncation identity), disagreeing in date range, name or label, SHALL be rejected at binding — backstopping the construction-time check for spellings that only prove equivalent once resolved. Such spellings that share their name (an unnamed one's derived from its template) and agree in date range and label SHALL be deduplicated at binding into one projected time dimension, as exact duplicates are at construction.

#### Scenario: two granularities of one column

- WHEN a query projects `date_trunc('month', created_at)` and `date_trunc('year', created_at)`
- THEN the result has two columns keyed `<model>.created_at.month` and `<model>.created_at.year` with correctly bucketed values

#### Scenario: two granularities of one expression

- WHEN a query projects `date_trunc('month', coalesce(shipped_at, created_at))` and `date_trunc('year', coalesce(shipped_at, created_at))`
- THEN the result has two columns keyed `<model>.coalesce_shipped_at_created_at.month` and `<model>.coalesce_shipped_at_created_at.year`

#### Scenario: single time dimension keeps its key

- WHEN a query projects a single time dimension on `created_at`
- THEN its result key stays `<model>.created_at` with no granularity suffix

#### Scenario: exact duplicates dedupe

- WHEN a query lists `date_trunc('month', created_at)` twice (in either form)
- THEN the query behaves as if it were listed once

#### Scenario: a root-qualified duplicate dedupes

- WHEN a query rooted at `orders` lists `date_trunc('month', created_at)` and `date_trunc('month', orders.created_at)`
- THEN the query behaves as if `date_trunc('month', created_at)` were listed once

#### Scenario: literals are never conflated

- WHEN a query lists the month time dimensions `iif(note = 'orders.a', created_at, shipped_at)` and `iif(note = 'a', created_at, shipped_at)` with different date ranges
- THEN construction accepts both as distinct time dimensions

#### Scenario: same column and granularity with differing metadata rejected

- WHEN a query lists two time dimensions on the same dimension and granularity that differ in date range or label
- THEN construction fails with an error naming the collision

#### Scenario: equivalent spellings with differing metadata rejected at binding

- WHEN two projected time dimensions name one source via different spellings (e.g. `coalesce(shipped_at, created_at)` and `COALESCE(shipped_at,created_at)`) that resolve to the same bucket but differ in date range or label
- THEN the query fails with an error naming the conflict

#### Scenario: equivalent spellings with agreeing metadata dedupe at binding

- WHEN two projected time dimensions name one source via different spellings (e.g. `coalesce(shipped_at, created_at)` and `COALESCE(shipped_at,created_at)`) at one granularity, agreeing in date range, name and label
- THEN the query behaves as if it were listed once, by executed values on SQLite and DuckDB

### Requirement: main_time_dimension over same-column buckets fails closed

`main_time_dimension` SHALL select a projected time dimension by its name (› Time dimension names), or, for a column time dimension, by the column's full or leaf name. When several projected time dimensions share a source at different granularities, a reference that matches them all cannot pick a bucket and SHALL be rejected as ambiguous, naming the per-granularity candidates (`date_trunc('month', created_at)`, `date_trunc('year', created_at)`). A `main_time_dimension` that resolves to exactly one projected time dimension (a lone time dimension, a distinct source, a full-name match, or a name match) SHALL continue to select it.

#### Scenario: ambiguous bare-column main_time_dimension rejected

- WHEN a query projects `date_trunc('month', created_at)` and `date_trunc('year', created_at)`, sets `main_time_dimension="created_at"`, and carries a time-ordered transform
- THEN the query fails with an ambiguity error naming `date_trunc('month', created_at)` and `date_trunc('year', created_at)`

#### Scenario: main_time_dimension on a distinct column still resolves

- WHEN a query projects time dimensions on two different columns and `main_time_dimension` names one of them
- THEN that time dimension is selected as the transform axis

#### Scenario: main_time_dimension names an expression time dimension

- WHEN a query projects `date_trunc('month', created_at)` and the time dimension `coalesce(shipped_at, created_at)` at `month` named `effective_at`, sets `main_time_dimension="effective_at"`, and selects `cumsum(count(*))`
- THEN the running count accumulates along the `effective_at` months, by executed values

### Requirement: Functional form is advertised

The MCP `query` tool documentation SHALL name every `TimeGranularity` value and the functional `date_trunc('<unit>', expr)` form for dimensions and time dimensions.

#### Scenario: tool docs list all granularities

- WHEN the MCP `query` tool's argument documentation is inspected
- THEN it names all nine granularity values and the `date_trunc('<unit>', expr)` form

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

- **WHEN** an inner stage grouped by `customer_id` selects `max(ordered_at, partition_by=customer_id)` (auto-named `ordered_at_max_partition_by_customer_id`), and the outer stage declares a time dimension on that column at `month`, a filter `ordered_at_max_partition_by_customer_id >= '<date>'` and a count
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

A `date_trunc('<unit>', col)` order entry over a stage time dimension SHALL sort by that bucket, and two stage time dimensions on one stage column at different granularities SHALL carry granularity-suffixed result keys, identically to the model-scope behaviour.

#### Scenario: Order by the stage bucket

- **WHEN** an outer stage declares a time dimension on `created_at` at `month` and orders by `"date_trunc('month', created_at)"` descending
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

### Requirement: Functional granularity order key

An order entry `date_trunc('<unit>', expr)` SHALL sort by the bucketed value of the query's time dimension on `expr` at granularity `<unit>` when that time dimension is projected, `expr` matching the time dimension's source by its bound identity. When no time dimension on `expr` is projected, or the projected granularity differs, the query SHALL fail with an error naming the remedy. An order entry that is a date-part call such as `month(expr)` SHALL be an ordinary expression order key over the extraction. A `date_trunc` call used inside a filter or another expression is a row-level expression (see `queries/time-points`).

#### Scenario: order by projected bucket

- WHEN a query projects `date_trunc('month', created_at)` (functional or explicit) and orders by `"date_trunc('month', created_at)"`
- THEN rows sort by the month bucket, identically to ordering by the time dimension's column name

#### Scenario: order by a projected expression bucket

- WHEN a query projects `date_trunc('month', coalesce(shipped_at, created_at))` and orders by `"date_trunc('month', coalesce(shipped_at, created_at))"` descending
- THEN rows sort by that month bucket descending

#### Scenario: order without matching time dimension

- WHEN a query orders by `"date_trunc('month', created_at)"` but projects no time dimension on `created_at`, or projects it at a different granularity
- THEN the query fails with an error naming the missing or mismatched time dimension and the remedy

#### Scenario: granularity call in a filter is recognised

- WHEN a query filter contains `date_trunc('month', created_at) >= '2024-01-01'`
- THEN the query executes and restricts rows to `created_at >= 2024-01-01`

#### Scenario: Extraction order key is an ordinary expression

- WHEN a query has `dimensions=["month(created_at)"]` and orders by `"month(created_at)"`
- THEN rows sort by the month of year (1–12), with no time-dimension matching and no error

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

Every rule stated for a time dimension's column SHALL apply to an expression time dimension's per-row value: its `date_range` SHALL restrict the rows whose value falls in the range and SHALL take date bounds when the expression types DATE and instant bounds when it types TIMESTAMP; `whole_periods_only` SHALL snap its bounds by the same rule; a frame bound written as a comparison of the expression (or `date_trunc('<unit>', expression)`) with a time point SHALL behave as a frame bound on that time dimension; `window=` aggregations, time-ordered transforms and the `first` / `last` collapse SHALL use it as their time axis; and a downstream stage SHALL see it as a temporal column carrying the bucket granularity, re-bucketable under the existing nesting rule. Its values, row counts and companion measures SHALL equal those of the same query over a column holding the expression's per-row values.

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
