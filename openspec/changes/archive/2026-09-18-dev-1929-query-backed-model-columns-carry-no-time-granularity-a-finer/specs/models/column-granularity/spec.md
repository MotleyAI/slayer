# models/column-granularity Delta

## Purpose

Defines a column's declared time-bucket granularity — the bucket the column is already truncated to — how it is set (by hand or by the engine for query-backed models), validated, persisted and surfaced, so the time-dimension refinement rule can treat model columns exactly like stage columns.

## ADDED Requirements

### Requirement: A column may declare the time-bucket granularity it is stored at

A model column SHALL accept an optional `granularity` whose value is one of the time-dimension granularities (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`), declaring that the column's values are already truncated to that bucket. The declaration SHALL be honoured on every model kind: a time dimension over such a column at a finer or non-nesting granularity is the typed re-bucketing error, while the same or a nesting-coarser granularity binds and executes, whether the column is referenced bare or through a join path. A `granularity` on a column whose type is not `time` or `date` SHALL be rejected when the column is constructed, with an error naming the column, the granularity and the type. The field SHALL persist through storage unchanged and SHALL accept its string spelling on input. The field's documentation MUST tell authors to set it only when they are certain the column is truncated at that bucket.

#### Scenario: Hand-set granularity rejects a finer time dimension

- **WHEN** a table-backed model declares `created_at` as a `time` column with `granularity: day` and a query requests a time dimension on it at `hour`
- **THEN** planning fails with the typed re-bucketing error naming `day`, `hour` and the remedy, and no SQL is generated

#### Scenario: Hand-set granularity is honoured through a join path

- **WHEN** a joined model's `time` column declares `granularity: month` and a query on the host requests a time dimension on it via the dotted path at `day`
- **THEN** planning fails with the typed re-bucketing error, and the same dotted time dimension at `year` binds

#### Scenario: Same or coarser granularity over a hand-set column executes

- **WHEN** `created_at` declares `granularity: day` and a query requests a time dimension on it at `day` or at `month`
- **THEN** the query executes on SQLite and DuckDB with the correctly bucketed values

#### Scenario: Granularity on a non-temporal column is rejected at construction

- **WHEN** a column with type `string` — or with no type given — is constructed with `granularity: month`
- **THEN** construction fails with an error naming the column, `month` and the observed type

#### Scenario: Granularity round-trips through persistence

- **WHEN** a model whose column declares `granularity: month` is saved and loaded again, or the column is constructed from a dict spelling the value as the string `"month"`
- **THEN** the loaded column's granularity is `month`

#### Scenario: Ingestion leaves granularity unset

- **WHEN** a datasource's tables are ingested into models
- **THEN** every ingested column has no granularity

### Requirement: Query-backed model columns record their bucket granularity

For a query-backed model the engine SHALL stamp each cached column produced by a time dimension of the final stage with that time dimension's granularity, and SHALL leave the granularity unset on every other cached column (a raw temporal column projected as a plain dimension, an aggregate output, a computed dimension). The stamped column SHALL keep its temporal type. The stamp SHALL be present on the persisted column snapshot written at save time and on the model as resolved for querying, so a nested query-backed model built over a bucketed one records the outer bucket.

#### Scenario: Bucketed final-stage column is stamped and persisted

- **WHEN** a query-backed model is created from a query with a `month` time dimension on `created_at` and a revenue sum
- **THEN** the returned model's cached `created_at` column has granularity `month` and a temporal type, the cached `rev` column has no granularity, and the model reloaded from storage shows the same

#### Scenario: Unbucketed cached columns carry no granularity

- **WHEN** a query-backed model's final stage projects `created_at` as a plain dimension, or selects `created_at:max`
- **THEN** the corresponding cached column has no granularity and any time-dimension granularity over it binds

#### Scenario: Nested query-backed models compose

- **WHEN** a query-backed model `yearly` is created over the query-backed model `monthly` with a `year` time dimension on `created_at`
- **THEN** `yearly`'s cached `created_at` has granularity `year`, and a `day` time dimension over `yearly` is the typed re-bucketing error

### Requirement: Model-editing surfaces round-trip the granularity

The MCP `create_model` and `edit_model` tools SHALL accept `granularity` on a column definition and document it among the column fields; `edit_model` SHALL preserve an existing granularity when a partial update omits it, clear it when the update passes `null`, and report the construction error when the update would put a granularity on a non-temporal column. `inspect_model`'s column summary SHALL show the granularity when one is set.

#### Scenario: edit_model sets, preserves and clears a granularity

- **WHEN** `edit_model` upserts `created_at` with `granularity: "month"`, then upserts the same column with only a new `description`, then upserts it with `granularity: null`
- **THEN** the persisted column's granularity is `month`, still `month`, then unset, in that order

#### Scenario: edit_model rejects a granularity on a non-temporal column

- **WHEN** `edit_model` upserts a `string` column with `granularity: "month"`
- **THEN** the tool reports an invalid-column error naming the column, `month` and the type, and the model is not changed

#### Scenario: inspect_model shows a set granularity

- **WHEN** a column with `granularity: month` is inspected
- **THEN** its summary entry carries `granularity: month`, and a column without one carries no such entry
