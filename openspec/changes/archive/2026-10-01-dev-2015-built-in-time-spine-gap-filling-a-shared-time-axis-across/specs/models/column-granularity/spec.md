## MODIFIED Requirements

### Requirement: A column may declare the time-bucket granularity it is stored at

A model column SHALL accept an optional `granularity` whose value is one of the time-dimension granularities (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`) or the name of a custom granularity defined on the model's datasource, declaring that the column's values are already truncated to that bucket. The declaration SHALL be honoured on every model kind: a time dimension over such a column at a finer or non-nesting granularity (nesting per `queries/custom-granularities`) is the typed re-bucketing error, while the same or a nesting-coarser granularity binds and executes, whether the column is referenced bare or through a join path. A `granularity` on a column whose type is not `time` or `date` SHALL be rejected when the column is constructed, with an error naming the column, the granularity and the type. A non-built-in name SHALL be accepted at construction and SHALL be checked against the model's datasource when the model is saved and when it is queried; a name the datasource does not define SHALL fail with the typed unknown-granularity error naming the column. The field SHALL persist through storage unchanged and SHALL accept its string spelling on input. The field's documentation MUST tell authors to set it only when they are certain the column is truncated at that bucket.

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

#### Scenario: Custom granularity on a column

- **WHEN** a column in a datasource defining `fiscal_year` declares `granularity: fiscal_year`, is saved and reloaded, and is queried at `fiscal_year` and at `month`
- **THEN** it round-trips unchanged, the `fiscal_year` query executes, and the `month` query fails with the typed re-bucketing error

#### Scenario: Undefined custom granularity on a column

- **WHEN** a model in a datasource that defines no `fiscal_year` is saved with a column declaring `granularity: fiscal_year`
- **THEN** the save fails with the typed unknown-granularity error naming the column
