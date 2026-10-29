## ADDED Requirements

### Requirement: Library growth never invalidates a stored artefact

Every name the Mode-B library reserves — scalar and list functions, function spellings, date-part aliases, transforms, built-in granularities and built-in aggregations — SHALL be pinned, so a newly reserved name is only accepted when it is a function or function spelling (which a model's own definition shadows) or when a stored-upgrade migration covers stored documents already using it. A stored model whose custom aggregation takes any reserved function or spelling name SHALL load and answer its aggregation, and a stored datasource whose custom granularity takes any reserved name SHALL load after migration and answer through the renamed granularity.

#### Scenario: Every reserved function name as a stored aggregation

- **WHEN** for each reserved function and spelling name a stored model defines a custom aggregation of that name with formula `SUM({value})`
- **THEN** each model loads and the aggregation returns the sum of its column

#### Scenario: Every reserved name as a stored granularity

- **WHEN** for each reserved name a stored datasource defines a custom granularity of that name used by a stored model's time dimension
- **THEN** the datasource and model load and the time dimension buckets by the renamed granularity

#### Scenario: An unpinned reservation is caught

- **WHEN** the library reserves a name outside the pinned set that is neither a function nor a function spelling and no migration covers it
- **THEN** the library-growth check fails, naming the unpinned name

### Requirement: Stored granularities colliding with a library name are renamed

On load, a stored datasource custom granularity whose name case-insensitively matches any reserved library name SHALL be renamed to `<name>_granularity`, or `<name>_granularity_<n>` with the smallest `n` ≥ 2 that is free, and every reference to it in that datasource's stored models, saved queries, query-backed models and memories SHALL be rewritten to the new name in every position a granularity name is accepted. Each rename SHALL be logged as a warning naming the datasource and both names. The rename SHALL be a function of the stored datasource alone, and dependent documents SHALL be persisted before the datasource, so a load interrupted at any point completes on the next load. A client saving a custom granularity with a reserved name SHALL still be refused.

#### Scenario: Granularity named left

- **WHEN** a stored datasource defines the granularity `left` and a stored model's query-backed source and a memory's query both bucket `created_at` by it
- **THEN** after load the granularity is `left_granularity`, both documents reference `left_granularity`, a warning names the rename, and both queries return the rows they returned before

#### Scenario: Granularity named collect

- **WHEN** a stored datasource defines the granularity `collect`
- **THEN** it loads as `collect_granularity`

#### Scenario: The new name is taken

- **WHEN** a stored datasource defines the granularities `left` and `left_granularity`
- **THEN** `left` is renamed to `left_granularity_2` and references to `left_granularity` are untouched

#### Scenario: An interrupted load completes

- **WHEN** a load is interrupted after some dependent documents were persisted with the new name and before the datasource was
- **THEN** the next load renames the datasource's granularity to the same name and rewrites the remaining documents, leaving one consistent store

#### Scenario: A new reserved-name granularity is refused

- **WHEN** a client saves a datasource defining the granularity `left`
- **THEN** the save fails naming the reserved name

### Requirement: Stored aggregations over a column named distinct are upgraded

On load, a stored model, saved query or memory written before this change whose Mode-B text aggregates a bare `distinct` (`agg(distinct)` or `agg(distinct, …)`) SHALL have that argument rewritten to the quoted identifier `"distinct"`, so it reads the column named `distinct` as it did when stored. Any other use of `DISTINCT` SHALL be untouched.

#### Scenario: Sum over a column named distinct

- **WHEN** a stored model has a column `distinct` and a measure `sum(distinct)` written by the previous release
- **THEN** it loads as `sum("distinct")` and returns the sum of that column

#### Scenario: The DISTINCT modifier is untouched

- **WHEN** a stored measure is `count(DISTINCT customer_id)`
- **THEN** it loads unchanged

### Requirement: A store written by this release reads on the previous release

Every persisted model, saved query, datasource and memory, in every storage backend, SHALL omit a field whose value is unset (`None`), so a document written with the fields added by this release left unset holds only fields the previous release (v1.4.1) declares. A backend's own document layout (a YAML memory's file name and body) SHALL be unchanged.

#### Scenario: Memory with an ordered query

- **WHEN** a memory whose query orders by a column without `nulls` is saved to YAML and to SQLite
- **THEN** neither stored form contains a `nulls` key, and each stored document validates against the v1.4.1 field sets

#### Scenario: No unset field in any kind

- **WHEN** a model, a saved query, a datasource and a memory are saved to each backend
- **THEN** no stored value is `null`

## MODIFIED Requirements

### Requirement: Stored Mode-B text is upgraded to the SQL string and identifier rules

On load, every Mode-B surface of a stored model, saved query or memory written before this change — `ModelMeasure.formula`, and a stored query's measures, dimensions, computed-dimension expressions, time-dimension columns, order columns and expressions, `main_time_dimension` and filters, recursively through stored `source_queries`, saved queries and inline `ModelExtension.measures` — SHALL be rewritten so that it means under the current grammar what it meant when stored: a double-quoted string literal becomes the equivalent single-quoted literal, a string literal using backslash escapes is re-encoded with its decoded value, and a `|` concatenation becomes `||`. The rewrite SHALL be token-exact: text inside string literals is never otherwise touched, and stored text that cannot be tokenised is left unchanged. Stored text that the grammar of the time it claims to be from could not have held SHALL be left unchanged, with a warning naming the document and the field. Mode-A surfaces SHALL NOT be rewritten, and a client query without a version SHALL NOT be rewritten.

#### Scenario: Double-quoted strings in a stored query

- **WHEN** a stored query written before this change filters `status == "paid"` and has measure `sum(iif(region == "EU", amount, 0))`
- **THEN** it loads as `status == 'paid'` and `sum(iif(region == 'EU', amount, 0))` and returns the rows it returned before

#### Scenario: Escapes and embedded quotes

- **WHEN** a stored filter is `name == "O'Brien"`, another `name == 'O\'Brien'`, and a stored measure formula compares with `"tab\there"`
- **THEN** they load as `name == 'O''Brien'` (twice) and a single-quoted literal containing a real tab character, and match the same rows as before

#### Scenario: Pipe concatenation

- **WHEN** a stored computed dimension is `status | region`
- **THEN** it loads as `status || region` and returns the same values

#### Scenario: Mode-A text untouched

- **WHEN** a stored model has `Column.sql` `"Order Total" * 2` and a model filter `status = "x"`
- **THEN** both load byte-for-byte unchanged

#### Scenario: Client queries are not rewritten

- **WHEN** a client sends a query without a version that filters `status = "paid"`
- **THEN** it is not rewritten and fails with the double-quoted-name hint unless a column named `paid` exists

#### Scenario: Released stores still open with their results

- **WHEN** the committed 0.10.2 store is opened
- **THEN** every model and memory loads and every recorded query returns the rows recorded from 0.10.2

#### Scenario: Text already in the current grammar is left alone

- **WHEN** a stored model still marked with a version before this rewrite has the measures `sum(x::int)`, `sum(a || b)`, `sum(CAST(x AS INT))`, `sum(x) FILTER (WHERE y > 1)` and the filter `x BETWEEN 1 AND 2`
- **THEN** each loads byte-for-byte unchanged and a warning names the model and each such field
