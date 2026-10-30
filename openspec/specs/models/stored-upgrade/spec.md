# models/stored-upgrade Specification

## Purpose
Guarantees that a store written by any released SLayer version still loads, re-saves, re-ingests and queries with that release's results when opened by a later version. Stored documents are repaired on load; live client input keeps today's validation.

## Requirements

### Requirement: A released store opens with its release's results

A store written by SLayer 0.10.2 SHALL load completely when opened by the current version, both with its datasource present and with the datasource entry absent. The store SHALL hold every model and memory shape that release could write: ingested models, a Cube import whose foreign key is not a dimension, a multi-stage query-backed model ordering by `rank(...)`, an aggregation reading a quoted parameter, legacy time literals, and a memory with a query. With the datasource present, every model SHALL re-save and the datasource SHALL re-ingest. Every query the release could run SHALL return the rows the release returned.

#### Scenario: The 0.10.2 corpus opens with its datasource present

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened with its DuckDB datasource present
- **THEN** every model and memory loads, every model re-saves, the datasource re-ingests, and every recorded query returns the rows recorded from 0.10.2

#### Scenario: The 0.10.2 corpus opens offline

- **WHEN** the same store is opened with its datasource entry removed
- **THEN** every model and memory loads, without any connection being attempted

### Requirement: Live type refinement applies only to models written before refining ingest

On load, a stored model below schema version 8 SHALL have its DOUBLE and SQLite INT base columns refined against the live datasource. At version 8 and above, ingest already wrote refined types. A stored model at version 8 or above SHALL load without consulting the datasource, keeping its stored column types.

#### Scenario: A v10 model loads with no datasource entry

- **WHEN** a raw v10 model with a DOUBLE base column is loaded and its datasource has no stored entry
- **THEN** it loads with the column still DOUBLE and is written back at the current version

#### Scenario: A v10 model loads with an unreachable datasource

- **WHEN** a raw v10 model with a DOUBLE base column is loaded and its datasource is a Postgres server that refuses connections
- **THEN** it loads, and no connection to the datasource is attempted

#### Scenario: A pre-v8 model still requires its datasource

- **WHEN** a raw v5 model with a refineable DOUBLE base column is loaded and its datasource has no stored entry
- **THEN** loading fails with the error stating that the datasource is unavailable for type refinement

#### Scenario: A v7 model is still refined

- **WHEN** a raw v7 model with a DOUBLE base column backed by a live integer column is loaded with its datasource present
- **THEN** the column loads as INT

### Requirement: Stored rank-family orders get an explicit direction

On load of a stored query, wherever it is stored (a model's `source_queries` and their stages, or a memory's query), every rank-family call without a direction SHALL be given `direction='desc'`. This covers order items persisted as a placeholder column with their expression text, and it SHALL also run on documents already stored at the version that first introduced the direction rewrite. A query sent by a client, without a stored version, SHALL NOT be rewritten.

#### Scenario: A 0.10.2 stage ordering by rank executes

- **WHEN** a raw 0.10.2 multi-stage query-backed model's second stage orders by an item persisted as `{column: {name: _funcstyle_pending}, direction: asc, raw_formula: "rank(rev:sum)"}`
- **THEN** the model loads and executes, ranking descending as 0.10.2 did, on both the YAML and the SQLite backends

#### Scenario: A 0.10.2 memory query ordering by rank executes

- **WHEN** a memory stored by 0.10.2, as YAML frontmatter or as a SQLite row, bundles a query whose order item is persisted with `raw_formula: "rank(rev:sum)"`
- **THEN** the memory loads and its query executes, ranking descending

#### Scenario: A document stored after the first direction rewrite is repaired

- **WHEN** a stored v13 model holds a v5 source query whose order item still carries `raw_formula: "rank(rev:sum)"` without a direction
- **THEN** it loads as the current model and query versions, with the order item ranking descending

#### Scenario: A memory stored after the first direction rewrite is repaired

- **WHEN** a stored v3 memory bundles a v5 query whose order item still carries `raw_formula: "rank(rev:sum)"` without a direction
- **THEN** it loads as the current memory and query versions, with the order item ranking descending

### Requirement: Stored legacy date ranges are repaired on load

On load of a stored query, wherever it is stored, each time dimension's `date_range` SHALL be repaired before validation:
- A range whose length is not 2 SHALL be dropped, as 0.10.x applied no filter for it.
- A bound carrying a zone offset (`Z` or `±HH:MM`) SHALL lose the offset and keep its wall-clock time, as 0.10.x databases read it.
- A slashed date `YYYY/MM/DD`, optionally followed by a time, SHALL become its dashed ISO form.
- Any other bound SHALL be left unchanged, for validation to judge.

A query sent by a client SHALL keep being rejected for these shapes.

#### Scenario: Offset bounds return the 0.10.2 rows

- **WHEN** a raw 0.10.2 query-backed model's source query has `date_range: ['2024-01-01T00:00:00Z', '2024-12-31T23:59:59Z']`
- **THEN** it loads and returns the rows 0.10.2 returned on DuckDB and on Postgres

#### Scenario: Slashed bounds return the 0.10.2 rows

- **WHEN** a raw 0.10.2 query-backed model's source query has `date_range: ['2024/01/01', '2024/12/31']`
- **THEN** it loads and returns the rows 0.10.2 returned

#### Scenario: Ranges of length other than two apply no filter

- **WHEN** a raw 0.10.2 query-backed model's source query has a `date_range` of `[]`, of one element, or of three elements
- **THEN** it loads and returns the unfiltered rows, as 0.10.2 did

#### Scenario: A memory query with an offset bound runs

- **WHEN** a memory stored by 0.10.2, as YAML frontmatter or as a SQLite row, bundles a query whose `date_range` bound carries `+02:00`
- **THEN** the memory loads and its query runs, filtering on the bound's wall-clock time

#### Scenario: Client queries keep the strict check

- **WHEN** a client sends a query without a version whose `date_range` is `[]` or has a bound with a zone offset
- **THEN** the query is rejected with today's construction error

### Requirement: Stored legacy filter literals are repaired on load

On load of a stored query, a string literal in a stored `filters` entry SHALL be repaired with the same offset and slash rules when both of these hold:
- It is a direct comparison operand (`=`, `!=`, `<`, `<=`, `>`, `>=`, either operand order, or any element of an all-literal `IN` list).
- The other side is a column reference that resolves, through the stored models and their joins, to a DATE or TIMESTAMP column.

A literal whose column cannot be resolved, or that compares a non-temporal column, SHALL be left unchanged.

#### Scenario: An offset literal against a timestamp runs

- **WHEN** a stored query filters `ordered_at >= '2024-01-01T00:00:00Z'` where `ordered_at` is a TIMESTAMP column
- **THEN** it loads and runs, filtering on `2024-01-01 00:00:00`

#### Scenario: Every operand layout is repaired

- **WHEN** stored filters read `'2024/01/01' <= ordered_at`, `ordered_at >= '2024-01-01T00:00:00Z' and ordered_at <= '2024-06-30T00:00:00+02:00'`, `ordered_at IN ('2024/01/01', '2024/02/01')` and `customers.signed_up_at >= '2024-01-01T00:00:00Z'` (a joined TIMESTAMP column)
- **THEN** every legacy literal in them is repaired and each filter runs

#### Scenario: Non-temporal and unresolvable comparisons stay verbatim

- **WHEN** stored filters read `status = '2024/01/01'` (a TEXT column) and `ordered_at IN ('2024/01/01', status)`
- **THEN** both are left unchanged

### Requirement: Stored query-backed models drop their cached backing SQL

A model SHALL NOT store the rendered SQL of its backing query. On load, a stored model below schema version 15 SHALL drop its `backing_query_sql` field. Inspect and REST SHALL NOT return backing SQL; a query-backed model's SQL SHALL be obtained by a `dry_run` query against it.

#### Scenario: A v14 query-backed model loads without backing SQL

- **WHEN** a stored v14 query-backed model holding `backing_query_sql` is loaded
- **THEN** it loads without the field, and is written back at version 15 without it

#### Scenario: Inspect shows no backing SQL

- **WHEN** a query-backed model is inspected with `show_sql=True`
- **THEN** the output has no backing-SQL section, and a `dry_run` query on the model returns its SQL

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

- **WHEN** a stored model still marked with a version before this rewrite has the measures `sum(x::int)`, `sum(a || b)`, `sum(CAST(x AS INT))` and `sum(x) FILTER (WHERE y > 1)`, and a stored query of the same era filters `x BETWEEN 1 AND 2`
- **THEN** each loads byte-for-byte unchanged and a warning names the document and each such field

### Requirement: Stored time dimensions are upgraded to expression text

On load, every time dimension of a stored model's `source_queries`, a saved query or a memory's bundled query written before this change, whose `dimension` (or `column`) is stored as a column-reference object `{"name": …, "model": …}`, SHALL be rewritten to the dotted text `model.name` (`name` when `model` is absent), recursively through nested stored queries, and re-saved in that form; the query SHALL return the rows it returned before. A time dimension already stored as text, and a client query without a version, SHALL NOT be rewritten.

#### Scenario: Stored object form becomes text

- **WHEN** a stored query-backed model's source query holds the time dimension `{"dimension": {"name": "created_at", "model": "customers"}, "granularity": "month"}`
- **THEN** it loads with `dimension` = `customers.created_at`, dumps it as that string, and returns the same rows as before

#### Scenario: Memory-bundled query

- **WHEN** a memory stored as YAML frontmatter or as a SQLite row bundles a query whose time dimension is stored in the object form
- **THEN** it loads with the dotted text and the bundled query returns the same rows

#### Scenario: The released corpus still opens

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened
- **THEN** every stored time dimension loads as text and every stored query returns its release's results

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

On every load, whatever the document's version, a stored datasource custom granularity whose name case-insensitively matches any reserved library name SHALL be renamed to `<name>_granularity`, or `<name>_granularity_<n>` with the smallest `n` ≥ 2 that is free, and every reference to it in that datasource's stored models, saved queries, query-backed models and memories SHALL be rewritten to the new name in every position a granularity name is accepted. Each rename SHALL be logged as a warning naming the datasource and both names. The rename SHALL be a function of the stored datasource alone, and dependent documents SHALL be persisted before the datasource, so a load interrupted at any point completes on the next load. A client saving a custom granularity with a reserved name SHALL still be refused.

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

#### Scenario: A granularity colliding with a later library addition

- **WHEN** a datasource stored at the current version defines the granularity `left`, which a later library release reserves, and a stored query buckets by `left(created_at)`
- **THEN** on load the granularity is `left_granularity`, the query reads `left_granularity(created_at)`, and it returns the rows it returned before

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
