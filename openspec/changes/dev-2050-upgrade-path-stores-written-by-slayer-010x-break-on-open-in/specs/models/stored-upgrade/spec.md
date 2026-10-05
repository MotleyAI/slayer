## Purpose

Guarantees that a store written by any released SLayer version still loads, re-saves, re-ingests and queries with that release's results when opened by a later version. Stored documents are repaired on load; live client input keeps today's validation.

## ADDED Requirements

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
- It is a direct comparison operand (`=`, `!=`, `<`, `<=`, `>`, `>=`, either operand order, either `BETWEEN` bound, or any element of an all-literal `IN` list).
- The other side is a column reference that resolves, through the stored models and their joins, to a DATE or TIMESTAMP column.

A literal whose column cannot be resolved, or that compares a non-temporal column, SHALL be left unchanged.

#### Scenario: An offset literal against a timestamp runs

- **WHEN** a stored query filters `ordered_at >= '2024-01-01T00:00:00Z'` where `ordered_at` is a TIMESTAMP column
- **THEN** it loads and runs, filtering on `2024-01-01 00:00:00`

#### Scenario: Every operand layout is repaired

- **WHEN** stored filters read `'2024/01/01' <= ordered_at`, `ordered_at BETWEEN '2024-01-01T00:00:00Z' AND '2024-06-30T00:00:00+02:00'`, `ordered_at IN ('2024/01/01', '2024/02/01')` and `customers.signed_up_at >= '2024-01-01T00:00:00Z'` (a joined TIMESTAMP column)
- **THEN** every legacy literal in them is repaired and each filter runs

#### Scenario: Non-temporal and unresolvable comparisons stay verbatim

- **WHEN** stored filters read `status = '2024/01/01'` (a TEXT column) and `ordered_at IN ('2024/01/01', status)`
- **THEN** both are left unchanged
