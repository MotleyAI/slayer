## MODIFIED Requirements

### Requirement: A released store opens with its release's results

A store written by SLayer 0.10.2, and a store written by SLayer 1.8.2, SHALL each load completely when opened by the current version, both with its datasource present and with the datasource entry absent. The 0.10.2 store SHALL hold every model and memory shape that release could write: ingested models, a Cube import whose foreign key is not a dimension, a multi-stage query-backed model ordering by `rank(...)`, an aggregation reading a quoted parameter, legacy time literals, and a memory with a query. The 1.8.2 store SHALL hold the list-column shapes that release could write: ingested native list columns, an `sql`-mode list column without an `element_type`, a sole primary key allowing `collect`, stored measures and a query-backed model using `collect`, the list functions and list elements, and a memory with such a query. With the datasource present, every model SHALL re-save and the datasource SHALL re-ingest. Every query the release could run SHALL return the rows the release returned.

#### Scenario: The 0.10.2 corpus opens with its datasource present

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened with its DuckDB datasource present
- **THEN** every model and memory loads, every model re-saves, the datasource re-ingests, and every recorded query returns the rows recorded from 0.10.2

#### Scenario: The 0.10.2 corpus opens offline

- **WHEN** the same store is opened with its datasource entry removed
- **THEN** every model and memory loads, without any connection being attempted

#### Scenario: The 1.8.2 corpus opens with its datasource present

- **WHEN** the committed store written by `motley-slayer==1.8.2`, on both the YAML and the SQLite store, is opened with its DuckDB datasource present
- **THEN** every model and memory loads, every model re-saves, the datasource re-ingests, and every recorded query returns the rows recorded from 1.8.2

#### Scenario: The 1.8.2 corpus opens offline

- **WHEN** the same store is opened with its datasource entry removed
- **THEN** every model and memory loads, without any connection being attempted

### Requirement: Stored granularities colliding with a library name are renamed

On every load, whatever the document's version, a stored datasource custom granularity whose name case-insensitively matches any reserved library name SHALL be renamed to `<name>_granularity`, or `<name>_granularity_<n>` with the smallest `n` ≥ 2 that is free, and every reference to it in that datasource's stored models, saved queries, query-backed models and memories SHALL be rewritten to the new name in every position a granularity name is accepted. Each rename SHALL be logged as a warning naming the datasource and both names. The rename SHALL be a function of the stored datasource alone, and dependent documents SHALL be persisted before the datasource, so a load interrupted at any point completes on the next load. This SHALL hold on both the YAML and the SQLite store. A client saving a custom granularity with a reserved name SHALL still be refused.

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

#### Scenario: A rename converges

- **WHEN** a store whose granularity `contains` was renamed is loaded again, on the YAML and on the SQLite store
- **THEN** no document changes and no rename warning is logged

#### Scenario: A granularity colliding with a later library addition

- **WHEN** a datasource stored at the current version defines the granularity `left`, which a later library release reserves, and a stored query buckets by `left(created_at)`
- **THEN** on load the granularity is `left_granularity`, the query reads `left_granularity(created_at)`, and it returns the rows it returned before

#### Scenario: A new reserved-name granularity is refused

- **WHEN** a client saves a datasource defining the granularity `left`
- **THEN** the save fails naming the reserved name
