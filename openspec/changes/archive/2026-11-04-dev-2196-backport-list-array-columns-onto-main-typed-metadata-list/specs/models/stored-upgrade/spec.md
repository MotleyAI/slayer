## ADDED Requirements

### Requirement: Library growth never invalidates a stored artefact

Every name the Mode-B library reserves — scalar and list functions, date-part aliases, transforms, built-in granularities and built-in aggregations — SHALL be pinned, so a newly reserved name is only accepted when it is a function (which a model's own definition shadows) or when a stored-upgrade migration covers stored documents already using it. A stored model whose custom aggregation takes any reserved function name SHALL load and answer its aggregation, and a stored datasource whose custom granularity takes any reserved name SHALL load after migration and answer through the renamed granularity.

#### Scenario: Every reserved function name as a stored aggregation

- **WHEN** for each reserved function name a stored model defines a custom aggregation of that name with formula `SUM({value})`
- **THEN** each model loads and the aggregation returns the sum of its column

#### Scenario: Every reserved name as a stored granularity

- **WHEN** for each reserved name a stored datasource defines a custom granularity of that name used by a stored model's time dimension
- **THEN** the datasource and model load and the time dimension buckets by the renamed granularity

#### Scenario: An unpinned reservation is caught

- **WHEN** the library reserves a name outside the pinned set that is neither a function nor covered by a migration
- **THEN** the library-growth check fails, naming the unpinned name

### Requirement: Stored granularities colliding with a library name are renamed

On every load, whatever the document's version, a stored datasource custom granularity whose name case-insensitively matches any reserved library name SHALL be renamed to `<name>_granularity`, or `<name>_granularity_<n>` with the smallest `n` ≥ 2 that is free, and every reference to it in that datasource's stored models, saved queries, query-backed models and memories SHALL be rewritten to the new name in every position a granularity name is accepted. Each rename SHALL be logged as a warning naming the datasource and both names. The rename SHALL be a function of the stored datasource alone, and dependent documents SHALL be persisted before the datasource, so a load interrupted at any point completes on the next load. This SHALL hold on both the YAML and the SQLite store. A client saving a custom granularity with a reserved name SHALL still be refused.

#### Scenario: Granularity named contains

- **WHEN** a datasource stored by the previous release defines the granularity `contains` and a stored model's query-backed source and a memory's query both bucket `created_at` by it
- **THEN** after load the granularity is `contains_granularity`, both documents reference `contains_granularity`, a warning names the rename, and both queries return the rows they returned before

#### Scenario: Granularity named collect

- **WHEN** a stored datasource defines the granularity `collect`
- **THEN** it loads as `collect_granularity`

#### Scenario: The new name is taken

- **WHEN** a stored datasource defines the granularities `contains` and `contains_granularity`
- **THEN** `contains` is renamed to `contains_granularity_2` and references to `contains_granularity` are untouched

#### Scenario: An interrupted load completes

- **WHEN** a load is interrupted after some dependent documents were persisted with the new name and before the datasource was
- **THEN** the next load renames the datasource's granularity to the same name and rewrites the remaining documents, leaving one consistent store

#### Scenario: A rename converges

- **WHEN** a store whose granularity `contains` was renamed is loaded again
- **THEN** no document changes and no rename warning is logged

#### Scenario: A new reserved-name granularity is refused

- **WHEN** a client saves a datasource defining the granularity `contains`
- **THEN** the save fails naming the reserved name
