## ADDED Requirements

### Requirement: Queries refuse unknown keys

A query, a query refinement, and every dimension, metric, time dimension, order and extension entry of either SHALL treat a key it does not read as follows. A key whose camelCase-to-snake_case form is a key the object accepts — canonical or a pre-2.0 spelling — SHALL be read as that key, silently, before any pre-2.0 spelling converts, so a camelCase pre-2.0 spelling converts on to its canonical key with that spelling's deprecation warning. Two keys of one object that resolve to the same canonical key SHALL be refused as a spelling conflict naming both. Any other key SHALL be refused with a typed error naming the key, giving its location in the input, and listing the keys the object takes; keys the object reads internally SHALL stay accepted but SHALL NOT be listed. This SHALL hold on every query surface (REST answering 422, MCP, Python client) and for objects built as Python values. A stored query SHALL follow the same rules on load: a camelCase key SHALL convert and the query SHALL serialize with the canonical key; any other unknown key SHALL make that stored document fail to load with the typed error while every other document of the store loads. Dataset-side objects nested in a query (an extension's metrics and relationships, an inline dataset) SHALL keep the dataset rules.

#### Scenario: A camelCase key converts silently
- **WHEN** a query is run with `timeDimensions`, `mainTimeDimension` and `wholePeriodsOnly`, a saved query is run with a refinement carrying `timeDimensions`, and both are run again in snake_case
- **THEN** each pair returns the same rows and SQL, the camelCase run carries no warning, and the camelCase query serializes with the snake_case keys

#### Scenario: A camelCase pre-2.0 spelling converts on to its canonical key
- **WHEN** a query is given `sourceModel: "orders"`
- **THEN** it runs as `population: "orders"` with the one deprecation warning `source_model` raises

#### Scenario: Two spellings of one key conflict
- **WHEN** a query carries both `sourceModel` and `population`, and a time dimension carries both `dateRange` and `date_range`
- **THEN** each is refused with a spelling-conflict error naming both keys

#### Scenario: An unknown key is refused with what the object takes
- **WHEN** queries are given an unknown key at the root, on a dimension entry, a metric entry, a time dimension, an order entry, an extension and a refinement, through REST, MCP and the Python client
- **THEN** each is refused with a typed error naming the key and its location and listing the object's keys, REST answers 422, and no internal key is listed

#### Scenario: An unrelated camelCase key stays unknown
- **WHEN** a time dimension carries `sortOrder`
- **THEN** it is refused with the typed unknown-key error

#### Scenario: Internal keys still round-trip
- **WHEN** a query ordering by an expression is saved, loaded and dumped
- **THEN** it loads and dumps unchanged, including the order entry's internal keys

#### Scenario: Stored queries follow the same rules
- **WHEN** a store holds one saved query whose time dimension carries `dateRange`, one whose dimension entry carries an unknown key, and others, each at the current version and at an older version that migrates
- **THEN** the `dateRange` queries load, apply the range and dump with `date_range`; the unknown-key queries fail to load with the typed error naming the key; every other document loads

### Requirement: Query objects validate assignments

A query, a query refinement, and every dimension, metric, time dimension, order and extension entry SHALL validate a value assigned to one of their attributes after construction with the checks construction applies. A failed assignment SHALL raise and SHALL leave the object exactly as it was before the assignment.

#### Scenario: An invalid assignment is refused and leaves the object unchanged
- **WHEN** a time dimension with a valid `date_range` is assigned `date_range = ["2024-01-01", 5]`, then `date_range = ["bogus"]`, and a query is assigned `limit = "abc"`
- **THEN** each assignment raises, and the time dimension and query keep their previous values

#### Scenario: A valid assignment still applies
- **WHEN** a time dimension is assigned a valid granularity and a one-sided `date_range`
- **THEN** both apply and the time dimension runs with them
