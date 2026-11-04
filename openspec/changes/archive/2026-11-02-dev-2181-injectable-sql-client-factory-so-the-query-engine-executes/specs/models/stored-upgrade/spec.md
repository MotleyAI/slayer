## MODIFIED Requirements

### Requirement: Live type refinement applies only to models written before refining ingest

On load, a stored model below schema version 8 SHALL have its DOUBLE and SQLite INT base columns refined against the live datasource. At version 8 and above, ingest already wrote refined types. A stored model at version 8 or above SHALL load without consulting the datasource, keeping its stored column types. Refinement SHALL read only the model's own table, through a client of the query engine that is loading the model. A load that needs refinement while the model's datasource has a stored entry but no query engine is loading it SHALL fail with an error stating that the model must be loaded through a query engine. On SQLite, refinement SHALL keep its sampling rules: a column is narrowed to INT only when the sample positively certifies integer storage, a saturated or failed sample leaves the stored type unchanged, and stored INT is widened only on evidence of non-integer storage.

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
- **WHEN** a raw v7 model with a DOUBLE base column backed by a live integer column is loaded through a query engine with its datasource present
- **THEN** the column loads as INT

#### Scenario: Refinement reads only the model's table
- **WHEN** a raw v7 model is loaded through a query engine whose datasource holds several tables
- **THEN** exactly one table's columns are read

#### Scenario: Refinement without a query engine is refused
- **WHEN** a raw v5 model with a refineable DOUBLE base column is loaded directly from storage, with its datasource entry present
- **THEN** loading fails with the error stating that the model must be loaded through a query engine

#### Scenario: Refinement through a gateway
- **WHEN** a raw v7 model is loaded through a query engine whose client factory returns a gateway client, with every direct connection path failing
- **THEN** the column is refined from the gateway's metadata

#### Scenario: SQLite refinement keeps its sampling rules
- **WHEN** raw v7 SQLite models are loaded through a query engine with base columns holding integers only, mixed REAL values, BLOBs, coercible text, non-coercible text, non-finite text, more rows than the sample cap, and in an attached schema with a quoted identifier
- **THEN** only the integers-only column is narrowed, stored INT columns with non-integer evidence are widened, and the saturated column keeps its stored type

#### Scenario: The 0.10.2 corpus opens through storage and through an engine
- **WHEN** the committed 0.10.2 store is opened offline, both directly from storage and through a query engine
- **THEN** every model and memory loads both ways, without any connection being attempted
