## MODIFIED Requirements

### Requirement: Report identity and time resolution
A usage report SHALL carry the install ID, a random batch ID, the report schema version (3), the first and last calendar dates it covers, and the install's first-successful-query date. It SHALL NOT carry timestamps finer than a date.

#### Scenario: Dates only
- **WHEN** a report covering activity on two days is built
- **THEN** its period holds two dates and no time-of-day values

#### Scenario: Schema version 2
- **WHEN** a report is built from a pending batch of report schema 2
- **THEN** its schema version is 3

#### Scenario: Schema version 3
- **WHEN** a report is built
- **THEN** its schema version is 3

### Requirement: A retired usage key counts as its surface's other
Counts recorded under a usage key that a later SLayer version no longer defines (a renamed or removed
command, tool or route) SHALL still be reported after an upgrade, as SHALL counts a batch of an older
report schema records under a context key since renamed. When a stored batch is read, the counts of a
usage key or context key renamed by the 2.0 vocabulary rename (an MCP tool, a REST route template, a CLI
command, the model-count bucket) SHALL be added to its new key's counts; each other undefined usage
key's counts SHALL be added to its surface's `other` key where that key exists, and SHALL be dropped on
their own where it does not. Every other count in the batch SHALL be reported unchanged; a retired key
SHALL NOT cause a batch to be discarded. A batch of an older report schema SHALL be sent in the current
schema.

#### Scenario: Counts under a renamed CLI command survive an upgrade
- **WHEN** a batch stored before the upgrade counts 2 successful `cli:search.refresh-samples` runs,
  1 `cli:other` run and 5 `cli:query` runs, and is read by a version where the command is
  `datasources refresh-samples`
- **THEN** the report counts 3 successful `cli:other` runs and 5 `cli:query` runs

#### Scenario: A retired key on a surface without an other key
- **WHEN** a stored batch carries counts under a `pg:` key the current version does not define,
  alongside `pg:query` counts
- **THEN** the unknown key's counts are dropped and the `pg:query` counts are reported

#### Scenario: Counts under a tool renamed by the vocabulary rename keep their tool
- **WHEN** a pending schema-2 batch counts 2 successful `mcp:create_model` calls and the model-count
  bucket `11-50` once, and a pending schema-3 batch counts 1 successful `mcp:create_dataset` call
- **THEN** one schema-3 report is sent counting 3 successful `mcp:create_dataset` calls, no
  `mcp:create_model` key, no `mcp:other` count from them, and the dataset-count bucket `11-50` once

#### Scenario: Mixed schema spool files are sent together
- **WHEN** schema-1, schema-2 and schema-3 spool files are pending
- **THEN** their counts are sent as one schema-3 report and every file is removed after the send succeeds
