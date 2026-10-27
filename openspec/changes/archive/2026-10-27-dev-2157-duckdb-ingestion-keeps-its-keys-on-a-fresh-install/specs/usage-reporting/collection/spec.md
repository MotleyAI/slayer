## ADDED Requirements

### Requirement: A retired usage key counts as its surface's other
Counts recorded under a usage key that a later SLayer version no longer defines (a renamed or removed
command, tool or route) SHALL still be reported after an upgrade: when a stored batch is read, each
such key's counts SHALL be added to its surface's `other` key where that key exists, and SHALL be
dropped on their own where it does not. Every other count in the batch SHALL be reported unchanged; a
retired key SHALL NOT cause a batch to be discarded.

#### Scenario: Counts under a renamed CLI command survive an upgrade
- **WHEN** a batch stored before the upgrade counts 2 successful `cli:search.refresh-samples` runs,
  1 `cli:other` run and 5 `cli:query` runs, and is read by a version where the command is
  `datasources refresh-samples`
- **THEN** the report counts 3 successful `cli:other` runs and 5 `cli:query` runs

#### Scenario: A retired key on a surface without an other key
- **WHEN** a stored batch carries counts under a `pg:` key the current version does not define,
  alongside `pg:query` counts
- **THEN** the unknown key's counts are dropped and the `pg:query` counts are reported
