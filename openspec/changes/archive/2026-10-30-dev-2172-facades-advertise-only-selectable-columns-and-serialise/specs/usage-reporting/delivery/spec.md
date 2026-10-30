## ADDED Requirements

### Requirement: A newer version's spool file keeps what this version can name

Reading a spool file SHALL drop every field and counter key this version does not define and keep every other counter, rather than discarding the file. The dropped parts SHALL never be sent or rewritten. A report SHALL still contain only this version's vocabulary.

#### Scenario: Spool from a newer version
- **WHEN** a spool file carries a known `sum` aggregation count, an unknown flag token and an unknown `features` field
- **THEN** the next report includes the `sum` count, and neither the unknown token nor the unknown field

#### Scenario: Spool from an older version
- **WHEN** a spool file written before function counters existed is read
- **THEN** every counter it holds is included in the next report
