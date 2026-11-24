## ADDED Requirements

### Requirement: Trust, enforced keys and an unset cardinality round-trip
A relationship's `trusted` flag and unset cardinality, and a dataset's `enforced_keys`, SHALL
be written to the entity's SLAYER entry and restored on load. A relationship whose
cardinality is unset SHALL NOT be written as a plain Ossie relationship. An Ossie
relationship without a SLAYER entry SHALL load untrusted with cardinality `many_to_one`, and a
dataset without one with no enforced keys.

#### Scenario: An imported document is never trusted
- **WHEN** the Ossie retail model is stored and queried on data with customer C1 duplicated
- **THEN** EAST is 350

#### Scenario: A trusted relationship round-trips
- **WHEN** a document dataset's relationship is saved with `trusted: true`, the document is
  exported and imported again
- **THEN** the relationship is trusted and the duplicated-C1 query returns EAST 650

#### Scenario: An unset cardinality round-trips
- **WHEN** a document dataset's relationship is saved with no cardinality
- **THEN** it loads with no cardinality, not `many_to_one`

#### Scenario: Enforced keys round-trip
- **WHEN** a document dataset is saved with `enforced_keys: [[customer_id]]`
- **THEN** it loads with the same enforced keys
