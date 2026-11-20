## MODIFIED Requirements

### Requirement: The rename changes no result

A query SHALL return the same rows, values, result keys and column order whether it is written in the pre-2.0 or the Ossie vocabulary.

#### Scenario: Old and new spellings give the same result
- **WHEN** a query is run once in the pre-2.0 spelling and once in the new spelling
- **THEN** both responses carry identical rows, values and result keys

#### Scenario: A multi-stage query keeps its keys
- **WHEN** a multi-stage query with metric entries, field references and an order item is run in both spellings
- **THEN** both responses carry identical result keys

#### Scenario: A refinement matches the same entries
- **WHEN** a saved query is run by name with a refinement written in the pre-2.0 spelling and with one written in the new spelling
- **THEN** both runs return identical results
