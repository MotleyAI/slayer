## ADDED Requirements

### Requirement: A cast to INT is 64-bit

Wherever generated SQL casts a value to SLayer's INT type, the cast target SHALL be the dialect's 64-bit integer type, so an integer value that fits in 64 bits is returned unchanged. A value already cast to that target SHALL NOT be cast again.

#### Scenario: Large integers survive a derived INT column
- **WHEN** a model's derived INT column computes values above 2³¹ on Postgres and DuckDB and a query selects it
- **THEN** the generated SQL casts it to the 64-bit integer type and the values are returned unchanged

#### Scenario: No double cast
- **WHEN** a derived INT column's expression is itself a cast to the 64-bit integer type
- **THEN** the generated SQL casts it once
