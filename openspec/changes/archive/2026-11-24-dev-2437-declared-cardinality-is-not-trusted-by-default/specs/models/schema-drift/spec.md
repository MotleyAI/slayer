## ADDED Requirements

### Requirement: Drift clears a vanished enforced key
Schema drift SHALL report, and its apply SHALL remove, each enforced key set of a dataset that
the live table no longer carries as a primary key or unconditional unique key, and every
enforced key set of a dataset whose dialect does not enforce unique keys. The edit removes
the enforcement record only: the declared key, the relationships and their `trusted` flags
are left as they are.

#### Scenario: A dropped constraint clears the enforced key
- **WHEN** `customers.enforced_keys` lists `[customer_id]` and the live DuckDB table no longer
  has that primary key
- **THEN** drift reports the enforced key and applying it removes the record, after which a
  join onto `customers` is no longer trusted unless its relationship declares `trusted`

#### Scenario: An author's trusted flag is never cleared
- **WHEN** a relationship onto a Snowflake table declares `trusted: true`
- **THEN** drift reports nothing about it and the flag survives an apply
