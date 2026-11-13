## ADDED Requirements

### Requirement: Date literals in imported predicates keep Ossie's meaning

A string literal compared with a temporal field in an imported metric — in a `FILTER (WHERE …)` predicate or a `CASE` condition — SHALL mean what it means in Ossie, a plain comparison against the literal's value; a date-only literal is the instant at its midnight.

#### Scenario: Date-only upper bound in a filtered count

- **WHEN** a dataset-scoped metric is `COUNT(*) FILTER (WHERE created_at <= '2024-04-10')` over rows at `2024-03-05 08:00` and `2024-04-10 09:30`
- **THEN** the measure returns 1

#### Scenario: Date-only bound in a CASE condition

- **WHEN** a dataset-scoped metric is `SUM(CASE WHEN created_at > '2024-04-10' THEN amount END)` over the same rows with amounts 10 and 20
- **THEN** the measure returns 20
