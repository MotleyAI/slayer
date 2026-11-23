## MODIFIED Requirements

### Requirement: Unit and part arguments are closed string literals
The `part` argument of `date_part` and the `unit` argument of `date_diff` / `date_add` SHALL be a
string literal, matched case-insensitively, from that function's closed set:
- `date_part`: `year`, `iso_year`, `quarter`, `month`, `week`, `day`, `day_of_week`,
  `day_of_year`, `hour`, `minute`, `second`;
- `date_diff` and `date_add`: `second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`,
  `quarter`, `year`.
The `part` of `date_part` MAY also be written as a bare word from that set or from the date-part
aliases, as `EXTRACT` takes it (Ossie's `DATE_PART(MONTH, x)`), with the same meaning as its quoted
spelling.
Any other value, and any non-literal (a column, a `{variable}` that does not substitute to a listed
word, an expression), SHALL be rejected before binding with an error listing the accepted values.
Differently-cased spellings of the same unit SHALL intern to the same value.

#### Scenario: Unknown unit rejected
- **WHEN** a filter uses `date_diff('fortnight', created_at, shipped_at) > 1`
- **THEN** the query is rejected with an error listing the accepted `date_diff` units

#### Scenario: Extraction-only part rejected as a difference unit
- **WHEN** a filter uses `date_diff('day_of_week', created_at, shipped_at) > 1`
- **THEN** the query is rejected with an error listing the accepted `date_diff` units

#### Scenario: Column as a unit rejected
- **WHEN** a filter uses `date_part(status, created_at) = 1`
- **THEN** the query is rejected because the part must be a string literal

#### Scenario: Unit case folds
- **WHEN** one measure uses `date_part('MONTH', created_at)` and another `date_part('month', created_at)`
- **THEN** both resolve to the same value

#### Scenario: Bare part word
- **WHEN** one measure uses `date_part(MONTH, created_at)` and another `date_part('month', created_at)`
- **THEN** both resolve to the same value
