## ADDED Requirements

### Requirement: Translated time bounds keep their SQL meaning

Every time bound the facade emits — a lifted `BETWEEN`, or a verbatim `=`, `!=`, `<`, `<=`, `>`, `>=` comparison whose other operand is a DATE or TIMESTAMP column in the catalog, whether or not it is projected — SHALL carry each date-only literal as the instant at its midnight (`'2024-12-31'` → `'2024-12-31 00:00:00'`), including typed `DATE '…'` / `TIMESTAMP '…'` literals and either operand order, so the translated query matches exactly the rows the source SQL compares against. The conversion SHALL operate on the parsed SQL tree, never on SQL text.

#### Scenario: Lifted BETWEEN keeps midnight semantics

- **WHEN** the facade translates `WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31'` over rows at `2024-12-31 00:00` and `2024-12-31 10:00`
- **THEN** the lifted `date_range` is `['2024-01-01 00:00:00', '2024-12-31 00:00:00']` and the executed result includes the midnight row and excludes the `10:00` row

#### Scenario: Verbatim comparator keeps midnight semantics

- **WHEN** the facade translates `WHERE ordered_at <= '2024-12-31'`
- **THEN** the emitted filter is `ordered_at <= '2024-12-31 00:00:00'`

#### Scenario: Typed literal and reversed operand order

- **WHEN** the facade translates `WHERE DATE '2024-01-01' <= ordered_at`
- **THEN** the emitted filter compares `ordered_at` against the instant `2024-01-01 00:00:00` with the same strictness

#### Scenario: Unprojected timestamp column

- **WHEN** the facade translates `SELECT revenue_sum FROM orders WHERE ordered_at <= '2024-12-31' AND ordered_at != '2024-06-01'` with no time dimension projected
- **THEN** the filters are `ordered_at <= '2024-12-31 00:00:00'` and `ordered_at != '2024-06-01 00:00:00'`, and the executed result includes a `2024-12-31 00:00` row and excludes a `2024-12-31 10:00` row

#### Scenario: Text columns are untouched

- **WHEN** the facade translates `WHERE status = '2024-12-31'`
- **THEN** the filter keeps `'2024-12-31'`

## MODIFIED Requirements

### Requirement: Only a source-SQL BETWEEN lifts to date_range

The facade SHALL lift a WHERE conjunct of the form `<time-dim> BETWEEN <literal> AND <literal>`
into the matching `TimeDimension.date_range` as a two-element `[low, high]` range, each date-only
literal carried as its midnight instant. No other predicate shape SHALL populate `date_range`.

#### Scenario: Two-literal BETWEEN lifts

- **WHEN** the facade translates `WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31'`
  and `ordered_at` is a projected time dimension
- **THEN** that time dimension's `date_range` is `['2024-01-01 00:00:00', '2024-12-31 00:00:00']` and no
  filter string is emitted for the conjunct

#### Scenario: BETWEEN plus a comparator on the same dimension

- **WHEN** the facade translates `WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31' AND ordered_at >= '2024-06-01'`
- **THEN** the BETWEEN lifts to `date_range` and the comparator is emitted as a query filter
  against the instant `2024-06-01 00:00:00`, preserving AND semantics

### Requirement: Relational time comparators translate verbatim

The facade SHALL translate every relational comparator (`>=`, `>`, `<=`, `<`) on a time
dimension verbatim into `SlayerQuery.filters` — whether one-sided or part of a pair —
preserving the operator's strictness exactly, with each date-only literal carried as its
midnight instant. It MUST NOT merge comparators into `date_range`, and MUST NOT produce a
`date_range` containing a null bound.

#### Scenario: One-sided lower bound

- **WHEN** the facade translates `WHERE ordered_at >= '2024-01-01'` with no upper bound
- **THEN** the time dimension's `date_range` is unset and the filters contain
  `ordered_at >= '2024-01-01 00:00:00'`

#### Scenario: One-sided upper bound

- **WHEN** the facade translates `WHERE ordered_at <= '2024-12-31'` with no lower bound
- **THEN** the time dimension's `date_range` is unset and the filters contain
  `ordered_at <= '2024-12-31 00:00:00'`

#### Scenario: Strict lower bound alone

- **WHEN** the facade translates `WHERE ordered_at > '2024-01-01'`
- **THEN** the time dimension's `date_range` is unset and the filters contain
  `ordered_at > '2024-01-01 00:00:00'`

#### Scenario: Strict upper bound alone

- **WHEN** the facade translates `WHERE ordered_at < '2025-01-01'`
- **THEN** the time dimension's `date_range` is unset and the filters contain
  `ordered_at < '2025-01-01 00:00:00'`

#### Scenario: Paired inclusive bounds do not lift

- **WHEN** the facade translates `WHERE ordered_at >= '2024-01-01' AND ordered_at <= '2024-12-31'`
- **THEN** the time dimension's `date_range` is unset and both comparators appear
  in the filters with their strictness, against the midnight instants

#### Scenario: Paired mixed-strictness bounds preserve strictness

- **WHEN** the facade translates `WHERE ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'`
- **THEN** the time dimension's `date_range` is unset and both comparators appear
  in the filters with their strictness, so a row at exactly `2025-01-01 00:00:00` is excluded

#### Scenario: Reversed operand order stays verbatim

- **WHEN** the facade translates `WHERE '2024-01-01' <= ordered_at`
- **THEN** the conjunct is emitted as a query filter (no `date_range`) against the instant
  `2024-01-01 00:00:00`, and the resulting query is executable
