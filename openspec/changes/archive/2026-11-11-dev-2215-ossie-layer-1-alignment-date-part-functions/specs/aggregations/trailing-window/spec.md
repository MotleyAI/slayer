## MODIFIED Requirements

### Requirement: Windowed first and last pick within the interval
`first(window=)` and `last(window=)` SHALL yield the value of the interval row that
ranks earliest or latest by the ranking time column, resolved as for plain `first`/`last`:
the explicit positional argument, else the query's temporal row dimension, else the time
dimension's raw column, else the model's `default_time_dimension`. An empty interval
SHALL yield NULL. A row whose ranking key is NULL SHALL never be picked, as for plain
`first`/`last` (`aggregations/functional-form` › first and last never pick a NULL ranking
key), so an interval whose rows all have a NULL ranking key SHALL yield NULL; ties SHALL
order per the dialect's native ordering.

#### Scenario: Default ranking is the window axis
- **WHEN** the rows of the first scenario are queried with `first(amount, window='90d')`
  and `last(amount, window='90d')`
- **THEN** first is `{Jan 100, Feb 100, Mar 200}` and last is `{Jan 200, Feb 300, Mar 300}`
  — March's last is the 2024-03-20 row's 300, not the interval max 400

#### Scenario: Explicit ranking column with NULL keys
- **WHEN** the same rows carry `updated_at` = 2024-01-02, 2024-01-16, NULL, 2024-03-25,
  2024-03-16 and the query selects `last(amount, updated_at, window='90d')`
- **THEN** last is `{Jan 200, Feb 200, Mar 400}` on every Tier-1 dialect: February skips the
  NULL-keyed row, and March picks the row with the latest `updated_at` rather than the latest
  `created_at`

#### Scenario: Interval whose only rows have NULL ranking keys
- **WHEN** the same query groups by `region` and the EU/February cell's interval holds
  only the NULL-keyed 300 row
- **THEN** `last(amount, updated_at, window='90d')` for that cell is NULL

#### Scenario: Cross-model windowed last
- **WHEN** a query rooted at `orders` over the `customers.signup_at` month time dimension
  selects `last(customers.spend, customers.signup_at, window='1y')` over customers signed
  up 2024-01-05 (100), 2024-02-10 (150), 2024-03-15 (60), 2024-03-20 (40)
- **THEN** the values are `{Jan 100, Feb 150, Mar 40}`

#### Scenario: Windowed first/last never renders as a plain aggregate
- **WHEN** any `first`/`last` with `window=` is rendered for a Tier-1 dialect
- **THEN** the emitted SQL ranks the interval rows and picks rank 1; it never raises the
  internal plain-renderer guard and never emits `MAX`/`MIN` over the value as a substitute
