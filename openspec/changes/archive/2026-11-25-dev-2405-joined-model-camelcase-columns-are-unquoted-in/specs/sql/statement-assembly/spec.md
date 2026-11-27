## ADDED Requirements

### Requirement: A column reference renders the same in every clause

Within one relational scope, every reference that reads the same physical model column — a host
column or a joined model's column across any join path — SHALL render as the same identifier in
every clause that reads it: the projection, `GROUP BY`, `ORDER BY`, `WHERE`, `HAVING`, aggregate
arguments, ranking and partition expressions, and time truncation. That identifier SHALL be quoted
exactly as the dialect quotes any other emitted column identifier, so a mixed-case column reaches the
same physical column on a case-folding dialect. Result aliases and columns read back from a
materialised stage or CTE are not physical model columns and are outside this requirement. A plain
column reference whose join path or leaf does not resolve to a declared column SHALL fail closed with
the typed unknown-reference error in every clause, and no SQL SHALL be emitted.

#### Scenario: A joined mixed-case dimension filtered on itself

- **WHEN** `orders` (`orderNumber`) joins `accounts` (`extRef`) and a query rooted at `orders` has
  dimensions `orderNumber` and `accounts.extRef` and the filter `accounts.extRef = 'a'`, on Postgres
- **THEN** `extRef` is read as the same quoted identifier on the `accounts` join alias in the
  projection, `GROUP BY` and `WHERE`

#### Scenario: A joined mixed-case dimension grouped under a measure

- **WHEN** a query rooted at `orders` has the dimension `accounts.extRef` and the measure
  `{"formula": "count(*)"}`, with no filter, on Postgres or Snowflake
- **THEN** `extRef` is quoted in the projection and `GROUP BY`

#### Scenario: A two-hop mixed-case column

- **WHEN** `accounts` joins `regions` (`regionName`) and a query rooted at `orders` groups by
  `accounts.regions.regionName`
- **THEN** `regionName` is quoted on the two-hop join alias, which equals the alias the `FROM` joins

#### Scenario: A two-hop column under a fitted or colliding join alias

- **WHEN** a two-hop path's join alias is shortened to the dialect's identifier limit or
  disambiguated against a colliding name
- **THEN** the mixed-case column's qualifier equals the emitted `JOIN` alias and the column is quoted

#### Scenario: A joined mixed-case time dimension

- **WHEN** a query rooted at `orders` has the time dimension `month(accounts.signedUpAt)`, directly
  and as the time axis of a duration window and of a collapsed window grain
- **THEN** `signedUpAt` is quoted inside every truncation and window-source expression that reads it

#### Scenario: A ranked aggregate partitioned or ordered by a joined mixed-case column

- **WHEN** a `first` / `last` measure is partitioned by `accounts.extRef`, and separately ranked by
  the joined mixed-case time column `accounts.signedUpAt`
- **THEN** the joined column is quoted inside the ranked subquery

#### Scenario: Ordering by a joined mixed-case column

- **WHEN** a query orders by `accounts.extRef` while projecting it, while grouping without
  projecting it, and in an ungrouped query that neither projects nor materialises it
- **THEN** every source-column read of `extRef` is quoted, and the ungrouped query orders by the
  quoted joined-table reference

#### Scenario: Lowercase columns are unchanged

- **WHEN** the joined columns' names are all lowercase
- **THEN** the emitted SQL is byte-identical to the SQL emitted before this requirement

#### Scenario: Joined mixed-case columns execute on Postgres

- **WHEN** Postgres tables are created with quoted camelCase names and the first two scenarios'
  queries execute against them
- **THEN** both return the hand-computed rows

#### Scenario: An unresolvable plain column fails closed

- **WHEN** a plain column reference reaching SQL emission names a leaf its terminal model does not
  declare, in a projection or in a filter
- **THEN** generation fails with the typed unknown-reference error and no SQL is emitted
