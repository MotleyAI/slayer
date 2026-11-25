## ADDED Requirements

### Requirement: A column reference renders the same in every clause

Within one relational scope, every reference that reads the same physical column of a dataset field — a host
field or a joined dataset's field across any relationship path — SHALL render as the same identifier in every
clause that reads it: the projection, `GROUP BY`, `ORDER BY`, `WHERE`, `HAVING`, aggregate arguments, ranking
and partition expressions, time truncation, a semi-join `EXISTS`, and the inner reads of an untrusted to-one
hop's distinct projection. That identifier SHALL be quoted exactly as the dialect quotes any other emitted
column identifier, so a mixed-case column reaches the same physical column on a case-folding dialect; a value
the distinct projection carries out under the field's own name SHALL be projected and read back under that
same quoted identifier. Other result aliases and columns read back from a materialised stage or CTE are outside
this requirement. A plain field reference whose relationship path or leaf does not resolve to a declared field
SHALL fail closed with the typed unknown-reference error in every clause, filters and ordering included, and no
SQL SHALL be emitted.

Each scenario that names no relationship shape holds both for untrusted relationships (read through the
distinct projection) and for trusted ones (a flat join).

#### Scenario: A joined mixed-case dimension filtered on itself

- **WHEN** `orders` (`orderNumber`) relates to `accounts` (`extRef`) and a query with population `orders` has
  dimensions `orderNumber` and `accounts.extRef` and the filter `accounts.extRef = 'a'`, on Postgres or
  Snowflake
- **THEN** every read of `extRef` is quoted under one qualifier in the projection, `GROUP BY` and `WHERE`, and
  on an untrusted relationship the distinct projection reads the quoted column from the `accounts` table and
  projects it under the same quoted name

#### Scenario: A joined mixed-case dimension grouped under a measure

- **WHEN** a query with population `orders` has the dimension `accounts.extRef` and the measure
  `{"formula": "count(*)"}`, with no filter, on Postgres or Snowflake
- **THEN** `extRef` is quoted in the projection and `GROUP BY`, and inside the distinct projection on an
  untrusted relationship

#### Scenario: A joined mixed-case field as an aggregate argument and in a post-aggregation filter

- **WHEN** a query with population `orders` has the dimension `orderNumber`, the measures
  `max(accounts.signedUpAt)` and `count(distinct accounts.extRef)`, and a filter on that count
- **THEN** every read of `signedUpAt` and `extRef` is quoted, inside the aggregates and in the `HAVING` that
  filters on them

#### Scenario: A two-hop mixed-case field

- **WHEN** `accounts` relates to `regions` (`regionName`) and a query with population `orders` groups by
  `accounts.regions.regionName`
- **THEN** `regionName` is quoted and qualified by the alias under which the `regions` table is read

#### Scenario: The same mixed-case leaf on two hops

- **WHEN** `accounts` and `regions` both declare `displayName` and a query with population `orders` groups by
  `accounts.displayName` and `accounts.regions.displayName` over untrusted relationships
- **THEN** both reads are quoted, each qualified by the alias of the table it reads, inside the nested distinct
  projections

#### Scenario: A two-hop field under a fitted or colliding relationship alias

- **WHEN** a two-hop path's alias is shortened to the dialect's identifier limit, or disambiguated against a
  dataset literally named `<a>__<b>`
- **THEN** the mixed-case field's qualifier equals the alias under which its table is read, and the field is
  quoted

#### Scenario: A filter on a two-hop path whose alias collides with a dataset name

- **WHEN** `orders` relates to `stores` → `regions` and to a dataset literally named `stores__regions`, and a
  query filters on `stores.regions.population = 1`
- **THEN** every read of `population` is qualified by the alias under which the `regions` table is read

#### Scenario: A joined mixed-case time dimension

- **WHEN** a query with population `orders` has the time dimension `month(accounts.signedUpAt)`, directly and
  as the time axis of a duration window and of a collapsed window grain
- **THEN** `signedUpAt` is quoted inside every truncation and window-source expression that reads it

#### Scenario: A ranked aggregate partitioned or ordered by a joined mixed-case field

- **WHEN** a `first` / `last` measure is partitioned by `accounts.extRef`, and separately ranked by the joined
  mixed-case time field `accounts.signedUpAt`
- **THEN** the joined field is quoted inside the ranked subquery

#### Scenario: Ordering by a joined mixed-case field

- **WHEN** a query orders by `accounts.extRef` while projecting it, while grouping without projecting it, and
  in an ungrouped query that neither projects nor materialises it
- **THEN** every source-column read of `extRef` is quoted

#### Scenario: A mixed-case field inside a semi-join

- **WHEN** a query with population `accounts` filters through its to-many relationship to `orders` on
  `orders.orderNumber = 'n1'`, lowered to an `EXISTS`
- **THEN** `orderNumber` is quoted inside the `EXISTS`

#### Scenario: Lowercase fields are unchanged

- **WHEN** every field name is lowercase
- **THEN** the emitted SQL is byte-identical to the SQL emitted before this requirement

#### Scenario: Joined mixed-case fields execute on Postgres

- **WHEN** Postgres tables are created with quoted camelCase names and the first two scenarios' queries execute
  against them
- **THEN** both return the hand-computed rows

#### Scenario: An unresolvable plain field fails closed

- **WHEN** a plain field reference reaching SQL emission names a leaf its terminal dataset does not declare, or
  a relationship path that does not resolve, in a projection, a filter, an ordering, or a semi-join node
- **THEN** generation fails with the typed unknown-reference error and no SQL is emitted
