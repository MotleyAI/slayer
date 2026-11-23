## MODIFIED Requirements

### Requirement: A spine query's population is the spine times P

A query with one or more time dimensions on `time_spine.timestamp` SHALL have population `time_spine × P`, where P is the model named by `population`, else the dimension anchor of its other dimensions and time dimensions (`queries/population` › Dimension-determined default; field filters restrict P by association and never choose it), or the one-row unit when there are no other dimensions (`population: time_spine` also denotes the unit). The result SHALL have one row per combination of a spine bucket in range and a distinct value combination of P's dimensions among P's filtered rows; adding or removing a measure SHALL never change this row set, and a query omitting `population` SHALL keep this row set rather than its row supply. This SHALL hold for every query of a query list alike: the main query and each named stage.

#### Scenario: Every month in range, both facts on one axis

- **WHEN** a query groups by months Jan–Jun and selects `sum(orders.amount)`, `sum(returns.amount)` and `count(orders.id)`
- **THEN** the rows are Jan (150, NULL, 2), Feb (70, 20, 1), Mar (NULL, 10, 0), Apr (NULL, NULL, 0), May (NULL, 5, 0), Jun (NULL, NULL, 0), with no warnings

#### Scenario: Per-group grid

- **WHEN** the query groups by `customers.region` and the spine month with `date_range: ["2025-01-01", "2025-03-31"]` and selects `sum(orders.amount)` and `sum(returns.amount)`
- **THEN** there are 9 rows (regions N, S, E × Jan–Mar): N Jan (100, NULL), N Feb (70, 20), N Mar (NULL, NULL), S Jan (50, NULL), S Feb (NULL, NULL), S Mar (NULL, 10), and E NULL in every month

#### Scenario: Explicit P and explicit spine

- **WHEN** the per-group query names `population: customers`, and the two-fact query names `population: time_spine`
- **THEN** each returns exactly the rows of its inferred twin

#### Scenario: Two spine time dimensions

- **WHEN** a query groups by the spine at `year` and at `month` over Jan–Jun
- **THEN** there are 6 rows, each month paired with year 2025

#### Scenario: Measures never change the rows

- **WHEN** `sum(returns.amount)` is removed from, or `avg(orders.amount)` added to, any spine query above
- **THEN** the row set is unchanged

#### Scenario: A spine stage keeps every bucket

- **WHEN** a query list's stage `monthly` groups by the spine month with `date_range: ["2025-01-01", "2025-03-31"]` and selects `sum(orders.amount)` named `o`, once without `population` and once with `population: time_spine`, and the main query over `monthly` selects `sum(o)` and `count(*)`
- **THEN** both variants read 220 and 3: the stage has one row per month Jan–Mar, March included

#### Scenario: A per-group spine stage

- **WHEN** the stage `monthly` also groups by `customers.region`, and the main query over it groups by `region` and selects `sum(o)` and `count(*)`
- **THEN** the rows are N (170, 3), S (50, 3), E (NULL, 3)

#### Scenario: A spine stage feeding a spine query

- **WHEN** the main query over the stage `monthly` groups by months Jan–Jun and selects `sum(monthly.o)`
- **THEN** the stage's axis is its month column and the rows are Jan 150, Feb 70, then NULL for Mar–Jun

#### Scenario: A filter on a finer dataset keeps P
- **WHEN** the per-group query omits `population` and adds the filter `orders.amount > 60`
- **THEN** P stays `customers`, restricted to customers with such an order, and the population is reported `time_spine × customers`

#### Scenario: An orphan beside the spine is not yet a row
- **WHEN** the per-group query omits `population` and an order references no customer
- **THEN** no NULL-region row appears: the spine query keeps `time_spine × customers`
