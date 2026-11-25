## MODIFIED Requirements

### Requirement: Filters whose dataset reaches itself are refused
A model's dataset SHALL depend on the dataset of every model its filters read — every model on every join
path in the filters' dependency closure, through the derived columns they name — and on the dataset of every
model owning a windowed column (`models/field-expressions` › A windowed field is a column of its dataset)
in that closure, a windowed column reading every row of its own model's dataset. A model whose dataset
depends on itself SHALL be refused with a filter-cycle error naming the cycle of models in order, and naming
the windowed column when the cycle closes through a windowed column reading its own model's dataset. Saving a model that closes such a cycle through itself
SHALL fail at both save doors (the engine and the storage backend), leaving the store unchanged. A query
reading a model on such a cycle, stored without validation, SHALL fail with the same error. A join edge
alone, or a filter reading a model whose own filters do not lead back, SHALL NOT form a cycle.

#### Scenario: Two filters reading each other are refused at save time
- **WHEN** `a` joins `b` and `b` joins `a`, `a` has filter `b.flag = 1`, and `b` is saved with filter
  `a.flag = 1`
- **THEN** the save fails with the filter-cycle error naming `b → a → b`, through the engine and through
  the storage backend alike

#### Scenario: A cycle through a derived column is refused
- **WHEN** `b`'s filter reads a derived column of `b` whose `sql` reads `a.flag`, and `a`'s filter reads `b`
- **THEN** the save closing the cycle fails with the filter-cycle error

#### Scenario: A filter reading its own model's windowed column is refused
- **WHEN** model `orders_win` declares the windowed column `ranked` (dialect entry
  `rank() over (order by amount)`) and is saved with the filter `orders_win.ranked > 1`, and separately with
  the filter `orders_win.rank_plus > 1` where `rank_plus` is the derived column `orders_win.ranked + 0`
- **THEN** each save fails with the filter-cycle error naming `orders_win → orders_win` and the column
  `ranked`, through the engine and through the storage backend alike

#### Scenario: A cycle closing through a joined windowed column's definition is refused
- **WHEN** `a` and `b` join each other many-to-one, `b` declares the windowed column `rn` partitioned by
  `a.flag`, and `a` is saved with the filter `b.rn = 1`
- **THEN** the save fails with the filter-cycle error naming `a → b → a`

#### Scenario: A stored cycle fails the query
- **WHEN** models forming a filter cycle were stored without validation and a query reads one of them
- **THEN** the query fails with the filter-cycle error

#### Scenario: A stored own windowed filter fails the query
- **WHEN** `orders_win` with the filter `orders_win.ranked > 1` was stored without validation and a query
  reads `orders_win`
- **THEN** the query fails with the filter-cycle error naming the column `ranked`

#### Scenario: Reverse edges without filter recursion are accepted
- **WHEN** `a` and `b` join each other, `a` has filter `b.flag = 1`, and `b` has no filter or a filter
  reading only its own columns
- **THEN** both saves succeed and queries reading either model run

### Requirement: Model filters are validated wherever read and at save time
A model filter referencing a saved measure SHALL be refused with the model-filter error — judged for each
reference against the model that owns it, including qualified references to joined models and references
reached through derived columns. Saving such a model SHALL fail at both save doors. A query SHALL fail with
the same error when it reads, by the query's read set, a model whose stored filter is invalid — whether as
its source or through any join, filter or definition. A model filter reading a joined model's windowed column
SHALL be accepted and SHALL restrict by that column's computed value, unless it closes a filter cycle
(Requirement: Filters whose dataset reaches itself are refused).

#### Scenario: Saving a filter on a measure is refused
- **WHEN** a model is saved with a filter `total > 100` where `total` is one of its saved measures
- **THEN** the save fails with the model-filter error naming the filter

#### Scenario: Saving a filter on a joined windowed column is refused
- **WHEN** a model is saved with a filter reading a joined model's windowed column whose definition reads
  the filtered model back (a filter cycle)
- **THEN** the save fails with the filter-cycle error, at both save doors

#### Scenario: A filter on a joined windowed column is accepted and evaluated
- **WHEN** `ranked_orders` reads table `orders(id, status, amount)` with the windowed column `ranked`
  (dialect entry `rank() over (order by amount)`), and `returns_ranked` joins it many-to-one and is saved
  with the filter `ranked_orders.ranked > 1`, through the engine and through the storage backend
- **THEN** both saves succeed, and a query counting `returns_ranked` rows counts exactly the returns whose
  order's rank over every order exceeds `1`

#### Scenario: A join target's invalid stored filter fails the query
- **WHEN** a join target's filter references one of its measures, stored without validation, and a query
  rooted elsewhere reads that target
- **THEN** the query fails with the model-filter error naming the target's filter
