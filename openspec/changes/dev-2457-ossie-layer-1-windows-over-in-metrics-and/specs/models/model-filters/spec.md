## MODIFIED Requirements

### Requirement: Filters whose dataset reaches itself are refused
A model's dataset SHALL depend on the dataset of every model its filters read — every model on every join
path in the filters' dependency closure, through the derived columns they name — and on the dataset of every
model owning a windowed column (`models/field-expressions` › A windowed field is a column of its dataset)
in that closure, a windowed column reading every row of its own model's dataset. A window function written
in a model's own `filters` is a windowed column of that model, so the model's dataset depends on itself. A model whose dataset
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

#### Scenario: A window written in a model's filters is refused
- **WHEN** model `orders_top` is saved with the filter `rank() over (order by amount) = 1`, through the engine
  and through the storage backend, and separately was stored without validation and is read by a query
- **THEN** each save fails with the filter-cycle error naming `orders_top → orders_top`, leaving the store
  unchanged, and the query fails with the same error
