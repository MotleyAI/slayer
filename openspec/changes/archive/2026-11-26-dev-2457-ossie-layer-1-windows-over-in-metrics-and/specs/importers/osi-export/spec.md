## ADDED Requirements

### Requirement: Window functions export as written

A field whose portable `sql` holds a window function SHALL export under "Each expression form exports as itself". A measure whose expression is a window function over aggregates (`queries/window-functions`) SHALL export as a metric under "Metrics", its window written as `OSSIE_SQL_2026`. A windowed measure holding a SLayer-only operand — a transform, a ranked `first` / `last`, an aggregate with `partition_by=` or `window=` — and a field or measure whose exported text would hold a window function directly inside another SHALL travel only in the `SLAYER` extension and be reported as SLayer-only. An exported window SHALL be valid for upstream's expression validator.

#### Scenario: A windowed measure exports as a metric
- **WHEN** `orders` has the measure `revenue_rank: RANK() OVER (ORDER BY SUM(amount) DESC)` and the portable field `rn: ROW_NUMBER() OVER (PARTITION BY region ORDER BY ordered_at)`
- **THEN** dataset `orders` has the metric `revenue_rank` with `sql: RANK() OVER (ORDER BY SUM(amount) DESC)` and the field `rn` with that `sql`, and re-importing the document returns the same values for both

#### Scenario: A SLayer-only operand keeps the measure in the extension
- **WHEN** `orders` has the measure `r30: RANK() OVER (ORDER BY sum(amount, window='30d') DESC)`
- **THEN** no metric `r30` is exported, `orders`' extension carries it, and the report lists it as SLayer-only

#### Scenario: A window inside a window stays in the extension
- **WHEN** `orders` has the portable fields `rn: ROW_NUMBER() OVER (ORDER BY id)` and `top: MAX(orders.rn) OVER ()`
- **THEN** field `rn` is exported with its `sql`, field `top` is carried only in the extension, and the document validates against upstream's validator
