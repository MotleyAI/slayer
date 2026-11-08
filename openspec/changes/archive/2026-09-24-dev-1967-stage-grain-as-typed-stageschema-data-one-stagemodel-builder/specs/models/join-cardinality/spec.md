## RENAMED Requirements

- FROM: `### Requirement: Query-backed models carry their provable uniqueness`
- TO: `### Requirement: Stage outputs carry their provable uniqueness`

## MODIFIED Requirements

### Requirement: Stage outputs carry their provable uniqueness
Every stage output that another model or stage reads as a model — a model created from a query, and a named sibling stage read by a later stage of the same query (as its source, through a `ModelExtension` over it, or as a join target) — SHALL declare its result grain's uniqueness when the stage provably deduplicates that grain: it aggregates, or is a dimension-only stage with distinct dimension values. The grain is the stage's dimension and time-dimension columns — including computed dimensions whose value derives from an aggregate or a transform — and never a column in measure position. A single-column grain SHALL be declared unique; a multi-column grain SHALL be declared as one composite key, provable only when fully covered. A stage that preserves duplicates, or has no dimensions, MUST NOT have uniqueness stamped.

#### Scenario: Aggregated backing query proves N:1 joins onto it
- WHEN a model is created from an aggregating query and another model joins onto its complete grain columns
- THEN that join is provably many-to-one and cross-model metrics over it keep exact per-dimension values

#### Scenario: Duplicate-preserving backing query stamps nothing
- WHEN a model is created from a measure-less query with `distinct_dimension_values=false`
- THEN its columns carry no uniqueness claims and joins onto it remain unproven

#### Scenario: A join onto a sibling stage's grain is proven
- WHEN a multi-stage query declares a stage `c` over `customers` with dimensions `["id"]` and measure `{"formula": "max(tier)", "name": "tr"}`, and a later stage over `orders` joins `c` on `customer_id → id` and reports `c.tr` with `sum(amount)`
- THEN the join is provably many-to-one: each tier's total is exact and no broadcast warning is raised

#### Scenario: A join covering part of a sibling's composite grain stays unproven
- WHEN a sibling stage has dimensions `["id", "tier"]` and a later stage joins it on `id` alone
- THEN the join is unproven and a metric across it broadcasts with a warning

#### Scenario: An extension over a sibling keeps the sibling's grain
- WHEN a stage's source is a `ModelExtension` over a sibling stage and adds columns or joins
- THEN a join onto the extended sibling's complete grain is still provably many-to-one

#### Scenario: An aggregate-valued computed dimension is part of the grain
- WHEN a model is created from a query with dimensions `["status", {"expression": "sum(amount, partition_by=city)", "name": "ct"}]`, and another model joins onto it on `status` alone
- THEN the join is unproven — a metric across it broadcasts with a warning and is never multiplied

#### Scenario: A measure is never part of the grain
- WHEN a query's dimensions are `["status", "city"]` and its measure is `{"formula": "sum(amount, partition_by=status)", "name": "st"}`
- THEN its grain is `status` and `city`, and `st` carries no uniqueness claim

#### Scenario: A single-column grain is unique, not an identifier
- WHEN a model is created from a query with dimensions `["tier"]` and measure `count(*)`
- THEN `tier` is declared unique (not a primary key), a join onto it on `tier` is provably many-to-one, and `max(tier)` over the model succeeds

#### Scenario: Composite grain members stay aggregatable
- WHEN a stage has dimensions `["id", "tier"]` and a later stage or query computes `max(tier)` over it
- THEN the aggregation succeeds
