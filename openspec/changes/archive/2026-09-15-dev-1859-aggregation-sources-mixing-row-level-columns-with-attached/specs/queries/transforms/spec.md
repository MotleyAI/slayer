# queries/transforms delta

## ADDED Requirements

### Requirement: Non-shift transforms reject grain-refining row-level leaves
A transform other than `time_shift`, `change`, and `change_pct`, used in
measure, filter, or order position, SHALL reject with a typed plan-time error —
before any SQL is generated — any row-level (non-aggregate) leaf in its input
that refines the consumer grain, that is, a leaf that is not itself a projected
query dimension. The error SHALL name the transform, the offending leaf kind,
and the remedy (aggregate the leaf, e.g. `cumsum(weight:sum)`), and cite no
tracking issue. The rule applies uniformly to every non-shift transform
operation, the rank family included. Leaves that are projected grain keys —
plain or computed dimensions — remain legal, evaluated at the query grain. The
raw source column of a bucketed time dimension is not a projected grain key
(it refines the bucket). `first`/`last` keep their aggregation dispatch, the
shift family keeps its bare-leaf regime and its existing composite row-leaf
rejection, and the stricter dimension-position rules are unchanged.

#### Scenario: Bare grain-refining leaf rejected
- **WHEN** a query with a month time dimension and no `weight` dimension selects
  the measure `cumsum(weight)`
- **THEN** it fails at plan time with the typed error naming the remedy — never
  SQL whose base grain is inflated to one row per (bucket, weight-value)

#### Scenario: Rank family is covered
- **WHEN** a query over `[store]` with a month time dimension selects the
  measure `rank(qty)`
- **THEN** it fails with the same typed error, never a result carrying one row
  per (store, month, qty-value)

#### Scenario: Predicate over an unprojected row column rejected
- **WHEN** a query selects the measure `consecutive_periods(weight > 0)` with
  `weight` not a query dimension
- **THEN** it fails with the same typed error naming the aggregate-the-leaf
  remedy

#### Scenario: A projected grain key stays legal
- **WHEN** a query projects `weight` as a dimension and selects the measure
  `rank(weight)`
- **THEN** it compiles at the query grain and executes with correct values —
  no error, no extra result rows

#### Scenario: Attached values do not launder a row leaf
- **WHEN** a query selects the measure
  `cumsum(weight * avg(unit_price, partition_by=product))` with `weight` not
  projected
- **THEN** it fails with the same typed error — a transform does not collapse
  row grain, unlike an aggregation

#### Scenario: Shift family keeps its bare-leaf regime
- **WHEN** a query selects `time_shift(weight, -1)` or `change(weight)` over a
  month time dimension
- **THEN** the established read-and-rebucket behavior is unchanged and the base
  grain is not inflated
