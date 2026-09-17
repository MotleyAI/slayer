# models/aggregation-names Delta

## Purpose

Defines which names are reserved and rejected for custom model aggregations so that the functional call form `name(col)` stays unambiguous across aggregations, scalars, transforms, and time granularities.

## ADDED Requirements

### Requirement: Granularity names are reserved

A custom model aggregation SHALL be rejected at model validation when its name case-insensitively matches a `TimeGranularity` value, with an error naming the reserved granularity set — so `gran(col)` in a query can never be shadowed by a like-named aggregation.

#### Scenario: aggregation named month rejected

- WHEN a model defines a custom aggregation named `month` (in any case)
- THEN model validation fails with an error naming the reserved time-granularity names
