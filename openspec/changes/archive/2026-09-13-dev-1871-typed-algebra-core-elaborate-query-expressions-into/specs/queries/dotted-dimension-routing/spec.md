# queries/dotted-dimension-routing Delta

## MODIFIED Requirements

### Requirement: Uniform application across query surfaces
Short-form routing SHALL apply identically wherever a Mode-B dotted reference appears: dimensions,
time dimensions, cross-model measures and aggregations, star aggregations, query filters, and ORDER BY.
A short-form ORDER BY entry MUST bind to the same routed value as the matching selected dimension.
Rootless population inference SHALL be a routing surface too: viability probing applies the same
short-form route enumeration per candidate root, with two inference-specific constraints — literal
resolution takes precedence over short-form reinterpretation, and a route only supports determination
when every oriented hop is provably to-one.

#### Scenario: Routing applies to a cross-model aggregation and star aggregation
- WHEN a query selects the measure `Consumer.amount:sum` and `Consumer.*:count` with `Consumer` uniquely routable
- THEN both resolve through the routed path and emit correct SQL

#### Scenario: Short-form ORDER BY matches its routed dimension
- WHEN a query selects the dimension `Consumer.name` and orders by `Consumer.name`
- THEN the order clause references the same column as the selected dimension

#### Scenario: Inference and binding agree on a routed short form
- WHEN a query without `source_model` uses a short-form dimension that inference resolved via a route
  from the chosen population
- THEN binding resolves the same short form through the same route, so the executed query matches the
  inference decision
