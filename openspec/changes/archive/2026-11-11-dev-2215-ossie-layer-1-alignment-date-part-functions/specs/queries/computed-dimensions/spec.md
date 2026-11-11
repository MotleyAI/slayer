## MODIFIED Requirements

### Requirement: Filters compose across attach and transform phases
When a query carries an aggregation-derived dimension, top-level AND conjuncts of a single filter string SHALL split and route independently, each to its own phase: a predicate over the computed dimension applies at its established placement, a predicate over a transformed value applies to the final result, and a plain row predicate copies into the producer and consumer. A single predicate whose operands share no scope (e.g. joined by OR across phases) SHALL fail with the established split-the-filter error.

#### Scenario: Filter on a shifted value with a banded dimension
- WHEN a query with a banded dimension filters on `change(sum(amount)) > 0`
- THEN only groups whose value grew survive, band values are unchanged, and no internal placeholder appears in the emitted SQL

#### Scenario: Conjunction splits across phases
- WHEN one filter string ANDs a predicate on the banded dimension with a predicate on a shifted value
- THEN each conjunct routes to its own phase and the result equals applying the two filters separately

#### Scenario: Conjunction with a row-level predicate splits too
- WHEN one filter string ANDs a predicate on the banded dimension with a plain row predicate (`band = 1 and status = 'ok'`)
- THEN the row conjunct copies into the producer and consumer, the band conjunct applies at the final stage, and the result equals applying the two filters separately

#### Scenario: Unsplittable mixed predicate still fails closed
- WHEN one filter string ORs a predicate on the banded dimension with a predicate from another phase
- THEN the query fails with the established split-the-filter directive
