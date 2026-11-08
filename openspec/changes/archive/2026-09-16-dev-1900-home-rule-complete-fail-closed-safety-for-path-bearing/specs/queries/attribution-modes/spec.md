## MODIFIED Requirements

### Requirement: Query-level mode selection
A query SHALL accept `to_many_handling` with values `"broadcast"`, `"associate"`, and
`"error"`, defaulting to `"broadcast"`. The mode governs, uniformly for cross-model and
local aggregates alike and in every consumer context (measure, composite leaf, filter,
ORDER BY, computed dimension, nested producer), how each pair of (aggregate, query
dimension unattributable from the aggregate's root) resolves. A dimension is attributable
from a root iff every path in its dependency closure — its own join path and every path
the definition of a derived column it names crosses, recursively — is provably
many-to-one from that root; a dimension naming a derived column whose definition crosses
a fanning hop is unattributable exactly as a structural fanning dimension is. The mode
MUST NOT affect aggregates all of whose grain dimensions are attributable, dimension-free
totals, or filter pushdown (semi-join pushdown applies in every mode). An unrecognized
value SHALL fail with a clear validation error.

#### Scenario: Default is broadcast and byte-identical for cross-model shapes
- **WHEN** a query omitting `to_many_handling` broadcasts a cross-model metric over an
  unattributable dimension
- **THEN** generated SQL and executed values are identical to the pre-change broadcast
  behavior, and the response carries the broadcast warning

#### Scenario: Fully attributable queries are mode-invariant
- **WHEN** the same query whose metrics are all computable at the full query grain runs
  once per mode value
- **THEN** all three runs return identical executed values with no mode-related
  warnings or errors

#### Scenario: Unrecognized mode value fails
- **WHEN** a query sets `to_many_handling` to a value outside the three modes
- **THEN** the query fails with a clear validation error naming the accepted values

#### Scenario: Derived fanning dimension broadcasts with a warning by default
- **WHEN** a query rooted at `orders` selects the local `amount:sum` and the cross-model
  `customers.spend:sum` by the dimension `customers.regions.bad_pop`, a derived column
  defined as `pop + region_events.value` over the one-to-many `regions → region_events`
  hop, omitting `to_many_handling`
- **THEN** each metric is broadcast across that dimension with a warning naming the hop,
  by executed values — never the figure multiplied once per region event

#### Scenario: Derived fanning dimension associates per cell
- **WHEN** the same query runs with `to_many_handling: "associate"`
- **THEN** each cell aggregates over the distinct root entities associated with that
  dimension value — each order once for the local metric, each customer once for the
  cross-model one — by hand-computed executed values on SQLite and DuckDB

#### Scenario: Derived fanning dimension refuses under error mode
- **WHEN** the same query runs with `to_many_handling: "error"`
- **THEN** the query fails with the error-mode broadcast refusal naming the metric and the
  dimension
