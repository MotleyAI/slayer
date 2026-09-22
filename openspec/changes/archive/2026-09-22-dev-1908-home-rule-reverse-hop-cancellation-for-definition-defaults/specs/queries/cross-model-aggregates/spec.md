## MODIFIED Requirements

### Requirement: Broadcast metadata
Whenever an aggregate's implicit grain loses a dimension to broadcasting, the response
SHALL carry a machine-readable warning naming the affected metric, each broadcast
dimension, and a per-dimension reason that reflects the actual path classification: a
fanning or unproven join hop when a path exists, or unreachable when no join path
resolves — a reachable-but-fanning dimension MUST NOT be reported as unreachable, and a
dimension on a prefix of the aggregate's own path is reachable back over that prefix,
never a round trip through the query root. Every
broadcast warning SHALL carry the dice–slice hint (per `queries/semantics` › Loud
degradation). One warning SHALL be emitted per distinct aggregate: identified by its
public measure name when directly selected, else by its canonical aggregate form and
role (expression, order, or filter). Hidden and filter-only aggregate uses emit
warnings too. Explicit `partition_by=` broadcasting is by design and MUST NOT warn.

#### Scenario: Broadcast is reported per metric and dimension
- **WHEN** a broadcast-mode query broadcasts `customers.spend:sum` over `orders.status`
- **THEN** the response warnings include an entry naming that measure, the `status`
  dimension, the reason, and the dice–slice hint, and a matching Python-level warning
  is emitted

#### Scenario: The same aggregate in several roles warns once
- **WHEN** one broadcast aggregate appears as a measure, in a filter, and in ORDER BY
- **THEN** exactly one broadcast warning is emitted for it

#### Scenario: Fanning dimension names the hop, not unreachability
- **WHEN** a broadcast dimension is reachable from the aggregate's root only across a
  fanning or unproven hop
- **THEN** the warning's reason names that hop classification — never "unreachable"

#### Scenario: A prefix-side dimension names the fanning hop
- **WHEN** a broadcast-mode query rooted at `orders` selects
  `customers.regions.countries.gdp:sum` by `customers.tier`, `regions → countries`
  provably to-one
- **THEN** the warning's reason names the fanning reverse hop to `regions` on the way
  back from `countries` to `customers` — never "unreachable", which the round trip
  through `orders` used to report
