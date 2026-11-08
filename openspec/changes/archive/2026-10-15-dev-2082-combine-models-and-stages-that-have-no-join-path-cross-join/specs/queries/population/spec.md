## MODIFIED Requirements

### Requirement: Root recommendation follows the population rule

`recommend_root_model` SHALL classify each resolved item by entity type: columns
are determination items (determined, counting routed hops); saved measures and
aggregation-suffixed items are attachments requiring reachability only and reported
as such. Selection SHALL use the same rule as population inference so the two
surfaces cannot disagree whenever inference succeeds: a column item is determined
from a candidate exactly when the inference probe finds it viable from that
candidate. Only when no candidate is viable and an item's probe failed solely because
a hop or its short-form route is ambiguous — never fanning or unreachable — SHALL
recommendation resolve that item along its tie-break-best fan-out-free route and emit
that full canonical path, so the recommended root executes once named as
`source_model`. When the items include at least one determination item, an attachment
on a dataset that no join path connects to a candidate SHALL NOT count against that
candidate; once a root is chosen, each such attachment SHALL be reported with the
path a query rooted there spells it by (its product hop, `queries/unrelated-datasets`)
and flagged `unrelated`, and SHALL never steer the choice. Without determination items,
attachments steer selection by join-path reachability alone. Every other item
SHALL carry `unrelated` false. A feasible `root_hint` (determines all determination
items under either rule, reaches all attachments, by a join path or as unrelated
datasets when determination items exist) SHALL override selection; an infeasible one
falls back with a warning; a malformed one raises. The no-common-root coverage output
SHALL be retained with the per-item criterion updated to match. A pair whose
unaddressable edges keep an item from being reached SHALL be named in the response
warnings with the remedy to name its edges.

#### Scenario: Saved measure does not steer the recommendation

- **WHEN** items mix columns with a suffix-less saved measure
- **THEN** the saved measure is reported as an attachment and does not change the
  recommended root

#### Scenario: Recommendation matches inference

- **WHEN** the same dimension items are given to `recommend_root_model` and queried
  without `source_model`
- **THEN** the recommended root equals the inferred population

#### Scenario: Ambiguity is resolved by an explicit path

- **WHEN** `orders` reaches `regions` by two fan-out-free routes (via `customers` and
  via `warehouses`) and `recommend_root_model` is given `orders.status` and
  `regions.population`
- **THEN** the root is `orders` and the path for `regions.population` is
  `customers.regions.population`, while
  `{"dimensions": ["orders.status", "regions.population"]}` without `source_model`
  fails with the ambiguous-path error

#### Scenario: Recommendation and inference agree on a directed cycle

- **WHEN** `recommend_root_model` is given `x.v`, `y.v` and `z.v` on the directed
  cycle `x → y → z → x`
- **THEN** no root is recommended, matching inference's failure, and the coverage
  output reports which items each candidate determines

#### Scenario: An unaddressable pair is named in the warnings

- **WHEN** `orders` declares two unnamed joins to `addresses` and
  `recommend_root_model` is given `orders.id` and `addresses.city`
- **THEN** the response warnings name `orders`↔`addresses` and the remedy to name
  the edges

#### Scenario: An unrelated attachment is reported through the product hop

- **WHEN** `customers` and `events` share no join path and `recommend_root_model` is
  given `customers.city` and `count(events.id)`
- **THEN** the root is `customers`, `count(events.id)` is reported as an attachment
  flagged `unrelated` with the path `count(events.id)`, `customers.city` is flagged
  `unrelated` false, and querying that path rooted at `customers` succeeds

#### Scenario: An unrelated column stays unreachable

- **WHEN** `recommend_root_model` is given `customers.city` and `events.kind`
- **THEN** no root is recommended, exactly as before this change

#### Scenario: Attachments alone keep join-path selection

- **WHEN** `recommend_root_model` is given only `count(customers.id)` and
  `count(events.id)`
- **THEN** no root is recommended, exactly as before this change
