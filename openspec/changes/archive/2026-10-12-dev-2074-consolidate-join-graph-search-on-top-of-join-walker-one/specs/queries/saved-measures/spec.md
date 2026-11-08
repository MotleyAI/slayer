## ADDED Requirements

### Requirement: Saved measures compose across frames
A dotted saved measure's formula SHALL be bound in its declaring model's frame and then composed onto the
host's path. A reference in that formula that reaches a model already on the host-to-target chain SHALL
denote a separate occurrence of that model — never a revisit — and SHALL be evaluated, homed and attributed
exactly like any other cross-model aggregate. A formula that revisits a model within its own frame SHALL
still be refused with the circular error.

#### Scenario: A measure crossing back to the host is attributed per host cell
- **WHEN** `orders` joins `customers` by the many-to-one edge `customer`, `customers.order_total` is saved as
  `sum(customer.amount)` (the reverse traversal reaching the customer's orders), the data holds orders `11`
  (amount `10`) and `12` (amount `20`) of customer `ann` and order `21` (amount `5`) of customer `bob`, and an
  `orders`-rooted query selects the dimension `customer.name` and the measure `customer.order_total`
- **THEN** the rows are `ann → 30` and `bob → 5`, with no broadcast warning

#### Scenario: An unattributable host dimension broadcasts or associates
- **WHEN** the same measure is selected with the dimension `id` instead
- **THEN** in the default mode every row carries `35` with the broadcast warning, and in associate mode the
  rows are `11 → 30`, `12 → 30` and `21 → 5`

#### Scenario: A measure through a pointer back to the host composes
- **WHEN** `customers` also joins `orders` by the many-to-one edge `last_order` (ann's latest order is `12`,
  bob's is `21`), `customers.latest_total` is saved as `sum(last_order.amount)`, and the query selects the
  dimension `customer.name` in associate mode
- **THEN** the rows are `ann → 20` and `bob → 5`

#### Scenario: A formula revisiting within its own frame is refused
- **WHEN** `customers.bad` is saved as `count(last_order.customer.id)` and any query references it
- **THEN** the query fails with the circular error naming the hop `customer` and the revisited `customers`

## MODIFIED Requirements

### Requirement: Dotted cross-model resolution
A dotted reference whose join path resolves through the host's join graph and whose terminal segment matches a saved measure on the terminal model (`customers.aov` from an `orders`-rooted query) SHALL resolve by binding the target measure's formula in the target's frame and re-anchoring every reference into the host's coordinate system. Whenever the hand-written host-prefixed formula is itself legal, the result MUST be bound-tree-identical to it, so generated SQL, executed values, broadcast metadata, `strict` behavior, and warnings are all identical to that hand-written form. When the host-prefixed spelling would revisit a model within the query's own frame, it is not an equivalent: the dotted reference composes (see "Saved measures compose across frames") while the hand-written spelling stays refused as circular.

#### Scenario: Dotted measure equals hand-expanded formula
- WHEN an `orders`-rooted query selects `customers.aov` (saved on `customers` as `sum(spend) / count(*)`) and an otherwise-identical query selects `sum(customers.spend) / count(customers.*)`
- THEN both produce identical SQL and identical executed values on SQLite and DuckDB

#### Scenario: Broadcast and strict semantics are inherited
- WHEN a dotted saved-measure reference expands to aggregates that broadcast over an unattributable dimension
- THEN the response carries the same broadcast warnings as the hand-expanded form, and with `strict=true` the query fails with the same error

#### Scenario: Composite, partitioned, and transform forms re-anchor
- WHEN the target's saved formula contains arithmetic, a `partition_by=` over target-local dimensions (including `[]`), or a transform such as `cumsum`, and the host query references it dotted
- THEN SQL and executed values match the hand-expanded equivalent, with transform time ordering taken from the host query's active time dimension

#### Scenario: Dotted measure inside transforms and arithmetic at the host
- WHEN a host measure formula wraps the dotted reference (`cumsum(customers.aov)`) or mixes it with local terms (`rev_total / customers.aov`)
- THEN the query executes with values matching the hand-expanded equivalent

#### Scenario: Dotted measure as a computed-dimension source
- WHEN a computed-dimension expression references a dotted saved measure in a form legal for aggregation-derived dimensions
- THEN the dimension behaves exactly as with the hand-expanded formula

#### Scenario: A composing measure has no hand-written equivalent
- WHEN `customers.latest_total` is saved as `sum(last_order.amount)` and an `orders`-rooted query selects `customer.latest_total`, while another selects `sum(customer.last_order.amount)`
- THEN the first executes and the second fails with the circular error

## REMOVED Requirements

### Requirement: Round-trip expansions are rejected
**Reason**: Revisits are judged per frame (semantics Axiom 1). A saved measure's formula is a definition bound in its declaring model's frame, so a reference reaching a model already on the host chain is a separate occurrence, not a revisit; it composes with correct per-cell values instead of failing.
**Migration**: None required — references that failed now return values. A query that means the host row's own value references the host column directly.
