## MODIFIED Requirements

### Requirement: Expression source typing
The system SHALL type an aggregation's expression source by its leaves and
constituents alone, never by its spelling. A `first`/`last` aggregation over an
expression source SHALL be rejected with the existing not-supported-over-an-expression
error. A numeric-only aggregation over a confidently non-numeric expression SHALL be
rejected at binding. An expression whose leaves no candidate home determines over
provably to-one hops SHALL fail with the existing input-safety error naming the leaf
and the fanning or unproven hop, and a leaf whose derived definition cannot be
analysed SHALL fail with the existing analyzability error naming the column — never
a multiplied or silently wrong value. A column carrying a column-level filter SHALL be
an ordinary derived operand (per `models/column-filters`). A transform nested in the
source SHALL be an attached constituent (per `queries/partitioned-aggregates` ›
Re-aggregation consumes attached operands as datasets); a row-level leaf inside it
that is not a projected grain key SHALL be rejected per `queries/transforms` ›
Non-shift transforms reject grain-refining row-level leaves. An aggregation source
consisting entirely of attached values is a re-aggregation and SHALL be accepted; a
source mixing row-level references with attached values is a row-grain aggregation
and SHALL be accepted (per `queries/semantics` › Row-grain aggregation sources), except
that a collapsing (`first`/`last`) transform constituent mixed with a row-level
reference SHALL be rejected with a typed error naming the shape (its broadcast onto
row-level operands is deferred to DEV-1928); an
attached (aggregate-valued) parameter on a row-level source SHALL be accepted when the
aggregation's operating grain determines it (per `queries/partitioned-aggregates` ›
Attached parameters on row-level sources). An attached input — a source constituent
or a parameter — is opaque to the enclosing aggregation: it is compiled at its own
home in every `to_many_handling` mode and attached onto the aggregation's home rows
by its grain; the mode governs only the aggregation's own unattributable dimensions,
so no mode restricts which columns an attached input may read. Whether an aggregation
runs over rows or over an operand dataset's cells is decided by its source alone;
every attached input, in the source or in a parameter, is then attached by one
mechanism — into the input relation for a row-level source, as a constituent of the
operand dataset for an attached one.

#### Scenario: Filtered-column operand accepted
- **WHEN** a measure is written `sum(q_amount - 1)` where `q_amount` is `amount`
  carrying the column filter `product = 'Q'`
- **THEN** it executes as the sum over Q rows of `amount - 1` — the operand is
  `CASE WHEN product = 'Q' THEN amount END` — by executed values, never the former
  filtered-operand rejection

#### Scenario: Grained transform in the source accepted
- **WHEN** a query over a month time dimension selects
  `sum(cumsum(amount:sum(partition_by=[region, ordered_at])) - 1)`
- **THEN** each month carries the sum over regions of that region's running total
  minus one per cell, by hand-computed values on SQLite and DuckDB, distinguishable
  from the ungrained identity — never the former nested-transform rejection

#### Scenario: Row leaf under a nested transform rejected
- **WHEN** a measure is written `sum(cumsum(weight) - 1)` with `weight` not a query
  dimension
- **THEN** it fails at plan time with the typed row-leaf error naming the transform
  and the aggregate-the-leaf remedy, citing no tracking issue

#### Scenario: Fanning leaf fails closed
- **WHEN** an expression leaf is reachable from every candidate home only across a
  fanning or unproven join hop
- **THEN** the query fails with the input-safety error naming the leaf and the hop,
  never a multiplied value

#### Scenario: Collapsing constituent mixed with a row leaf fails closed
- **WHEN** a query over a month time dimension selects
  `sum(amount * last(amount:sum(partition_by=[region, ordered_at])))`
- **THEN** it fails with a typed error naming the collapsing transform and the
  row-level column, never a broadcast or multiplied value

#### Scenario: Unanalysable derived leaf fails closed
- **WHEN** an expression leaf names a derived column whose definition no dialect can
  parse
- **THEN** the query fails with the analyzability error naming the column

#### Scenario: Ranked aggregation over an expression keeps its error
- **WHEN** a measure is written `first(amount - customers.discount)`
- **THEN** it fails with the existing error that `first` is not supported over an
  expression — never a cross-model error and never wrong values

#### Scenario: Fully attached source accepted
- **WHEN** a measure is written `avg(sum(amount, partition_by=[city, region]))`
- **THEN** it is accepted and compiles as a re-aggregation, not rejected by the
  expression gate

#### Scenario: Mixed row and attached source accepted
- **WHEN** a measure is written
  `sum(quantity * avg(unit_price, partition_by=product))`
- **THEN** it is accepted and compiles at row grain — the attached value
  broadcast per base row — not rejected by the expression gate

#### Scenario: Attached parameter on a row-level source accepted
- **WHEN** a measure is written
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  rooted at `orders` under `to_many_handling: "associate"`
- **THEN** it is accepted and compiles with the parameter's value attached into
  the aggregation's input relation — never the attached-parameter rejection —
  and the same aggregation with a row-level parameter is unaffected

#### Scenario: Attached parameter on a row-level source executes under broadcast
- **WHEN** a measure is written
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  rooted at `orders` under the default `broadcast` `to_many_handling`
- **THEN** it executes — the parameter's producer rooted at `orders`, attached per
  customer row inside the `customers`-rooted producer — with values identical to
  `associate` and `error` on every dimension the home determines, never the former
  typed rejection naming the `associate` remedy

#### Scenario: Attached parameter on a row-level source rejected
- **WHEN** a measure is written
  `customers.spend:weighted_avg(weight=sum(customers.spend, partition_by=status))`
  rooted at `orders`, in any `to_many_handling` mode — the parameter's own home is
  `customers` and `status` fans from it
- **THEN** it fails with the mode-invariant partition-key error naming `status`
  (per `queries/attribution-modes` › Partition key fanning from its host fails closed
  in every mode); what is rejected is the parameter's own ill-typed grain, never the
  mode or the columns the parameter reads

#### Scenario: Mixed-source constituent homed toward the host executes in every mode
- **WHEN** a query rooted at `orders` selects
  `sum(customers.spend * sum(amount, partition_by=customers.regions.name))` — a
  row-grain source homed at `customers` whose constituent is homed at `orders`
- **THEN** the constituent's producer is rooted at `orders` and attached per
  customer row; by `status` under the default mode the customers-rooted total
  (33780 on the reference dataset) is broadcast to both cells with the usual
  warning, and by `customers.tier` every mode returns identical hand-computed
  values (gold 15300, silver 16600, bronze 1880) — never the input-safety error the
  constituent's interior would raise from `customers`
