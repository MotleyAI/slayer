# queries/computed-dimensions — delta

## MODIFIED Requirements

### Requirement: Grain self-containment error surface
Expressions that are not grain-self-contained SHALL fail with clear errors naming the offending construct: a bare aggregate without `partition_by=`, an aggregate over another attached aggregate value, and an aggregate whose partition keys or inputs are not attributable from its root. Two deliberate, permanent type rules govern transforms in dimension position — they are typed residue of the closure axiom, not deferrals, and their errors SHALL cite no tracking issue: a transform inside a dimension expression SHALL have at least one aggregate in its input (a transform acts on aggregates), and every aggregate contained in such a transform SHALL declare `partition_by=` explicitly — the ungrained default (the query's dimensions) would include the dimension being defined, a self-referential grain. The two violations SHALL raise distinct errors, each naming its own remedy.

#### Scenario: Bare aggregate in a dimension is rejected
- WHEN a dimension expression contains an aggregate with no `partition_by=`
- THEN the query fails with an error stating that aggregates in dimension expressions must declare `partition_by=`

#### Scenario: Aggregate over an attached value is rejected
- WHEN a dimension expression aggregates over a subexpression that itself contains a partitioned aggregate
- THEN the query fails with a clear not-yet-supported error, not an internal error

#### Scenario: Unattributable partition key in a dimension expression is rejected
- WHEN a dimension expression's aggregate declares a partition key reachable from its root only across a join with unproven arity
- THEN the query fails with a clear error naming the key and the remedy

#### Scenario: Transform without an aggregate input in a dimension is rejected
- **WHEN** a dimension expression contains a transform over a row-level input
  (for example `cumsum(amount)`)
- **THEN** the query fails with a typed error stating that a transform in a
  dimension expression must wrap an aggregate, with no issue reference in the
  message

#### Scenario: Transform over an ungrained aggregate in a dimension is rejected
- **WHEN** a dimension expression contains a transform wrapping an aggregate
  with no `partition_by=` (for example `cumsum(amount:sum)`)
- **THEN** the query fails with a typed error naming the explicit-`partition_by`
  remedy and the self-containment rationale, with no issue reference

#### Scenario: Mixed grained and ungrained aggregates under one transform are rejected
- **WHEN** a dimension-position transform's input combines one aggregate that
  declares `partition_by=` with one that does not
- **THEN** the query fails with the explicit-`partition_by` error naming the
  ungrained aggregate, never partially compiling
