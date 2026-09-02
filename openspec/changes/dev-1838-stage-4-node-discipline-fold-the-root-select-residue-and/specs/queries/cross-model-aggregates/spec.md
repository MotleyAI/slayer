# queries/cross-model-aggregates Delta

## MODIFIED Requirements

### Requirement: Cross-model aggregates compose in expressions and dimensions
Cross-model aggregates SHALL be legal wherever local aggregates are: in arithmetic and scalar-call composites (including mixed with local aggregates and with aggregates from different joined models in one expression), inside transforms, in dimension expressions, in filters, and in ORDER BY. A computed dimension whose expression columns are all attributable from a metric's root participates in that metric's grain; otherwise the metric broadcasts across it. Exception: the same cross-model PARTITIONED aggregate consumed by a computed dimension and selected as a measure requires its partition key among the query dimensions — the keyless-grain dual-role variant SHALL fail with a clear error (support tracked separately).

#### Scenario: Local and cross-model aggregates in one expression
- WHEN a query selects the measure `orders.revenue:sum / customers.spend:sum`
- THEN each cell's value is the ratio of the two correctly-computed aggregates, by executed values

#### Scenario: Cross-model aggregate source inside a computed dimension
- WHEN a query declares a dimension banding `customers.spend:sum(partition_by=<customer-level dimension>)`
- THEN rows group by the band with correct executed values and unchanged cardinality

#### Scenario: Computed dimension coexists with a cross-model measure
- WHEN a query combines an aggregation-derived dimension (banded, bare, or transform-root) with a cross-model measure
- THEN both are correct by executed values in one result, replacing the former fail-closed guard

#### Scenario: Keyless-grain dual-role partitioned aggregate is rejected
- WHEN the same cross-model partitioned aggregate is consumed by a computed dimension and selected as a measure while its partition key is not among the query dimensions
- THEN the query fails with a clear error instead of silently mis-joining or broadcasting

### Requirement: Unsafe aggregate inputs fail closed
An aggregate whose inputs — positional args, keyword args (including aggregation-parameter fragments), or measure-level column filter references — cross a join hop that is not provably many-to-one from the aggregate's root SHALL fail with a clear error in both modes, whatever the aggregate's root: target-rooted, host-rooted, and local aggregates alike. Multiplying a host-side operand through a fanning join is ambiguous and MUST never silently compute over multiplied rows. The rule applies per input role: a *filter reference* or *argument* crossing an unproven hop fails closed; a crossing *source* (a source expression column read through the join — a host-grain wrap's sort key, or a derived column reading a joined model's values) is defined over the join result and consumes the target's values per matched row, so it SHALL stay legal with its established values.

#### Scenario: Aggregate reading through an unproven join errors
- WHEN an aggregate's column filter references a column across a join with unproven arity from the aggregate's root
- THEN the query fails with an error naming the input and the join hop, and the remedy

#### Scenario: Local measure with a filter over a provably safe hop keeps exact values
- WHEN a local measure's column filter references a column reached over a provably many-to-one join
- THEN the query executes with values identical to the pre-unification behavior

#### Scenario: Local measure with a filter over an unproven hop errors instead of fanning
- WHEN a local measure's column filter references a column across an unproven or one-to-many hop
- THEN the query fails with an error naming the hop and the remedy — never the silently multiplied aggregate this shape previously produced

#### Scenario: Host-grain wrap over a to-many source stays legal
- WHEN a query orders by an aggregate of a joined column evaluated at host grain across a to-many join
- THEN the aggregate evaluates over the joined rows as before, with unchanged executed values
