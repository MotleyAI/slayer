# models/join-cardinality Specification

## Purpose
Defines how join arity (cardinality) is declared, structurally proven from unique-key metadata, imported from external schemas, propagated onto query-backed models, and validated — the evidence the query engine consumes to decide fan-out safety.

## Requirements

### Requirement: Provable many-to-one arity
A join hop SHALL count as provably many-to-one iff (a) its target-side join columns cover a declared primary-key or unique set of the target model (structural proof), or (b) the join declares `cardinality` of `many_to_one` or `one_to_one`. Declarations are trusted at query time. An undeclared, unproven hop is unproven and MUST be treated as unsafe (fail-closed). Safety is evaluated over existing stored join edges only: a reverse edge participates only where one exists (mirrored INNER joins carry the inverted forward cardinality; a declared reverse join carries its own), and proving arity MUST never make an absent edge traversable.

#### Scenario: Structural proof from a covered primary key
- WHEN a join's target-side columns cover the target model's declared primary key
- THEN the hop is provably many-to-one with no cardinality declaration needed

#### Scenario: Unknown arity fails closed
- WHEN a join has no cardinality declaration and its target-side columns cover no declared unique set
- THEN the engine treats the hop as unsafe for value paths (metrics broadcast rather than join through it)

#### Scenario: No synthesized traversal
- WHEN a model A declares a join to B but B stores no join back to A
- THEN no reverse hop B→A is available to path resolution, whatever the forward join's cardinality

### Requirement: Composite keys prove arity only when fully covered
When a target's uniqueness is composite (several primary-key columns, or a multi-column unique set), a join SHALL be provably many-to-one only when its target-side columns cover the complete set; covering a strict subset proves nothing.

#### Scenario: Full composite coverage proves, partial does not
- WHEN a target model declares a two-column composite primary key
- THEN a join on both columns is provably many-to-one, and a join on only one of them is unproven

### Requirement: Query-backed models carry their provable uniqueness
A model created from a query SHALL declare its result grain's uniqueness on its columns when the backing query provably deduplicates that grain — it aggregates, or is a dimension-only query with distinct dimension values. A backing query that preserves duplicates MUST NOT have uniqueness stamped.

#### Scenario: Aggregated backing query proves N:1 joins onto it
- WHEN a model is created from an aggregating query and another model joins onto its complete grain columns
- THEN that join is provably many-to-one and cross-model metrics over it keep exact per-dimension values

#### Scenario: Duplicate-preserving backing query stamps nothing
- WHEN a model is created from a measure-less query with `distinct_dimension_values=false`
- THEN its columns carry no uniqueness claims and joins onto it remain unproven

### Requirement: Cube import maps relationship to cardinality
The Cube importer SHALL map the parsed join `relationship` onto `Join.cardinality`: `many_to_one`/`belongs_to` → many-to-one, `one_to_many`/`has_many` → one-to-many, `one_to_one`/`has_one` → one-to-one. An unrecognized relationship string SHALL leave cardinality unset and add a conversion-report warning — never coerce into safety evidence.

#### Scenario: Cube belongs_to becomes many_to_one
- WHEN a Cube schema declares a join with `relationship: belongs_to`
- THEN the imported model's join carries `cardinality: many_to_one`

#### Scenario: Unknown relationship string is not trusted
- WHEN a Cube join declares an unrecognized relationship value
- THEN the imported join's cardinality is unset and the conversion report warns about it

### Requirement: Validation surfaces unproven and contradicted joins
Model validation and import reports SHALL flag every join that is neither declared many-to-one/one-to-one nor structurally proven, stating that metrics crossing it will broadcast and naming the remedies (declare `cardinality`, declare a covering unique key, or run cardinality detection). Where a cardinality detection report exists, validation SHALL also flag declarations the detected data hard-contradicts.

#### Scenario: Unproven join is flagged with remedies
- WHEN model validation runs over a model whose join has no declaration and no structural proof
- THEN the report flags that join with the broadcast consequence and the remedies

#### Scenario: Contradicted declaration is flagged
- WHEN a join declares many-to-one but a detection report records observed duplicates on the target side
- THEN validation flags the contradiction
