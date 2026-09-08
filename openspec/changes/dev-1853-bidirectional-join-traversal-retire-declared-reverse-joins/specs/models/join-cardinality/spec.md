# models/join-cardinality Delta

## REMOVED Requirements

### Requirement: Provable many-to-one arity
**Reason**: Bidirectional traversal makes arity a per-orientation property of a
symmetric edge, retiring this requirement's stored-edges-only scoping and its
"No synthesized traversal" promise.
**Migration**: Replaced by "Provable to-one arity per orientation" below; traversal
itself is specified in `models/join-traversal`.

## ADDED Requirements

### Requirement: Provable to-one arity per orientation
A join hop SHALL count as provably many-to-one iff, on the hop's *traversal
orientation*, (a) the traversal-target-side join columns cover a declared primary-key
or unique set of the traversal-target model (structural proof), or (b) the oriented
cardinality — the declared label when traversed as declared, its inverse when
traversed in reverse — is `many_to_one` or `one_to_one`. Declarations are trusted at
query time. An undeclared, unproven orientation is unproven and MUST be treated as
unsafe (fail-closed). Both orientations of every declared edge participate in safety;
proof is per orientation, so one direction of an edge may be provably to-one while the
other fans out.

#### Scenario: Structural proof from a covered primary key
- WHEN a join's target-side columns cover the target model's declared primary key
- THEN the hop is provably many-to-one with no cardinality declaration needed

#### Scenario: Unknown arity fails closed
- WHEN a join has no cardinality declaration and its target-side columns cover no declared unique set
- THEN the engine treats the hop as unsafe for value paths (metrics broadcast rather than join through it)

#### Scenario: Inverted declared one-to-many proves many-to-one
- WHEN the only stored edge is `customers → orders (one_to_many)` and a hop traverses
  `orders → customers`
- THEN the oriented cardinality is `many_to_one` and dimensions across that hop keep
  exact attributed values instead of broadcasting

#### Scenario: Inverting a to-one hop yields fan-out
- WHEN a hop traverses `customers → orders` over a stored
  `orders → customers (many_to_one)` edge whose source columns cover no unique set of
  `orders`
- THEN that orientation is one-to-many, is not provably to-one, and metrics crossing
  it broadcast

## MODIFIED Requirements

### Requirement: Cube import maps relationship to cardinality
The Cube importer SHALL map the parsed join `relationship` onto `Join.cardinality`:
`many_to_one`/`belongs_to` → many-to-one, `one_to_many`/`has_many` → one-to-many,
`one_to_one`/`has_one` → one-to-one. An unrecognized relationship string SHALL leave
cardinality unset and add a conversion-report warning — never coerce into safety
evidence. When a Cube schema declares both directions of the same relationship, the
importer SHALL emit a single edge where the two declarations are exact inverses, and
both edges (surfacing as an ambiguous hop) where they contradict.

#### Scenario: Cube belongs_to becomes many_to_one
- WHEN a Cube schema declares a join with `relationship: belongs_to`
- THEN the imported model's join carries `cardinality: many_to_one`

#### Scenario: Unknown relationship string is not trusted
- WHEN a Cube join declares an unrecognized relationship value
- THEN the imported join's cardinality is unset and the conversion report warns about it

#### Scenario: Mutually-inverse Cube declarations import as one edge
- WHEN a Cube schema declares `orders belongs_to customers` and
  `customers has_many orders` over the same columns
- THEN the imported models carry exactly one edge between the pair, traversable both
  ways

### Requirement: Validation surfaces unproven and contradicted joins
Model validation and import reports SHALL flag every join neither declared
many-to-one/one-to-one nor structurally proven *in its declared orientation*, stating
that metrics crossing it will broadcast and naming the remedies (declare
`cardinality`, declare a covering unique key, or run cardinality detection). The
audit SHALL report each edge once with the provability of both orientations, so
diagnostics can explain why an inverted hop was classified safe or fanning. Where a
cardinality detection report exists, validation SHALL also flag declarations the
detected data hard-contradicts.

#### Scenario: Unproven join is flagged with remedies
- WHEN model validation runs over a model whose join has no declaration and no structural proof
- THEN the report flags that join with the broadcast consequence and the remedies

#### Scenario: Contradicted declaration is flagged
- WHEN a join declares many-to-one but a detection report records observed duplicates on the target side
- THEN validation flags the contradiction

#### Scenario: Audit explains both orientations
- WHEN the safety audit runs over a declared `many_to_one` edge
- THEN its finding records the declared orientation as provably to-one and the reverse
  orientation as one-to-many
