# models/join-cardinality Delta

## ADDED Requirements

### Requirement: Determination through to-one chains
A model SHALL determine a joined column iff a chain of provably many-to-one hops (per
the provable-arity rules of this spec) leads from the model to that column along
stored join edges. What a model determines has exactly one value per model row and
behaves as the model's own field; determination is the evidence attribution and
filter-inlining decisions consume (`queries/semantics`). References name their join
path explicitly as dotted paths, so a reference never leaves the chain ambiguous.

#### Scenario: A chain of proven hops determines
- **WHEN** `orders` joins `customers` and `customers` joins `regions`, both hops
  provably many-to-one
- **THEN** `orders` determines `customers.regions.name` and metrics attribute exact
  per-region values from an orders-rooted query

#### Scenario: One unproven hop breaks determination
- **WHEN** the first hop of a two-hop chain is provably many-to-one but the second is
  undeclared and structurally unproven
- **THEN** the chain's terminal columns are not determined and metrics broadcast
  across them instead of joining through the unproven hop
