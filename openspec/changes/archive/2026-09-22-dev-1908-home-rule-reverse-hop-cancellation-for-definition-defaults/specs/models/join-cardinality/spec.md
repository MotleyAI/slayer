## MODIFIED Requirements

### Requirement: Determination through to-one chains
A model SHALL determine a joined column iff a chain of provably many-to-one hops (per
the per-orientation provable-arity rules of this spec) leads from the model to that
column along stored join edges, traversed in either orientation. Between a model on
one join path from the query root and a column on another, the chain SHALL be the one
that steps back from the model only as far as the two paths' longest common prefix and
forward from there — never a round trip through the root — so a model reached over a
provably to-one reverse hop from a dataset on its own path determines that dataset's
columns, and a model whose reverse suffix fans does not. What a model determines has
exactly one value per model row and behaves as the model's own field;
determination is the evidence attribution and filter-inlining decisions consume
(`queries/semantics`). References name their join path explicitly as dotted paths —
with an edge-name segment where parallel edges would otherwise make a hop ambiguous —
so a reference never leaves the chain ambiguous.

#### Scenario: A chain of proven hops determines
- **WHEN** `orders` joins `customers` and `customers` joins `regions`, both hops
  provably many-to-one
- **THEN** `orders` determines `customers.regions.name` and metrics attribute exact
  per-region values from an orders-rooted query

#### Scenario: An inverted declared one-to-many hop determines
- **WHEN** the only stored edge is `customers → orders (one_to_many)`
- **THEN** `orders` determines the customers columns over the inverted (oriented
  `many_to_one`) hop, with no reverse declaration

#### Scenario: One unproven hop breaks determination
- **WHEN** the first hop of a two-hop chain is provably many-to-one but the second is
  undeclared and structurally unproven
- **THEN** the chain's terminal columns are not determined and metrics broadcast
  across them instead of joining through the unproven hop

#### Scenario: A to-one reverse suffix determines
- **WHEN** `regions → region_events` fans and its inverse is provably to-one, and a
  query rooted at `orders` selects
  `sum(customers.regions.region_events.value * customers.regions.pop)` or the explicit
  `customers.regions.region_events.value:wsum_rp(weight=customers.regions.pop)`
- **THEN** `region_events` determines `customers.regions.pop` over the one reverse hop
  back to `regions`, the aggregation homes at `customers.regions.region_events`, and
  each event is counted once (16000 on the reference dataset), never refused as a
  revisiting round trip through `orders`

#### Scenario: A fanning reverse suffix does not determine
- **WHEN** `regions → countries` is provably to-one and a query rooted at `orders`
  needs `customers.regions.pop` from a `countries`-rooted aggregation
- **THEN** `countries` does not determine it — the reverse hop to `regions` fans — and
  the home falls to `customers.regions`, never to `countries`

#### Scenario: Paths sharing only the root keep the route through the root
- **WHEN** a query rooted at `orders` needs `stores.rent` from a `customers`-rooted
  aggregation, `orders → customers` and `orders → stores` sharing no hop
- **THEN** the route is back through `orders` and then to `stores`, judged exactly as
  before this change — the reverse `customers → orders` hop fans, so `customers` does
  not determine `stores.rent`
