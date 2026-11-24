## ADDED Requirements

### Requirement: Raw-row dimensions are determined by the population
In raw-row mode (`distinct_dimension_values: false`) the population — named, inferred, pinned by a saved stage, or a
query stage — SHALL provably determine every dimension and time dimension: each row-level field the entry reads,
through the dependency closure of every derived field it names, SHALL be reached from the population over provably
to-one hops only. An entry reached across a fanning hop (a list child included) or an unproven hop (no declared cardinality and no covering unique key) SHALL fail with `DistinctDimensionValuesError` naming the entry and that hop, in every `to_many_handling` mode, never listing
a population row more than once. An entry whose dependencies cannot be analysed SHALL fail closed with
`UnanalyzableDependencyError`. An entry no dataset determines (Axiom 2.2) keeps its own error, raised before this
rule. A query omitting `population` SHALL still be refused at inference with `PopulationInferenceError` when no
dataset provably determines its dimensions. Filters are not entries: a filter crossing a fanning hop restricts the
population without multiplying it (Grain guarantee).

#### Scenario: A fanning dimension under a named population is refused
- **WHEN** a query with `population: "customers"`, `distinct_dimension_values: false` and `dimensions:
  ["orders.amount"]` runs on the Appendix B seed (`orders` many-to-one to `customers`, C1 with two orders), with
  `to_many_handling` omitted, `associate`, `error` or the deprecated `broadcast`
- **THEN** each fails with `DistinctDimensionValuesError` naming `orders.amount` and the hop `orders`, never returning
  five rows for four customers

#### Scenario: A computed dimension over a fanning field is refused
- **WHEN** the same raw-row query from `customers` selects the computed dimension `orders.amount * 2`, or
  `customers.region || orders.status`
- **THEN** it fails with `DistinctDimensionValuesError` naming the dimension and the hop `orders`

#### Scenario: A time dimension across a fanning hop is refused
- **WHEN** a raw-row query from `customers` selects the time dimension `orders.ordered_at` at month granularity
- **THEN** it fails with `DistinctDimensionValuesError` naming the time dimension and the hop `orders`

#### Scenario: A time dimension expression no dataset determines keeps its own error
- **WHEN** a raw-row query from `customers` selects a time dimension expression reading both `orders.ordered_at` and
  `returns.amount`
- **THEN** it fails with `UnsafeRelationshipInputError` (no dataset determines every field it reads), not with
  `DistinctDimensionValuesError`

#### Scenario: A list child is refused and the whole list is not
- **WHEN** a raw-row query from `baskets` (B1 holding `['a', 'b']`, B2 holding `['c']`) selects `baskets.items.value`,
  and another selects `baskets.items`
- **THEN** the first fails with `DistinctDimensionValuesError` naming `baskets.items.value` and the hop `items`; the
  second returns one row per basket (two rows)

#### Scenario: An unknown-cardinality hop under a named population is refused
- **WHEN** `orders` joins `customers` with no declared cardinality and no covering unique key, and a raw-row query
  from `orders` selects `customers.region`
- **THEN** it fails with `DistinctDimensionValuesError` naming `customers.region` and the hop `customers`

#### Scenario: A derived field crossing a fanning hop is refused through its closure
- **WHEN** `customers` declares the derived field `order_amount` defined as `orders.amount`, and a raw-row query
  from `customers` selects `customers.order_amount`
- **THEN** it fails with `DistinctDimensionValuesError` naming `customers.order_amount` and the hop `orders`

#### Scenario: A derived field local to the population is accepted
- **WHEN** `orders` declares the derived field `double_amount` defined as `amount * 2`, and a raw-row query from
  `orders` selects `orders.double_amount`
- **THEN** it returns one row per order (four rows)

#### Scenario: An unanalysable dependency fails closed
- **WHEN** a raw-row query from a population selects a derived field of it whose definition cannot be analysed for
  relationship hops
- **THEN** it fails with `UnanalyzableDependencyError`

#### Scenario: A stage population's dimension across a fanning inherited edge is refused
- **WHEN** a query list's stage `c` lists `customers.customer_id` and `customers.region` from `customers`, and the
  main raw-row query from population `c` selects `c.region` and `orders.amount`
- **THEN** it fails with `DistinctDimensionValuesError` naming `orders.amount` and the hop `orders`; selecting only
  `c.region` returns one row per stage row (four rows)

#### Scenario: Determined dimensions keep one row per population row
- **WHEN** a raw-row query from `orders` selects `orders.amount` and `customers.region`
- **THEN** it returns one row per order (four rows), each with its customer's region

#### Scenario: Grouped mode under the same population is unchanged
- **WHEN** the query from `customers` selecting `orders.amount` runs with distinct dimension values (the default)
- **THEN** it returns each present value once, NULL included for the customer with no order (five rows)

#### Scenario: An inferred raw-row query is still refused at inference
- **WHEN** a raw-row query omitting `population` selects `customers.region` with the computed dimension
  `customers.region || orders.status` over an unknown-cardinality `orders` → `customers` hop, or a time dimension or
  list child no dataset provably determines together with the other dimensions
- **THEN** it fails with `PopulationInferenceError` before binding
