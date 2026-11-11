## MODIFIED Requirements

### Requirement: Non-routing resolution is unchanged
A directly-joined dotted path, a self-prefixed root reference, and a full explicit path SHALL resolve
exactly as before, with byte-identical result keys whenever the typed path is already canonical (every
unnamed edge, and every named edge spelled by its name). A fully valid path whose terminal column does not
exist SHALL keep failing as an unknown reference, not as a routing failure.

A reference whose leading qualifier is the query root's own name — a model's name, an extension's base
model, an inline model's name, a stage's name in its own query list, or a spliced query-backed model's name
— SHALL resolve as the rest of the reference from that root (`orders.amount` ≡ `amount` from root
`orders`), in every Mode-B position (dimensions, time dimensions, measures and aggregation arguments,
filters, order, `main_time_dimension`, computed-dimension and expression time-dimension expressions) and
with the same SQL, values and result keys as the unqualified spelling, except that an auto-named
computed dimension or expression time dimension keeps the written qualifier in its name. The root
qualifier SHALL be read before any join hop or query-level alias, so it wins over an edge spelled the same.

#### Scenario: Existing full and self-prefixed paths are unchanged
- **WHEN** a query uses a full explicit join path or a self-prefixed root column, spelled canonically
- **THEN** resolution and result keys are identical to prior behaviour

#### Scenario: A root-qualified reference equals the bare one in every position
- **WHEN** a query rooted at `orders` selects dimension `orders.status`, time dimension
  `date_trunc('month', orders.created_at)`, measure `sum(orders.amount)`, filter `orders.amount > 5` and order
  `orders.status`, and a second query spells all of them without the `orders.` qualifier
- **THEN** both produce the same SQL, the same rows and the same result keys
  (`orders.status`, `orders.created_at`, `orders.amount_sum`)

#### Scenario: An auto-name keeps the written qualifier
- **WHEN** a query rooted at `orders` selects the unnamed computed dimension `orders.amount * 2`
- **THEN** its result key is `orders.orders_amount_2`

#### Scenario: A stage root's own name qualifies its columns
- **WHEN** a query list's stage `s1` projects `status` and `sum(amount)` named `tot`, and the final query
  over `s1` selects the computed dimension `s1.tot * 2`, the measure `sum(s1.tot)` and `count(s1.*)`
- **THEN** each equals its unqualified spelling (`tot * 2`, `sum(tot)`, `count(*)`)

#### Scenario: Every root kind takes its own name as qualifier
- **WHEN** a query rooted at an extension of `orders`, at an inline model named `ad_hoc`, or at a
  query-backed model `qb` selects `orders.status`, `ad_hoc.status` or `qb.status` respectively
- **THEN** each equals its query selecting `status`

#### Scenario: The root wins over an edge named like it
- **WHEN** model `orders` declares a join to `customers` named `orders`, and a query rooted at `orders`
  selects `orders.status`
- **THEN** it reads `orders`' own `status`, and the joined `customers` column is reached as
  `orders.orders.<column>`

#### Scenario: A root-qualified filter matches a selected dotted measure
- **WHEN** a query rooted at `orders` selects the saved measure `customers.aov` and filters on
  `orders.customers.aov > 10`
- **THEN** the filter reads that selected measure, exactly as the filter `customers.aov > 10` does
