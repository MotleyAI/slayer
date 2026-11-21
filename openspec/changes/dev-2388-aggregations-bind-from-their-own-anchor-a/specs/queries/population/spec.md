## MODIFIED Requirements

### Requirement: Population may be omitted

`SlayerQuery.population` SHALL be optional. A query naming it — as a stored model or stage name,
with or without `extensions` — is **anchored**: it SHALL return the rows it returned before the
row-supply rule, one per combination of dimension values among the population's row-filtered rows.
The population SHALL decide only those rows: each aggregate SHALL bind from its own anchor and be
computed at its home (`queries/semantics` › An aggregation binds from its own anchor), reading its
grain dimensions and filters from that home (`queries/references`), and SHALL meet the rows only on
grain values — so the population never needs a route to a home, and a restriction of the population
(its model filters, a restricting stage) narrows a home only where the home reads the population on
its own paths. The named population MAY be a bridge model owning none of the queried items. A query
omitting it is **inferred**: its rows SHALL be its row supply ("Rows of an
inferred query are the row supply"). `population_inferred: true` beside a named `population`
SHALL make the query inferred with that dataset as its dimension anchor; `population_inferred:
true` without `population` SHALL fail with a typed validation error.

#### Scenario: Explicit population unchanged

- **WHEN** a query names `population` explicitly
- **THEN** it returns the rows it returned before the row-supply rule, each value equals that
  aggregate's value at its home on the row's grain values, and no inference runs

#### Scenario: A named population never routes to a home

- **WHEN** `orders → customers → regions`, `regions → managers` and `orders → managers` are
  many-to-one, and a query with `population: "regions"` groups by `regions.name` and measures
  `orders.total_revenue`
- **THEN** it returns North 150 and South 300, each order reaching its region through `customers`,
  though `regions` reaches `orders` by two routes neither of which is to-one

#### Scenario: A restricted population narrows a home only on the home's own paths

- **WHEN** `customers` carries the model filter `segment = 'retail'`, and a query with
  `population: "customers"` measures `sum(orders.amount)`, once grouped by `orders.status` and once
  by `customers.region`, over the seed of "Rows of an inferred query are the row supply"
- **THEN** by `orders.status` the rows are completed 150 and open 500 — every order counts, its home
  reading no customer; by `customers.region` the rows are EAST 300, NORTH NULL and WEST 300 — the
  corporate customer's order reaches no region and lands in the NULL tuple, which is no row

#### Scenario: Omitted population infers

- **WHEN** a query omits `population`
- **THEN** its rows are its row supply and its dimension anchor is inferred from its dimensions

#### Scenario: A pinned anchor keeps the rows inferred

- **WHEN** a query has `population: "customers"`, `population_inferred: true`, the dimension
  `customers.region` and the metric `orders.total_revenue`, over the seed of "Rows of an inferred
  query are the row supply"
- **THEN** the rows are EAST 350 and WEST 300, exactly as the same query without `population`

#### Scenario: The inferred marker needs a population

- **WHEN** a query sets `population_inferred: true` without `population`
- **THEN** it fails with a typed validation error naming `population_inferred`

