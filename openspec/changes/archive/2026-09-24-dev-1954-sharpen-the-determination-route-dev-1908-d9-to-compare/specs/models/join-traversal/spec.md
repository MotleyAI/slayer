## MODIFIED Requirements

### Requirement: Edge names disambiguate paths
A join MAY declare a `name`. A path segment matching an incident edge's name SHALL
traverse that edge from either endpoint, whether or not the pair is ambiguous, in
model SQL and query references alike. Result columns and metadata SHALL carry the
path's canonical spelling (the edge name for a named edge, whichever spelling was
typed), and downstream consumers (response metadata, time-dimension ownership,
ordering, filters, multi-stage scopes) SHALL resolve the traversal target from the
resolved edge, never by reading path tokens as model names.

#### Scenario: Named hop resolves an ambiguous pair
- **WHEN** the two `orders`↔`customers` edges are named `billing_customer` and
  `shipping_customer` and a query selects `orders.billing_customer.name`
- **THEN** it traverses the billing edge and the result key is
  `orders.billing_customer.name`

#### Scenario: Names are direction-agnostic
- **WHEN** a query rooted at `customers` references `billing_customer.amount:sum`
- **THEN** the same token traverses the same edge in reverse, reaching `orders`

#### Scenario: Named paths carry correct terminal metadata
- **WHEN** a named-edge path terminates in a time dimension used for ordering and
  response metadata
- **THEN** ownership, ordering, and metadata behave exactly as for the equivalent
  model-name path

#### Scenario: A named edge typed by its model name answers under the edge name
- **WHEN** `customers`→`regions` is a single edge named `hr` and a query rooted at
  `orders` selects the dimension `customers.regions.rname` and the measure
  `customers.regions.pop:max`
- **THEN** the result keys are `orders.customers.hr.rname` and
  `orders.customers.hr.pop_max`, with the same label, format and type metadata as
  the edge-name spelling

## ADDED Requirements

### Requirement: Paths resolve to one canonical spelling
Every resolved join path SHALL be spelled canonically: per hop, the traversed edge's
declared name, else its traversal-target model's name. Query references, saved
measures, auto-routed short forms, model SQL (`Column.sql`, `Column.filter`, model
filters) and definition defaults SHALL all resolve to that spelling, so two spellings
of one edge denote one value: one join, one slot, one home, and identical results.
Persisted models SHALL keep their authored spelling; error messages SHALL quote the
typed spelling.

#### Scenario: Mixed spellings of one edge join once
- **WHEN** a query rooted at `orders` filters on `customers.regions.rname` and selects
  the dimension `customers.hr.rname` (`hr` names the `customers`→`regions` edge)
- **THEN** the generated SQL joins `regions` exactly once and the filter and the
  dimension reference the same value

#### Scenario: An aggregate is grouped by a dimension spelled through the other name
- **WHEN** a query rooted at `orders` selects the dimension `customers.hr.rname` and
  the measure `customers.regions.pop:max`
- **THEN** the measure is computed per region exactly as for
  `customers.hr.pop:max`, and no broadcast warning is raised

#### Scenario: A reverse-hop route determines under a divergent spelling
- **WHEN** `regions`→`region_events` is one-to-many and a query rooted at `orders`
  selects the dimension `customers.regions.rname` and the measure
  `customers.hr.region_events.value:max`
- **THEN** the measure is computed per region and joined back exactly as when both
  are spelled `customers.hr`, and no broadcast warning is raised

#### Scenario: Two spellings of one dimension equal a duplicate
- **WHEN** a query selects both `customers.hr.rname` and `customers.regions.rname`
- **THEN** it behaves exactly as a query selecting `customers.hr.rname` twice

#### Scenario: Model SQL spelled by the model name meets the edge name
- **WHEN** a derived column on `orders` has `sql` `customers.regions.rname` and a
  query selects it together with the dimension `customers.hr.rname`
- **THEN** the generated SQL joins `regions` exactly once

#### Scenario: A definition default spelled by the model name resolves canonically
- **WHEN** a custom aggregation on `orders` has a parameter default
  `customers.regions.pop` and a query groups by `customers.hr.rname`
- **THEN** the default resolves to the same value and home as `customers.hr.pop`

#### Scenario: An auto-routed short form through a named edge is canonical
- **WHEN** a query rooted at `orders` selects the short form `regions.rname`, which
  routes uniquely through the `hr` edge
- **THEN** the result key is `orders.customers.hr.rname`

#### Scenario: Spelling twins share one cached result
- **WHEN** the result cache is enabled and the same query is executed once with
  `customers.regions.rname` and once with `customers.hr.rname`
- **THEN** the database is queried once, and both responses carry the same columns
  and metadata
