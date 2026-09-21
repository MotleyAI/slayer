## MODIFIED Requirements

### Requirement: Filters restrict by association or fail loudly
A row-level filter conjunct that reaches a dataset's root only across non-determining
paths SHALL either restrict that dataset's population by association — the dataset
computes over exactly the root rows related to at least one surviving row combination,
each counted once — or be loudly excluded (dropped-filter warning, an error under
`to_many_handling: "error"`) per the pushdown-scope rules in
`queries/cross-model-aggregates`. This applies to the query population itself exactly as
to an aggregate's root: a conjunct reaching the population root only across a
non-determining path restricts the population by association, every aggregate evaluated
over the population rows — inline or in a producer rooted at the population — counts each
population row once, the filtered model is not joined into the base query for that
conjunct, and the restriction is reported through the semi-join informational entry with
no aggregate named — never an error, in any `to_many_handling` mode, whatever the query
selects. A conjunct whose non-determining paths a consumer's grain already materialises (a
projected dimension or time dimension on the same join branch) applies to that consumer on
the same related row instead, so filtering and grouping on one branch bind to one row. A
stated restriction SHALL never be silently ignored and SHALL never fan out an
aggregation's inputs. Whether a conjunct crosses a non-determining path is judged on its
dependency closure — a reference to a derived column whose definition crosses such a path
crosses it too; a conjunct whose closure cannot be analysed SHALL fail at plan time with a
typed error naming the filter and the column, in every mode, whether or not the query
aggregates. A conjunct outside pushdown scope — a root-local and a cross-path reference
mixed under `OR`/`NOT`, or cross-path references spanning several join branches — keeps
applying to the result rows, is excluded from every producer with the dropped-filter
warning (an error under `to_many_handling: "error"`), and SHALL fail at plan time with a
typed error naming the filter, the reason, and the remedy whenever it would multiply the
population — a plain aggregate is evaluated inline over the population rows, or raw-row
mode returns one row per population row — never a silently multiplied aggregate or
multiplied raw row (restricting such a conjunct by association is deferred to DEV-1935).

#### Scenario: Cross-path filter restricts the population by association
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects
  `customers.spend:sum`
- **THEN** by executed values the metric counts exactly the customers with at least
  one order passing the predicate, each once

#### Scenario: A restriction is never silently ignored
- **WHEN** a filter conjunct cannot be applied to an aggregate's population
- **THEN** the response carries the dropped-filter warning (or the query errors under
  `to_many_handling: "error"`) — never an unrestricted value presented as restricted

#### Scenario: Population filter across a fanning hop restricts the population
- **WHEN** a query rooted at `customers` filters on `orders.status = 'ok'` and selects the
  local `spend:sum`, in any `to_many_handling` mode, and one customer has two `ok` orders
- **THEN** by executed values the metric equals the spend of the distinct customers with at
  least one `ok` order, each once (420 on the reference dataset, never 520); the base query
  restricts `customers` by a correlated semi-join and does not join `orders`; the response
  carries a `semi_join_pushed` entry naming the filter with no aggregate; no Python-level
  warning is emitted

#### Scenario: Derived population filter across a fanning hop restricts the population
- **WHEN** a query rooted at `orders` filters on `customers.regions.bad_pop > 0` (a derived
  column whose definition crosses the one-to-many `regions → region_events` hop) and
  selects the local `amount:sum`, in any `to_many_handling` mode
- **THEN** by executed values the metric equals the amount of the orders whose region
  passes the predicate, each order once (120 on the reference dataset, never 220)

#### Scenario: Population filter over a provably to-one path stays inline
- **WHEN** a query rooted at `orders` filters on `customers.tier = 'gold'` and selects the
  local `amount:sum`
- **THEN** the query executes with the filter applied as a plain row restriction, by
  executed values and generated SQL unchanged from today

#### Scenario: Filter and dimension on one branch bind to the same row
- **WHEN** a query rooted at `customers` selects `dimensions: ["orders.status"]`, the
  local `spend:sum`, and `filters: ["orders.amount = 20"]`, where the only order of
  amount 20 has status `new` and its customer also has `ok` orders
- **THEN** the result has exactly one cell, `new`, by executed values — the filter and
  the dimension are evaluated on the same joined row, never on the customer's other orders

#### Scenario: Two branches restrict the population independently
- **WHEN** a query rooted at `customers` selects `spend:sum` with
  `filters: ["orders.status = 'ok'", "regions.region_events.value >= 50"]`, where
  `customers → orders` and `regions → region_events` are one-to-many
- **THEN** by executed values the metric counts exactly the customers with at least one
  `ok` order whose region has at least one event of value 50 or more, each once (280 on the
  reference dataset), and the response carries one `semi_join_pushed` entry per filter

#### Scenario: Unanalyzable filter dependency fails closed
- **WHEN** a query filters on `customers.regions.unparseable > 0`, a derived column no
  supported dialect can parse, with or without a measure, in any `to_many_handling` mode
- **THEN** the query fails at plan time with a typed error naming the filter and the column,
  containing no issue reference — never SQL routed as if the column crossed nothing

#### Scenario: Population filter across a fanning hop with an inline aggregate fails closed
- **WHEN** a query rooted at `customers` filters on
  `tier = 'bronze' or orders.status = 'ok'` (a root-local and a cross-path reference under
  `OR`, the cross-path leg reaching only across the fanning `orders` hop) and selects the
  local `spend:sum`, in any `to_many_handling` mode
- **THEN** the query fails at plan time with a typed error naming the filter, the reason
  (the `OR` mix), and the remedy (split the filter or restate it on one branch), containing
  no issue reference — never the join-multiplied total

#### Scenario: Out-of-scope conjunct in raw-row mode fails closed
- **WHEN** the same `tier = 'bronze' or orders.status = 'ok'` filter runs with
  `dimensions: ["tier"]`, `distinct_dimension_values: false`, and no measures, where a
  matching customer has several `ok` orders
- **THEN** the query fails at plan time with the same typed error rather than returning the
  join-multiplied rows — the raw-row grain guarantee is never silently broken by an
  out-of-scope conjunct

#### Scenario: Out-of-scope conjunct without an inline aggregate keeps applying
- **WHEN** the same filter runs with `dimensions: ["tier"]` and distinct dimension values
  (the default), or with only aggregates that compute in producers
- **THEN** the result rows are still restricted by the filter, by executed values (tiers
  `bronze`, `gold`, `silver` on the reference dataset), each producer drops the conjunct
  with the dropped-filter warning, and `to_many_handling: "error"` errors

#### Scenario: Dimension-only and producer-only queries are unaffected by the guard
- **WHEN** a query rooted at `customers` filters on `orders.status = 'ok'` and selects no
  measures, or selects only aggregates that compute in their own producers (cross-model,
  partitioned, or windowed)
- **THEN** the retired guard blocks nothing: the query executes with values unchanged from
  today, the filter applied to the population and to each producer by association

#### Scenario: Population filter across a fanning hop restricts a mixed re-aggregation constituent
- **WHEN** a customers query over `[tier]` selects
  `sum(spend * avg(spend:sum(partition_by=[tier, plan_code]), partition_by=tier))` with
  the filter `orders.status = 'ok'`
- **THEN** the constituent's operand cells and its per-tier average are computed over
  the restricted population only (gold 18050 = 190 × 95, silver 26450 = 230 × 115),
  never over rows multiplied by the fanning join; every producer relation carries the
  semi-join and none joins `orders`; the semi-join informational entries name the
  selected measure; and selecting the same re-aggregation standalone beside it shares
  one producer relation and reports under its own name

#### Scenario: Re-aggregation producers report a dropped out-of-scope conjunct
- **WHEN** a customers query over `[tier]` selects
  `min(spend:sum(partition_by=[tier, plan_code]), partition_by=tier)` with the filter
  `tier = 'bronze' or orders.status = 'ok'`, which no semi-join can preserve
- **THEN** the response carries the dropped-filter warning naming that conjunct exactly
  as it does for a plain partitioned producer — never a silent drop
