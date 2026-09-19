## MODIFIED Requirements

### Requirement: Association eligibility and input handling
Associate-mode resolution SHALL support the full plain scalar aggregation family
(including count, count_distinct, avg, min, max, median, percentile, and stddev-class
aggregations), subject to each dialect's existing aggregate capability — grouped
`median`/`percentile` remains a `NotImplementedError` on T-SQL and MySQL, unchanged by
mode (per the divergence ledger). Every input expression of the aggregation — arguments and
aggregation-parameter fragments alike — is evaluated per associated entity (constant
per entity under the established unsafe-aggregate-inputs rule, which keeps applying
unchanged); a column-reference or aggregate-valued parameter — explicit, positional, or
supplied by the aggregation definition's default — is legal exactly when the entity grain
determines it (per `queries/semantics` › Aggregation parameters are typed by the home
dataset's grain) and is then picked once per associated entity alongside the aggregate's
own value; `*:count` counts the distinct associated entities per cell. An
aggregation's own column filter restricts the associated entities before per-cell
aggregation. Association is needed only when at least one grain dimension is
unattributable from the aggregate's home: an aggregate whose grain dimensions the home
all determines takes the plain path under `associate` exactly as under `broadcast`,
its attached inputs compiled at their own homes, so the eligibility rules below apply
only when association is needed. Combining associate-mode resolution with `window=` or
`first`/`last` on the same aggregate SHALL fail with a clear typed error naming the
combination and the remedy. An aggregate root model without a declared unique key SHALL
fail associate-mode resolution with a clear typed error naming the model and the remedy
(declare a primary or unique key).

#### Scenario: Percentile attributes over the association
- **WHEN** an associate-mode query slices a cross-model percentile aggregate by an
  unattributable dimension, on a dialect that supports grouped percentile
- **THEN** each cell's value is the percentile over the distinct associated entities'
  values, by executed values; on a dialect without grouped percentile (T-SQL, MySQL)
  the query raises the established `NotImplementedError`, unchanged by mode

#### Scenario: Star-count counts distinct associated entities
- **WHEN** an associate-mode query rooted at `orders` selects `customers.*:count` by an
  orders-level dimension
- **THEN** each cell counts the distinct customers associated with it, by executed
  values

#### Scenario: Measure-local filter restricts the association
- **WHEN** an associate-mode aggregate carries its own column filter
- **THEN** each cell aggregates only the associated entities passing the filter, and
  result cardinality is unchanged

#### Scenario: Weighted association by executed values
- **WHEN** an associate-mode query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=customers.spend)`, the custom `customers.spend:wsum`
  whose `weight` defaults to `spend`, and `customers.spend:weighted_avg(weight=customers.regions.pop)`
  by the orders-level dimension `status`
- **THEN** each executes with hand-computed per-cell values over the distinct associated
  customers on SQLite and DuckDB — the explicit and defaulted spellings identical, a
  customer with two orders in one cell weighted once — with unchanged result grain and
  sibling values

#### Scenario: Windowed or first/last combination fails closed
- **WHEN** an associate-mode query needs association for an aggregate that also
  declares `window=` or uses `first`/`last`
- **THEN** the query fails with a clear typed error naming the unsupported combination
  — never a silently wrong value

#### Scenario: Root without a unique key fails closed
- **WHEN** an associate-mode query needs association for an aggregate whose root model
  declares no primary or unique key
- **THEN** the query fails with a clear typed error naming the model and the remedy

#### Scenario: Attributable dimensions need no association
- **WHEN** an associate-mode query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  by `customers.tier` against a `customers` model declaring no primary or unique key
- **THEN** the query executes with the same values as under `broadcast` and no
  association warning — the home determines every dimension, so no entity
  deduplication is needed and the unique-key rule does not apply
