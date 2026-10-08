## MODIFIED Requirements

### Requirement: Expression result keys are deterministic
The result-column key for an expression aggregation SHALL be derived by the
same auto-naming rule used for computed dimensions (non-word characters
collapsed to underscores, digit-leading names prefixed, long names capped with
a stable hash), followed by the aggregation name and any existing parametric or
partition suffixes — insensitive to whitespace and formatting variants, a dotted
leaf spelling with its dots collapsed like any other non-word character. The
derived segment SHALL be the formula text of the expression as written, for every
operand kind — columns, literals, scalar calls, arithmetic, comparisons (date-literal
comparisons included, whatever their operator, point or operand bucketing), boolean
connectives, `IN` predicates, nested aggregates and nested transforms — and SHALL
never contain an internal representation; it is identical across runs and
independent of the evaluation clock. A decimal literal SHALL be spelled as a plain
decimal and a null literal as `null`. A row-level column operand SHALL be spelled
relative to the expression's home path, which the key already carries as its
prefix; references inside a nested aggregate or transform keep their own spelling.
A date-literal comparison as the whole source SHALL be named like any other
expression source. An explicit rename overrides the derived key. Two distinct
expressions whose derived keys collide SHALL fail with a clear duplicate-key error
advising a rename — never silently share a column. A derived key SHALL be
referenceable in filters, in order and from a downstream stage.

#### Scenario: Derived key
- **WHEN** a measure on model `orders` is written `sum(amount - cost)`
- **THEN** its result key is `orders.amount_cost_sum` (same sanitizer as a computed dimension named from `amount - cost`)

#### Scenario: Dotted leaf in the derived key
- **WHEN** a measure on model `orders` is written `sum(amount - customers.discount)`
- **THEN** its result key is `orders.amount_customers_discount_sum`, and `name`
  overrides it as for any measure

#### Scenario: Colliding derived keys fail loudly
- **WHEN** one query contains both `sum(amount - cost)` and `sum(amount + cost)`
  without renames, or both `sum(iif(ordered_at == '2024-02', 1, 0))` and
  `sum(iif(ordered_at != '2024-02', 1, 0))`
- **THEN** it fails with a duplicate-key error naming both expressions and advising a rename

#### Scenario: Formatting-insensitive identity
- **WHEN** the same expression is written `sum(amount-cost)` and `sum( amount - cost )`
- **THEN** both produce the identical result key

#### Scenario: Long expression capped
- **WHEN** the sanitized expression segment exceeds the length cap
- **THEN** the key uses a truncated prefix plus a short stable hash, deterministic across runs

#### Scenario: Rename override
- **WHEN** a measure is declared `{"formula": "sum(amount - cost)", "name": "profit"}`
- **THEN** the result key uses `profit`

#### Scenario: Date comparison inside a scalar call
- **WHEN** a query rooted at `regions` (`customers → regions`, `orders → customers`)
  selects `sum(iif(customers.orders.order_date >= '2025-01-01', 1, 0))`
- **THEN** its result key is `regions.customers.orders.iif_order_date_2025_01_01_1_0_sum`,
  identical across runs, with the values unchanged

#### Scenario: Date comparison forms
- **WHEN** a measure on `orders` is written with a date comparison inside `iif` or
  joined by `and` — `iif(ordered_at >= 'last month', 1, 0)`,
  `iif(ordered_at == '2024-02', 1, 0)`, `iif(ordered_at != '2024-02', 1, 0)`,
  `iif(month(ordered_at) >= '2024-02', 1, 0)`,
  `amount > 5 and ordered_at >= '2024-02-01'` — each summed
- **THEN** the keys are `orders.iif_ordered_at_last_month_1_0_sum`,
  `orders.iif_ordered_at_2024_02_1_0_sum` (for both `==` and `!=`),
  `orders.iif_month_ordered_at_2024_02_1_0_sum` and
  `orders.amount_5_and_ordered_at_2024_02_01_sum` — the relative point named by its
  text, never by the date it resolves to

#### Scenario: Date comparison as the whole source
- **WHEN** a measure on `orders` is written `sum(ordered_at >= '2024-02-01')`
- **THEN** its result key is `orders.ordered_at_2024_02_01_sum`, named like
  `sum(amount > 5)` → `orders.amount_5_sum`

#### Scenario: Nested aggregate operand
- **WHEN** a measure on `orders` is written `sum(amount * avg(amount, partition_by=status))`
- **THEN** its result key is `orders.amount_avg_amount_partition_by_status_sum`

#### Scenario: Nested transform operand
- **WHEN** a query over a month time dimension selects
  `sum(cumsum(sum(amount, partition_by=[status, ordered_at])) - 1)`
- **THEN** its key is derived from that formula text by the shared sanitizer (here
  capped with a stable hash), contains no internal representation, and is identical
  across runs

#### Scenario: Literal spellings
- **WHEN** measures on `orders` are written `sum(amount * 0.0000001)` and
  `sum(coalesce(amount, null))`
- **THEN** their result keys are `orders.amount_0_0000001_sum` and
  `orders.coalesce_amount_null_sum`

#### Scenario: Home-relative operand spelling
- **WHEN** a query rooted at `orders` selects `sum(customers.spend - 1)` and
  `sum(customers.spend - customers.regions.weight)`, both homed at `customers`
- **THEN** their result keys are `orders.customers.spend_1_sum` and
  `orders.customers.spend_regions_weight_sum` — the home path spelled once, as for
  `sum(customers.spend)` → `orders.customers.spend_sum`; the same holds for a
  pathed derived column, a pathed bucketed column and a pathed `IN` operand

#### Scenario: Nested constituent keeps its spelling
- **WHEN** a query rooted at `orders` selects
  `sum(customers.spend * sum(amount, partition_by=customers.regions.name))`, homed
  at `customers`
- **THEN** its key spells the row-level operand as `spend` and the nested
  aggregate's references as written (`amount`, `customers.regions.name`)

#### Scenario: Distinct constituents never share a column
- **WHEN** one query selects two distinct expression aggregates whose nested
  aggregates differ only in a detail their formula text does not spell
- **THEN** each returns its own value from its own column, or the query fails with
  the duplicate-key error — never one column silently serving both

#### Scenario: Derived key is referenceable
- **WHEN** a query selects one of the date-comparison or pathed expression
  aggregates above and references its derived key in a filter, in order, and from a
  downstream stage
- **THEN** each reference resolves to that measure, by executed values
