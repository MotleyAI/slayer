## MODIFIED Requirements

### Requirement: Gate and type semantics for expressions
Per-column eligibility gates (allowed-aggregations whitelists, primary-key and
type-default gates) SHALL NOT apply to multi-token expression operands — the
expression is a new derived quantity owned by the query author — while global
validation still applies: the aggregation name must be known, and numeric-only
aggregations SHALL be rejected when the expression is confidently non-numeric;
a boolean-valued expression is numeric for exactly the aggregations of the BOOLEAN
type-default set (per `aggregations/boolean-inputs`); display classification derives
from the inferred value class, defaulting to plain numeric.

#### Scenario: Whitelist does not block expressions
- **WHEN** column `quantity` whitelists only `min` and `max`, and a measure is written `sum(price * quantity)`
- **THEN** the query succeeds

#### Scenario: Confidently non-numeric rejected
- **WHEN** a measure is written `sum(lower(name))`
- **THEN** binding fails with a type error rather than failing in the database

#### Scenario: Boolean expression accepted for the boolean default set
- **WHEN** a measure is written `avg(coalesce(flag, false))`
- **THEN** binding succeeds and the measure is the share of true rows, while
  `stddev_samp(coalesce(flag, false))` fails at binding with a type error

### Requirement: Row-level expressions can be aggregated
The system SHALL accept `agg(<expression>, [args])` where the expression is built
from row-level column references — bare host-model columns and dotted joined-model
paths alike — scalar-allowlist functions, arithmetic operators, comparisons, boolean
connectives, `IN` / `NOT IN` predicates, and literals, in
every position that accepts functional aggregations, composing with reserved kwargs
(`window`, `partition_by`), parametric aggregations, custom aggregations, rename,
filter-form measures, post-aggregation filters, and order. The aggregation SHALL run
over the rows of the expression's home dataset (per `queries/semantics` › Home
dataset of a row-level aggregation source) and SHALL otherwise follow every rule a
single-column source rooted at that dataset follows: attribution and
`to_many_handling` modes, explicit partition keys, `window=`, filter routing, and
the filter, order and computed-dimension positions. Custom aggregation names SHALL
resolve on the model at the expression's anchor — the longest common prefix of its
leaves' join paths (the host for a literal-only or host-mixed expression).

#### Scenario: Arithmetic expression
- **WHEN** a query measure is written `sum(amount - cost)`
- **THEN** the generated SQL aggregates the row-level expression (`SUM(amount - cost)`), grouped like any other measure

#### Scenario: Comparison expression
- **WHEN** a query measure is written `sum(amount > 15)`
- **THEN** it is accepted — never the former cannot-aggregate-a-comparison rejection — and
  returns the number of rows whose `amount` exceeds 15 (per `aggregations/boolean-inputs`)

#### Scenario: Scalar function inside
- **WHEN** a measure is written `count_distinct(upper(email))`
- **THEN** the aggregation applies over the scalar-transformed value

#### Scenario: Parametric and custom aggregations over expressions
- **WHEN** a measure is written `percentile(price * quantity, p=0.5)` or `my_agg(price * quantity)` for a model-defined custom aggregation
- **THEN** the aggregation receives the row-level expression as its value

#### Scenario: Expression aggregation in a post-aggregation filter
- **WHEN** a filter is written `sum(amount - cost) > 0`
- **THEN** it is applied after aggregation (HAVING semantics), consistent with single-column aggregate filters

#### Scenario: Constant-only expression
- **WHEN** a measure is written `count(1)`
- **THEN** it succeeds (a constant is a valid expression, homed at the host)

#### Scenario: Derived SQL columns as operands
- **WHEN** the expression references columns that are themselves defined by model SQL expressions
- **THEN** the aggregation is computed over their evaluated values

#### Scenario: Stage-scope expressions
- **WHEN** a stage formula in a multi-stage query aggregates an expression over the current stage's output columns
- **THEN** it succeeds, named within the stage's namespace

#### Scenario: Expression aggregation inside a computed dimension
- **WHEN** a computed dimension's expression contains `sum(amount - cost, partition_by=region)`
- **THEN** it behaves like any partitioned aggregate inside a dimension expression, subject to the same grain guards

#### Scenario: Host-homed cross-model expression
- **WHEN** a query rooted at `orders` selects `sum(amount - customers.discount)` by an
  orders-level dimension, `orders → customers` being provably to-one
- **THEN** it executes over the `orders` rows, each order paired with its own
  customer's discount, correct by hand-computed values on SQLite and DuckDB — never
  the former cross-model rejection

#### Scenario: Target-homed cross-model expression
- **WHEN** a query rooted at `orders` selects `sum(customers.spend - customers.regions.pop)`
- **THEN** it runs over the `customers` rows, each customer counted exactly once
  however many orders it has, by executed values, and `sum(customers.spend)` and
  `sum(customers.spend + 0)` return identical values

#### Scenario: Two-branch expression homes at the common ancestor
- **WHEN** a query rooted at `orders` selects `sum(customers.spend - stores.rent)`
  with both hops provably to-one
- **THEN** it runs over the `orders` rows with no warning, by executed values

#### Scenario: Cross-model expression with explicit grain and window
- **WHEN** a query selects `sum(amount - customers.discount, partition_by=region)` and,
  over a month time dimension, `sum(amount - customers.discount, window='90d')`
- **THEN** each behaves exactly as the same modifier over a single-column source at
  the same home, by executed values, with unchanged cardinality

#### Scenario: Cross-model expression in filter, order and dimension positions
- **WHEN** the expression aggregate appears only in a filter, only as an ORDER BY
  target, or grain-self-contained inside a computed dimension
- **THEN** each position yields the value the measure form returns, and the filter
  prunes result rows without altering surviving values

#### Scenario: Custom aggregation resolves on the anchor model
- **WHEN** `wsum(customers.spend - customers.regions.pop)` names a custom aggregation
  defined on `customers`
- **THEN** it resolves and executes; the same name defined only on `orders` is an
  unknown aggregation for that expression
