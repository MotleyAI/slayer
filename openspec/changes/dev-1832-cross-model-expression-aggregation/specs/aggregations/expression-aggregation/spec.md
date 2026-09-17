## REMOVED Requirements

### Requirement: Same-model scalar expressions can be aggregated
**Reason**: Superseded by *Row-level expressions can be aggregated*, which admits
joined-model (dotted) leaves and roots the aggregation at the expression's home
dataset.
**Migration**: None — every same-model expression keeps its behaviour, result key and
generated SQL.

### Requirement: Unsupported expression shapes fail with clear errors
**Reason**: The three v1 boundaries (cross-model leaves, filtered-column operands,
transforms nested in the source) are lifted; the surviving typing rules move to
*Expression source typing*.
**Migration**: Formerly rejected queries now compile per the new requirements;
`first`/`last` over an expression keeps its existing rejection.

## ADDED Requirements

### Requirement: Row-level expressions can be aggregated
The system SHALL accept `agg(<expression>, [args])` where the expression is built
from row-level column references — bare host-model columns and dotted joined-model
paths alike — scalar-allowlist functions, arithmetic operators, and literals, in
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

### Requirement: Expression source typing
The system SHALL type an aggregation's expression source by its leaves and
constituents alone, never by its spelling. A `first`/`last` aggregation over an
expression source SHALL be rejected with the existing not-supported-over-an-expression
error. A numeric-only aggregation over a confidently non-numeric expression SHALL be
rejected at binding. An expression whose leaves no candidate home determines over
provably to-one hops SHALL fail with the existing input-safety error naming the leaf
and the fanning or unproven hop, and a leaf whose derived definition cannot be
analysed SHALL fail with the existing analyzability error naming the column — never
a multiplied or silently wrong value. A column carrying a column-level filter SHALL be
an ordinary derived operand (per `models/column-filters`). A transform nested in the
source SHALL be an attached constituent (per `queries/partitioned-aggregates` ›
Re-aggregation consumes attached operands as datasets); a row-level leaf inside it
that is not a projected grain key SHALL be rejected per `queries/transforms` ›
Non-shift transforms reject grain-refining row-level leaves. An aggregation source
consisting entirely of attached values is a re-aggregation and SHALL be accepted; a
source mixing row-level references with attached values is a row-grain aggregation
and SHALL be accepted (per `queries/semantics` › Row-grain aggregation sources), except
that a collapsing (`first`/`last`) transform constituent mixed with a row-level
reference SHALL be rejected with a typed error naming the shape (its broadcast onto
row-level operands is deferred to DEV-1928); an
attached (aggregate-valued) parameter on a row-level source SHALL be accepted when the
aggregation's operating grain determines it (per `queries/partitioned-aggregates` ›
Attached parameters on row-level sources). Under `broadcast`/`error` a cross-model
aggregation's attached input SHALL read only columns attributable from the
aggregation's home — a typed error names the root, the leaf and the `associate`
remedy otherwise (per `queries/partitioned-aggregates` › Default-mode twin of the
associate shape). Whether an aggregation runs over rows or over an operand dataset's
cells is decided by its source alone; every attached input, in the source or in a
parameter, is then attached by one mechanism — into the input relation for a
row-level source, as a constituent of the operand dataset for an attached one.

#### Scenario: Filtered-column operand accepted
- **WHEN** a measure is written `sum(q_amount - 1)` where `q_amount` is `amount`
  carrying the column filter `product = 'Q'`
- **THEN** it executes as the sum over Q rows of `amount - 1` — the operand is
  `CASE WHEN product = 'Q' THEN amount END` — by executed values, never the former
  filtered-operand rejection

#### Scenario: Grained transform in the source accepted
- **WHEN** a query over a month time dimension selects
  `sum(cumsum(amount:sum(partition_by=[region, ordered_at])) - 1)`
- **THEN** each month carries the sum over regions of that region's running total
  minus one per cell, by hand-computed values on SQLite and DuckDB, distinguishable
  from the ungrained identity — never the former nested-transform rejection

#### Scenario: Row leaf under a nested transform rejected
- **WHEN** a measure is written `sum(cumsum(weight) - 1)` with `weight` not a query
  dimension
- **THEN** it fails at plan time with the typed row-leaf error naming the transform
  and the aggregate-the-leaf remedy, citing no tracking issue

#### Scenario: Fanning leaf fails closed
- **WHEN** an expression leaf is reachable from every candidate home only across a
  fanning or unproven join hop
- **THEN** the query fails with the input-safety error naming the leaf and the hop,
  never a multiplied value

#### Scenario: Unanalysable derived leaf fails closed
- **WHEN** an expression leaf names a derived column whose definition no dialect can
  parse
- **THEN** the query fails with the analyzability error naming the column

#### Scenario: Ranked aggregation over an expression keeps its error
- **WHEN** a measure is written `first(amount - customers.discount)`
- **THEN** it fails with the existing error that `first` is not supported over an
  expression — never a cross-model error and never wrong values

#### Scenario: Fully attached source accepted
- **WHEN** a measure is written `avg(sum(amount, partition_by=[city, region]))`
- **THEN** it is accepted and compiles as a re-aggregation, not rejected by the
  expression gate

#### Scenario: Mixed row and attached source accepted
- **WHEN** a measure is written
  `sum(quantity * avg(unit_price, partition_by=product))`
- **THEN** it is accepted and compiles at row grain — the attached value
  broadcast per base row — not rejected by the expression gate

#### Scenario: Attached parameter on a row-level source accepted
- **WHEN** a measure is written
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  rooted at `orders` under `to_many_handling: "associate"`
- **THEN** it is accepted and compiles with the parameter's value attached into
  the aggregation's input relation — never the attached-parameter rejection —
  and the same aggregation with a row-level parameter is unaffected

#### Scenario: Attached parameter on a row-level source rejected
- **WHEN** a measure is written
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  rooted at `orders` under the default `broadcast` `to_many_handling`
- **THEN** it fails at plan time with a typed error naming the producer's root
  `customers`, the unreachable leaf `amount` and the `associate` remedy,
  containing no issue reference; the same aggregation with a parameter reading
  only `customers`-side columns executes

## MODIFIED Requirements

### Requirement: Expression result keys are deterministic
The result-column key for an expression aggregation SHALL be derived by the
same auto-naming rule used for computed dimensions (non-word characters
collapsed to underscores, digit-leading names prefixed, long names capped with
a stable hash), followed by the aggregation name and any existing parametric or
partition suffixes — insensitive to whitespace and formatting variants, a dotted
leaf spelling with its dots collapsed like any other non-word character. An
explicit rename overrides the derived key. Two distinct expressions whose
derived keys collide SHALL fail with a clear duplicate-key error advising a
rename — never silently share a column.

#### Scenario: Derived key
- **WHEN** a measure on model `orders` is written `sum(amount - cost)`
- **THEN** its result key is `orders.amount_cost_sum` (same sanitizer as a computed dimension named from `amount - cost`)

#### Scenario: Dotted leaf in the derived key
- **WHEN** a measure on model `orders` is written `sum(amount - customers.discount)`
- **THEN** its result key is `orders.amount_customers_discount_sum`, and `name`
  overrides it as for any measure

#### Scenario: Colliding derived keys fail loudly
- **WHEN** one query contains both `sum(amount - cost)` and `sum(amount + cost)` without renames
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
