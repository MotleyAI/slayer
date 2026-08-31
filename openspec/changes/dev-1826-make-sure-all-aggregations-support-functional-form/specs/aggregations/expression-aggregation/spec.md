## Purpose

Allows aggregating a same-model scalar expression directly — `sum(amount - cost)` —
with deterministic result naming and explicit boundaries for shapes that are not
yet supported (cross-model expressions, filtered columns, nesting).

## ADDED Requirements

### Requirement: Same-model scalar expressions can be aggregated
The system SHALL accept `agg(<expression>, [args])` where the expression is
built from bare host-model column references, scalar-allowlist functions,
arithmetic operators, and literals — in every position that accepts functional
aggregations, composing with reserved kwargs (`window`, `partition_by`),
parametric aggregations, custom aggregations, rename, filter-form measures,
post-aggregation filters, and order.

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
- **THEN** it succeeds (a constant is a valid same-model expression)

#### Scenario: Derived SQL columns as operands
- **WHEN** the expression references columns that are themselves defined by model SQL expressions
- **THEN** the aggregation is computed over their evaluated values

#### Scenario: Stage-scope expressions
- **WHEN** a stage formula in a multi-stage query aggregates an expression over the current stage's output columns
- **THEN** it succeeds, named within the stage's namespace

### Requirement: Expression result keys are deterministic
The result-column key for an expression aggregation SHALL be derived from the
canonical parsed expression — insensitive to whitespace and formatting
variants — by mapping operators to words, sanitizing remaining punctuation,
appending the aggregation name and any existing parametric or partition
suffixes, and capping length with a stable hash tail; an explicit rename
overrides the derived key.

#### Scenario: Derived key
- **WHEN** a measure on model `orders` is written `sum(amount - cost)`
- **THEN** its result key is `orders.amount_minus_cost_sum`

#### Scenario: Formatting-insensitive identity
- **WHEN** the same expression is written `sum(amount-cost)` and `sum( amount - cost )`
- **THEN** both produce the identical result key

#### Scenario: Long expression capped
- **WHEN** the sanitized expression segment exceeds the length cap
- **THEN** the key uses a truncated prefix plus a short stable hash, deterministic across runs

#### Scenario: Rename override
- **WHEN** a measure is declared `{"formula": "sum(amount - cost)", "name": "profit"}`
- **THEN** the result key uses `profit`

### Requirement: Unsupported expression shapes fail with clear errors
The system SHALL reject, with errors naming the limitation: expressions
referencing joined-model columns (cross-model), expressions referencing
columns that carry a column-level filter, and nested aggregations or
transforms inside the aggregated expression.

#### Scenario: Cross-model expression rejected
- **WHEN** a measure is written `sum(amount - customers.discount)`
- **THEN** it fails with an error stating cross-model expression aggregation is not supported

#### Scenario: Filtered-column operand rejected
- **WHEN** the expression references a column that has a column-level filter
- **THEN** it fails with an error naming the column and suggesting the colon form on a derived model column

#### Scenario: Nested aggregation rejected
- **WHEN** a measure is written `sum(sum(x))` or `sum(cumsum(x) - 1)`
- **THEN** it fails with an error stating aggregations/transforms cannot be nested inside an aggregated expression

### Requirement: Gate and type semantics for expressions
Per-column eligibility gates (allowed-aggregations whitelists, primary-key and
type-default gates) SHALL NOT apply to multi-token expression operands — the
expression is a new derived quantity owned by the query author — while global
validation still applies: the aggregation name must be known, and numeric-only
aggregations SHALL be rejected when the expression is confidently non-numeric;
display classification derives from the inferred value class, defaulting to
plain numeric.

#### Scenario: Whitelist does not block expressions
- **WHEN** column `quantity` whitelists only `min` and `max`, and a measure is written `sum(price * quantity)`
- **THEN** the query succeeds

#### Scenario: Confidently non-numeric rejected
- **WHEN** a measure is written `sum(lower(name))`
- **THEN** binding fails with a type error rather than failing in the database
