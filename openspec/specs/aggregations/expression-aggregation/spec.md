# aggregations/expression-aggregation Specification

## Purpose
Allows aggregating a same-model scalar expression directly — `sum(amount - cost)` —
with deterministic result naming and explicit boundaries for shapes that are not
yet supported (cross-model expressions, filtered columns, nesting).

## Requirements

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

#### Scenario: Expression aggregation inside a computed dimension
- **WHEN** a computed dimension's expression contains `sum(amount - cost, partition_by=region)`
- **THEN** it behaves like any partitioned aggregate inside a dimension expression, subject to the same grain guards

### Requirement: Expression result keys are deterministic
The result-column key for an expression aggregation SHALL be derived by the
same auto-naming rule used for computed dimensions (non-word characters
collapsed to underscores, digit-leading names prefixed, long names capped with
a stable hash), followed by the aggregation name and any existing parametric or
partition suffixes — insensitive to whitespace and formatting variants. An
explicit rename overrides the derived key. Two distinct expressions whose
derived keys collide SHALL fail with a clear duplicate-key error advising a
rename — never silently share a column.

#### Scenario: Derived key
- **WHEN** a measure on model `orders` is written `sum(amount - cost)`
- **THEN** its result key is `orders.amount_cost_sum` (same sanitizer as a computed dimension named from `amount - cost`)

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

### Requirement: Unsupported expression shapes fail with clear errors
The system SHALL reject, with errors naming the limitation: expressions
referencing joined-model columns (cross-model) at row level, expressions
referencing columns that carry a column-level filter, and transforms nested
inside the aggregated expression. An aggregation source consisting entirely of
attached values is a re-aggregation and SHALL be accepted (per
`queries/partitioned-aggregates` › Re-aggregation consumes attached operands as
datasets). An aggregation source mixing row-level references with attached
values is a row-grain aggregation and SHALL be accepted (per
`queries/semantics` › Row-grain aggregation sources). An attached
(aggregate-valued) parameter on an aggregation whose source is row-level SHALL
be accepted when the aggregation's operating grain determines it (per
`queries/partitioned-aggregates` › Attached parameters on row-level sources).
Under `broadcast`/`error` a cross-model aggregation's attached input SHALL read
only columns attributable from the aggregation's root — a typed error names the
root, the leaf and the `associate` remedy otherwise (per
`queries/partitioned-aggregates` › Default-mode twin of the associate shape).
Whether an aggregation runs over rows or over an operand dataset's cells is
decided by its source alone; every attached input, in the source or in a
parameter, is then attached by one mechanism — into the input relation for a
row-level source, as a constituent of the operand dataset for an attached one.

#### Scenario: Cross-model expression rejected
- **WHEN** a measure is written `sum(amount - customers.discount)`
- **THEN** it fails with an error stating cross-model expression aggregation is not supported

#### Scenario: Filtered-column operand rejected
- **WHEN** the expression references a column that has a column-level filter
- **THEN** it fails with an error naming the column and suggesting the colon form on a derived model column

#### Scenario: Nested aggregation rejected
- **WHEN** a measure is written `sum(cumsum(x) - 1)`
- **THEN** it fails with a typed error stating transforms cannot nest inside an
  aggregated expression

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
