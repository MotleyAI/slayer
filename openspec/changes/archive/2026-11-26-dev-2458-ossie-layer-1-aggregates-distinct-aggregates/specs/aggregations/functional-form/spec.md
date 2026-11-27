## MODIFIED Requirements

### Requirement: Syntax boundaries are preserved
Mode-A raw-SQL surfaces SHALL continue to treat `SUM(x)` as raw SQL. In Mode B, a leading `DISTINCT` inside a call
(`distinct` and the callee matched case-insensitively, over one operand `<expr>` that is any aggregation source other
than `*`) SHALL be an exact equivalent — same result key, SQL and values — of:
`count(distinct <expr>[, <arguments>])` of `count_distinct(<expr>[, <arguments>])`,
`sum(distinct <expr>[, <arguments>])` of `sum_distinct(<expr>[, <arguments>])`,
`avg(distinct <expr>[, <arguments>])` of `avg_distinct(<expr>[, <arguments>])`,
`min(distinct <expr>[, <arguments>])` and `max(distinct <expr>[, <arguments>])` of `min(<expr>[, <arguments>])` and
`max(<expr>[, <arguments>])`, and `array_agg(distinct <expr>)` of `collect(<expr>)`. Every other SQL-style `DISTINCT`
inside a call — another aggregation, `distinct *`, or several distinct operands — SHALL be rejected with an error
listing the accepted DISTINCT forms. Text inside string literals SHALL never be read as `distinct`.

#### Scenario: Mode A unchanged
- **WHEN** a model column's `sql` contains `SUM(amount)`
- **THEN** it is passed through as raw SQL exactly as before

#### Scenario: count(distinct) equals count_distinct
- **WHEN** a measure is written `count(distinct user_id)` or `COUNT(DISTINCT user_id)`
- **THEN** its columns, rows and SQL equal those of `count_distinct(user_id)`

#### Scenario: count(distinct) with an expression and a partition
- **WHEN** a measure is written `count(distinct coalesce(user_id, 0), partition_by=region)`
- **THEN** it equals `count_distinct(coalesce(user_id, 0), partition_by=region)`

#### Scenario: sum, avg, min and max with DISTINCT
- **WHEN** measures are written `sum(distinct amount)`, `AVG(DISTINCT amount)`, `min(distinct amount)` and `MAX(DISTINCT amount, partition_by=region)`
- **THEN** they equal `sum_distinct(amount)`, `avg_distinct(amount)`, `min(amount)` and `max(amount, partition_by=region)`

#### Scenario: DISTINCT keyword rejected
- **WHEN** a measure is written `stddev(distinct amount)`, `count(distinct *)` or `count(distinct a, b)`
- **THEN** each is rejected with the error listing the accepted DISTINCT forms

### Requirement: Positional parameters fold onto declared parameter order
An aggregation call MAY pass declared parameters positionally after its source
(or, for a re-aggregation, after its operand): positional values SHALL bind to
the aggregation's declared parameter order — the built-in registry
(`percentile`, `percentile_disc` and `percentile_approx` → `p`; `weighted_avg` → `weight`;
`corr`/`covar_samp`/`covar_pop` → `other`) or a custom aggregation's `params` declaration order — yielding the
identical aggregation identity, SQL, results, and result keys as the named
spelling. Passing a parameter both positionally and by name, or more positional
values than declared parameters — any positional value at all on an aggregation
that declares none — SHALL fail with a clear error naming the rule. Ranked
`first`/`last` declare no parameters — they take at most one positional value,
their ranking key (resolved per › Default ranking key when omitted), which, when
given, SHALL be a row-level expression — a column being the one-leaf case — never a
literal-only expression or one carrying an aggregate, transform or attached value. After binding an
attached (aggregate- or transform-valued) parameter is therefore always a named
parameter.

#### Scenario: Positional percentile equals named
- **WHEN** a measure is written `percentile(price, 0.9)`, `percentile_disc(price, 0.9)` or `percentile_approx(price, 0.9)`
- **THEN** SQL, results, and result keys are identical to the `p=0.9` spelling of the same aggregation

#### Scenario: Positional parameter on a re-aggregation outer
- **WHEN** a measure is written
  `percentile(sum(amount, partition_by=[city, region]), 0.9)`
- **THEN** it equals the `p=0.9` spelling by executed values

#### Scenario: Custom aggregation binds positionals by declared order
- **WHEN** a model declares `wavg(weight)` and a measure is written `wavg(amount, id)`
- **THEN** it equals `wavg(amount, weight=id)` by executed values

#### Scenario: Duplicate and excess positional parameters error
- **WHEN** `percentile(price, 0.9, p=0.5)` or `percentile(price, 0.9, 0.5)` is submitted
- **THEN** each fails with a clear error naming the duplicated parameter or the
  declared-parameter count

#### Scenario: Positional value on a parameterless aggregation errors
- **WHEN** `sum(amount, 1)` or `sum(customers.spend, sum(customers.spend, partition_by=status))`
  is submitted, under any `to_many_handling` mode
- **THEN** each fails at bind with a clear error naming the aggregation and that it
  takes no parameters — never an executed value

#### Scenario: Invalid first/last ranking key errors
- **WHEN** `last(amount, sum(amount, partition_by=region))`, `last(amount, 1)` or
  `last(amount, id, amount)` is submitted
- **THEN** each fails at bind with a clear error naming the ranking-key rule

#### Scenario: Expression ranking key
- **WHEN** a measure is written `last(balance, coalesce(updated_at, created_at))`
- **THEN** it is the `balance` of the row latest by the coalesced timestamp, by
  executed values on SQLite and DuckDB

#### Scenario: Positional transform parameter equals named
- **WHEN** a measure is written
  `weighted_avg(customers.spend, rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
  rooted at `orders`
- **THEN** it binds to the identical aggregation identity as the `weight=rank(..., direction='desc')`
  spelling and returns identical result keys and values
