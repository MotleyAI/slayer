## MODIFIED Requirements

### Requirement: Stored rank calls without a direction load as descending

A persisted model, query or memory whose stored schema version predates this
change SHALL load with every `rank(` / `dense_rank(` call lacking a top-level
`direction=` in its Mode-B fields rewritten to carry `direction='desc'`,
preserving its pre-change ordering. The Mode-B fields are `ModelMeasure.formula`
and a query's `measures`, `filters`, `dimensions`, `time_dimensions`, `order` and
`main_time_dimension`, including queries nested in `source_queries`, the measures of a
stored query's extensions (in either stored spelling), and `Memory.query`. Mode-A SQL (`Column.sql`, model
`filters`, `Column.filter`, aggregation templates) and `ntile` / `percent_rank`
calls SHALL never be rewritten. The rewrite SHALL apply only to a payload read
from storage or one that declares an explicit schema version older than the
current one; a payload without a version, or at the current version, SHALL be
left as written, so a bare call in it fails with the missing-direction error. The
rewrite SHALL be idempotent, SHALL leave a formula it cannot tokenise
byte-identical, and a migrated model SHALL be persisted back at the current
version on first load.

#### Scenario: A stored model measure keeps its descending meaning

- **WHEN** a model stored at the previous version holds the measure
  `rank(sum(amount))` and is loaded
- **THEN** the measure reads `rank(sum(amount), direction='desc')`, the stored document
  is rewritten at the current version, and querying it returns the descending ranks

#### Scenario: A stored query's every Mode-B field is rewritten

- **WHEN** a stored query-backed model's `source_queries` entry holds bare
  `rank(` / `dense_rank(` calls in a measure, a filter, an order item and a computed
  dimension expression, nested inside other calls and colon syntax
- **THEN** every call gains `direction='desc'` and nothing else in the formulas changes

#### Scenario: Unversioned legacy documents are rewritten

- **WHEN** a stored model with no `version`, whose nested source query also has no
  `version`, or a stored memory (YAML and SQLite) with no `version` or with an
  unversioned `query`, holds bare `rank(` calls
- **THEN** they load with `direction='desc'` filled in

#### Scenario: A fresh payload is never filled in

- **WHEN** a query or model with no `version`, or at the current version, is
  submitted through the API, MCP or Python with a bare `rank(sum(amount))`
- **THEN** it fails with the missing-direction `TransformArgumentError`

#### Scenario: A payload declaring an old version is treated as legacy

- **WHEN** a query submitted with an explicit older `version` holds `rank(sum(amount))`
- **THEN** it is filled in with `direction='desc'`, exactly as a stored document would be

#### Scenario: Calls that need no rewrite are untouched

- **WHEN** a stored document holds `rank(sum(amount), direction='asc')`, the text
  `rank(` inside a string literal, an attribute call `x.rank(`, `ntile(sum(amount), n=4)`,
  `percent_rank(sum(amount))`, or `dense_rank() over (order by id)` in a `Column.sql`
- **THEN** each is left byte-identical

#### Scenario: The rewrite is idempotent

- **WHEN** an already-migrated document is migrated again
- **THEN** it is unchanged

#### Scenario: An untokenisable formula still loads

- **WHEN** a stored model's measure formula cannot be tokenised
- **THEN** the formula is left byte-identical and the model still loads
