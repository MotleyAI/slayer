## MODIFIED Requirements

### Requirement: Statements are composed as a syntax tree and rendered once

Every generated statement, including its CTEs, stages, producers, outer projection and
filters, SHALL be composed as a syntax tree and rendered to SQL text once, after its last
rewrite. SQL text that has already been emitted for a statement SHALL NOT be parsed back
for any purpose — to compose a larger statement, to apply a session policy, to decide a
database refusal, to read the response's result columns, to find the tables a cached result
depends on, or to probe column types; each of these reads or rewrites the statement's
syntax tree. Only the validators that exist to check the emitted text read it. A
dialect-specific clause (a function name, pagination syntax, identifier quoting) chosen
during composition SHALL therefore reach the emitted SQL unchanged.

#### Scenario: A statement is never rendered while it is being composed

- **WHEN** any query in the test suite is compiled on any dialect
- **THEN** no statement-level SQL node is rendered to text before the final render
- **AND** a deliberate render-then-reparse inside composition (verbatim or with the text
  edited in between) fails the harness law

#### Scenario: T-SQL composition needs no re-parse

- **WHEN** a transform-chain query with a limit is compiled for T-SQL
- **THEN** the emitted SQL is produced without parsing any previously emitted SQL
- **AND** a composition failure raises an error instead of falling back to a different
  statement shape

#### Scenario: No execution parses a statement it rendered

- **WHEN** any engine operation in the test suite runs — execute, dry run, explain,
  cache evict or refresh, column-type probe, join-cardinality detection, model save
- **THEN** no SQL text equal to a statement rendered during that operation is parsed,
  except by the emitted-text validators
- **AND** a deliberate re-parse of the rendered statement, directly in the execution
  pipeline or several calls below it, fails the harness law

#### Scenario: Result columns and cache tables match the rendered statement

- **WHEN** a query is executed on any dialect, including one whose result names are
  length-fitted or mangled on emission
- **THEN** the response's result columns are the statement's canonical result keys in
  projection order
- **AND** the tables recorded for cache invalidation are exactly the physical tables the
  executed (policy-scoped) statement reads

#### Scenario: ClickHouse list refusal is decided from the statement

- **WHEN** a ClickHouse query that both unnests a list and filters through a correlated
  semi-join in the same SELECT runs on a server from 25.4 up to (excluding) 25.8
- **THEN** it is refused naming 25.8, exactly as before
- **AND** the same query with the list unnest and the semi-join in different SELECTs is
  not refused for that reason
