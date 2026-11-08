# queries/cross-model-aggregates — delta

## MODIFIED Requirements

### Requirement: Every aggregate has exactly one disposition
Planning SHALL guarantee that every aggregate reference — measure, composite leaf, computed-dimension, filter-only, order-only, windowed, ranked, host-grain, or nested — is either computed inline, routed to exactly one producer, or rejected with a clear error. A discovery gap MUST surface as an explicit planner error, never as a silently dropped or wrong value.

Beyond routing, planning SHALL assign every value the plan carries — row leaf, aggregate, producer placeholder, composite, transform, filter predicate — exactly one materialisation stage in the emitted statement's pipeline (host base, producer, combined, derived level — where a value reading a transform is staged one level above the deepest transform it reads: a transform is one more relation layer, a composite renders inline and is never a layer of its own), derived only from the stages of the values it references, together with whether any consumer requires it as a column. A plan in which a value references a value of a later stage, or in which any value is left unstaged, SHALL be rejected at plan time with a typed error before any SQL is generated. SQL generation SHALL place each value in the relation its stage names and read earlier relations by alias only, never re-deriving placement from the value's shape; a value whose stage names a relation that cannot render it MUST fail closed, never render at the wrong grain.

#### Scenario: Unrouted shapes fail loudly
- WHEN a query contains an aggregate shape the planner cannot route
- THEN the query fails with a clear error naming the shape, never with missing or incorrect values

#### Scenario: Every planned value carries one stage
- **WHEN** any query is planned, including the producer bodies nested inside it
- **THEN** every value in the plan carries exactly one materialisation stage; a value that
  reads a transform is staged strictly later than every transform it reads, and no value
  is staged earlier than any operand

#### Scenario: A transform over a composite over a transform
- **WHEN** a query projects `change(x)` and filters on `last(change(x))`
- **THEN** `change(x)` is staged one level above its `time_shift`, `last(change(x))`
  shares the composite's level (only transforms stratify; the composite renders inline),
  and the query executes selecting the declining partition (pinned by the DEV-1859
  last-over-change test)

#### Scenario: Staging is a function of the term alone
- **WHEN** the same query filters on `last(change(x))` with and without also projecting
  `change(x)`
- **THEN** every shared value carries the same stage in both plans and the surviving rows
  are identical

#### Scenario: Deeper alternation of transforms and composites
- **WHEN** a transform's operand is a composite over a transform over a transform (for
  example `last(change(cumsum(x)))`)
- **THEN** each transform is staged one level above the deepest transform it reads and
  the query plans with no construction-inspecting refusal

#### Scenario: Later-stage reference is rejected at plan time
- **WHEN** a plan is constructed in which a value references a value staged later than
  itself, in which a transform reads a transform not staged strictly earlier, or in which
  some value has no stage
- **THEN** plan construction fails with a typed error naming the values, and no SQL is
  generated

#### Scenario: Operands needed later are materialised earlier
- **WHEN** a value at a later stage (a transform, a composite reading a derived value, a
  filter predicate, an ORDER BY key) reads a value materialised at an earlier stage that
  is not otherwise selected
- **THEN** the earlier value is projected as a hidden column of its own relation and the
  later value reads it by alias, by executed values and by the generated SQL

#### Scenario: Already-legal shapes keep their SQL

- **WHEN** the golden-SQL suites for every previously supported shape run after
  materialisation becomes planner-owned
- **THEN** generated SQL is byte-identical except for individually approved divergences,
  and executed values are unchanged for every divergence
