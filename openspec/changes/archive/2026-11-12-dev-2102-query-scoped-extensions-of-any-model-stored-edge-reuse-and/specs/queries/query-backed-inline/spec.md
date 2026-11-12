## MODIFIED Requirements

### Requirement: Extensions over a spliced model
An extension of a stored query-backed model SHALL apply its columns, measures and joins exactly
once, to the model's spliced output, in every stage of a query list — whether the stage reads
the model as its source, as a join target or along a path — with the values an equivalent
extension of an explicit sibling stage holding the same queries produces.

#### Scenario: Root extension applies once
- **WHEN** a query's source is a stored query-backed model that the query extends with a column and a join
- **THEN** the column and the join are each present once and the values match the explicit splice

#### Scenario: Stage-level measure extension stays rejected
- **WHEN** a non-root stage reads a stored query-backed model and extends it with a measure that the stage selects
- **THEN** the stage executes, and the measure's value equals the same measure declared by an extension of an explicit sibling stage holding the model's queries
