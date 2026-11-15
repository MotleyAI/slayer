## MODIFIED Requirements

### Requirement: Extensions are scoped to their query object

An extension SHALL be visible only inside the query object that declares it.
- A later stage reading that stage SHALL see only the stage's output schema: its output columns and the edges it inherits. An edge the stage inherits from an extension's relationship (`queries/multi-stage` › Stages inherit edges on pass-through keys) is a new edge of the stage's dataset, carrying that relationship's name; the extension itself, its metrics and its relationships on the extended dataset SHALL stay invisible.
- Other stages SHALL NOT see the extension.
- Stored definitions SHALL NOT see it: stored relationships, stored fields, saved metrics, and the stages of a stored query-backed model.

An extension's `source_name` and each of its relationships' `to` SHALL resolve, in order, to:
1. a sibling stage of the query list;
2. else a stored dataset of the query's datasource;
3. else fail with a model-not-found error.

#### Scenario: Extension invisible to the next stage
- **WHEN** a stage extends `customers` with a metric it does not project, and a later stage of the same list with population `orders` references `customers.<metric>`
- **THEN** the later stage fails with an unknown reference

#### Scenario: Stage shadows a same-named model
- **WHEN** a query list has a stage named `customers` and another stage extends `customers`
- **THEN** the extension applies to the stage's output, not to the stored dataset

#### Scenario: Unknown extension target
- **WHEN** a query extends a name that is no stage or stored dataset
- **THEN** it fails with a model-not-found error naming that name

#### Scenario: An extension relationship reaches a later stage only through a pass-through key
- **WHEN** a stage extends `orders` with a relationship to `regions` on `region_id`, once projecting `region_id` and once not, and a later stage over it selects `regions.name`
- **THEN** with `region_id` projected the later stage reaches `regions` through the inherited edge; without it, `regions` is reached by no join path
