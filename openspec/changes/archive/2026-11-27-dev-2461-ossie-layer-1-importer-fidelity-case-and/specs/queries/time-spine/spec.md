## MODIFIED Requirements

### Requirement: The spine is reached in any case
Every model-name position a query writes — `population`, an extension's model, the join targets of an extension, and the first segment of every dotted path in its dimensions, time dimensions, measures, filters and order — SHALL reach the built-in `time_spine` when its unquoted spelling equals `time_spine` case-insensitively, exactly as `time_spine` would, in every query of a query list. A stage of the list whose name the spelling matches SHALL take precedence; a spelling that names a stored model exactly SHALL reach that model. When the datasource the query runs against stores a model whose name differs from `time_spine` only in case, any other spelling that is not exactly `time_spine` SHALL fail with the typed ambiguity error naming `time_spine` and that model. A quoted spelling SHALL reach the spine only when it is exactly `"time_spine"` or its Ossie normalized form `"TIME_SPINE"`. String literals and the rest of every text SHALL be left unchanged.

#### Scenario: Upper-case spine root
- **WHEN** a query has `population: TIME_SPINE`, the time dimension `TIME_SPINE.timestamp` by month over Jan–Feb, and the measure `sum(orders.amount)`
- **THEN** it returns the same rows and result keys as the same query spelled `time_spine`

#### Scenario: Mixed-case spine reference without a root
- **WHEN** a query without `population` groups by `Time_Spine.timestamp` month over Jan–Feb and sums `orders.amount`, once as a dimension filter `Time_Spine.timestamp >= '2024-01-01'` and once in its order
- **THEN** each returns the same rows as the query spelled `time_spine`

#### Scenario: Quoted and literal spellings are untouched
- **WHEN** a query groups by `"Time_Spine".timestamp` month, and another filters `orders.note = 'TIME_SPINE.timestamp'` while grouping by `time_spine.timestamp` month
- **THEN** the first fails with the unresolved-reference error and the second compares against the literal text unchanged

#### Scenario: A quoted spine name in Ossie's normalized form is the spine
- **WHEN** a query groups by `"TIME_SPINE".timestamp` month
- **THEN** it returns the same rows as the query grouping by `time_spine.timestamp` month

#### Scenario: A stage named like the spine wins
- **WHEN** a query list has a stage `Time_Spine` and its main query is rooted at `TIME_SPINE`
- **THEN** the main query reads the stage

#### Scenario: A stored case variant makes other spellings ambiguous
- **WHEN** a datasource already stores a model `Time_Spine` and queries there are rooted at `TIME_SPINE`, at `time_spine` and at `Time_Spine`
- **THEN** the first fails with the ambiguity error naming `time_spine` and `Time_Spine`, the second reads the built-in spine, and the third reads the stored model with the name-collision warning

#### Scenario: A case variant in another datasource is no ambiguity
- **WHEN** datasource `a` stores a model `Time_Spine` and a query against datasource `b` is rooted at `TIME_SPINE`
- **THEN** it reads datasource `b`'s built-in spine
