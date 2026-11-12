## MODIFIED Requirements

### Requirement: Every datasource has a virtual time spine model

Every datasource SHALL expose a model named `time_spine` with exactly one column, `timestamp`, of type `time`, which is its primary key and denotes every instant. The model SHALL NOT be stored; it resolves in the datasource a query runs against. It SHALL appear in model listings, `inspect` and search as a built-in model, and its description SHALL list the models wired to it with their axis columns. Saving a user model whose name equals `time_spine` case-insensitively SHALL be rejected with a typed error, unless a model of that exact name and datasource is already stored; if a stored model is named exactly `time_spine`, any query referencing `time_spine` SHALL fail with a typed error naming the conflict, never silently using either model. A stored model whose name differs from `time_spine` only in case SHALL keep loading, saving and answering queries that spell it exactly; its load, its save and every query response that reads it SHALL carry the structured name-collision warning naming it and `time_spine`.

#### Scenario: Spine is listed with its wired models

- **WHEN** the models of the scenario datasource are listed and `time_spine` is inspected
- **THEN** `time_spine` appears as a built-in model with the single column `timestamp`, and its inspection names `orders` via `order_date` and `returns` via `return_date`

#### Scenario: Reserved name rejected on save

- **WHEN** a user saves a model named `time_spine`, and another named `Time_Spine`
- **THEN** each save fails with a typed error naming the reserved model and nothing is stored

#### Scenario: Pre-existing stored clash fails closed

- **WHEN** a model named `time_spine` is already stored in a datasource and a query there groups by `time_spine.timestamp` month
- **THEN** the query fails with a typed error naming the clash and asking to rename the stored model

#### Scenario: A pre-existing case variant keeps working and warns

- **WHEN** a model named `Time_Spine` is already stored in a datasource, it is loaded, saved unchanged, and a query rooted at `Time_Spine` counts its rows
- **THEN** the load, the save and the query each succeed and each carries the name-collision warning naming `Time_Spine` and `time_spine`

## ADDED Requirements

### Requirement: The spine is reached in any case
Every model-name position a query writes — `source_model`, an extension's model, the join targets of an extension, and the first segment of every dotted path in its dimensions, time dimensions, measures, filters and order — SHALL reach the built-in `time_spine` when its unquoted spelling equals `time_spine` case-insensitively, exactly as `time_spine` would, in every query of a query list. A stage of the list whose name the spelling matches SHALL take precedence; a spelling that names a stored model exactly SHALL reach that model. When the datasource the query runs against stores a model whose name differs from `time_spine` only in case, any other spelling that is not exactly `time_spine` SHALL fail with the typed ambiguity error naming `time_spine` and that model. A quoted spelling SHALL reach the spine only when it is exactly `"time_spine"`. String literals and the rest of every text SHALL be left unchanged.

#### Scenario: Upper-case spine root
- **WHEN** a query has `source_model: TIME_SPINE`, the time dimension `TIME_SPINE.timestamp` by month over Jan–Feb, and the measure `sum(orders.amount)`
- **THEN** it returns the same rows and result keys as the same query spelled `time_spine`

#### Scenario: Mixed-case spine reference without a root
- **WHEN** a query without `source_model` groups by `Time_Spine.timestamp` month over Jan–Feb and sums `orders.amount`, once as a dimension filter `Time_Spine.timestamp >= '2024-01-01'` and once in its order
- **THEN** each returns the same rows as the query spelled `time_spine`

#### Scenario: Quoted and literal spellings are untouched
- **WHEN** a query groups by `"TIME_SPINE".timestamp` month, and another filters `orders.note = 'TIME_SPINE.timestamp'` while grouping by `time_spine.timestamp` month
- **THEN** the first fails with the unresolved-reference error and the second compares against the literal text unchanged

#### Scenario: A stage named like the spine wins
- **WHEN** a query list has a stage `Time_Spine` and its main query is rooted at `TIME_SPINE`
- **THEN** the main query reads the stage

#### Scenario: A stored case variant makes other spellings ambiguous
- **WHEN** a datasource already stores a model `Time_Spine` and queries there are rooted at `TIME_SPINE`, at `time_spine` and at `Time_Spine`
- **THEN** the first fails with the ambiguity error naming `time_spine` and `Time_Spine`, the second reads the built-in spine, and the third reads the stored model with the name-collision warning

#### Scenario: A case variant in another datasource is no ambiguity
- **WHEN** datasource `a` stores a model `Time_Spine` and a query against datasource `b` is rooted at `TIME_SPINE`
- **THEN** it reads datasource `b`'s built-in spine

### Requirement: A stored join never targets the spine
Saving a model SHALL be refused with a typed error naming the model and the join when a join's `target_model` equals `time_spine` case-insensitively and no stored model of its datasource is named exactly as written; the error SHALL state that the spine joins each dataset through its time axis, so a declared join to it is never needed. On load, a stored model below schema version 23 SHALL have each such join removed, with a structured warning naming the model and the join, and SHALL be written back at the current version.

#### Scenario: A join to the spine is refused in any spelling
- **WHEN** a model `orders` is saved with a join to `time_spine` on `created_at = timestamp`, and again with the join target `TIME_SPINE`
- **THEN** each save fails with the typed error naming `orders` and the join, and nothing is stored

#### Scenario: A stored join to the spine is dropped on load
- **WHEN** a store written before schema version 23 holds a model `ev` without a time axis whose join targets `time_spine`, and a query groups by `time_spine.timestamp` month over Jan–Mar and sums `ev.amount`
- **THEN** `ev` loads without that join, with the warning naming `ev` and the join, is written back at the current version, and the query returns the monthly sums
