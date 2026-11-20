## ADDED Requirements

### Requirement: A custom aggregation's result type
An aggregation definition MAY declare its result `datatype` (any dataset field type); a
declared ARRAY has elements of unknown type. The result type of an aggregation rendered
from a formula — a custom aggregation, or a model formula overriding a built-in — SHALL
be, in order:
1. the definition's declared `datatype`;
2. else the type of its formula, inferred from the formula's SQL with each placeholder
   bound to its argument in that call: an argument value by its own type, a parameter
   default written as SQL by that SQL with each column it reads typed, a literal as
   itself; a boolean in a numeric position is read as its integer (per
   `aggregations/boolean-inputs`);
3. else, when the formula's type cannot be inferred (an unknown function), the type of
   its input, a boolean included; over `*`, INT.

The type SHALL be the same in the query plan, in a later stage's schema and in the SQL
facade's metric catalogue. A declared `datatype` SHALL be validated when the dataset is
saved and SHALL round-trip through storage and the Ossie export's SLayer extension.

#### Scenario: A count of trues is INT
- **WHEN** a model defines `trues` as `COUNT(CASE WHEN {value} THEN 1 END)` and a query selects `trues(orders.flag)` over a BOOLEAN `flag`
- **THEN** the measure is INT

#### Scenario: A share over a boolean is DOUBLE
- **WHEN** a model defines `share` as `SUM({value}) * 1.0 / COUNT(*)` and a query selects `share(orders.status = 'paid')`
- **THEN** the measure is DOUBLE

#### Scenario: A boolean-returning formula is BOOLEAN
- **WHEN** a model defines `all_true` as `BOOL_AND({value})` and a query selects it over a BOOLEAN column on Postgres
- **THEN** the measure is BOOLEAN

#### Scenario: An average over an INT is DOUBLE
- **WHEN** a model defines `my_avg` as `AVG({value})` and a query selects it over an INT column
- **THEN** the measure is DOUBLE, not INT

#### Scenario: Parameter types enter the inference
- **WHEN** a model defines `wsum` as `SUM({value} * {weight})` and queries select `wsum(orders.quantity, weight=orders.quantity)` over INT columns and `wsum(orders.quantity, weight=orders.rate)` with a DOUBLE `rate`
- **THEN** the measures are INT and DOUBLE respectively

#### Scenario: A declared datatype wins
- **WHEN** a model defines `trues` with `datatype: Float` and a query selects `trues(orders.flag)`
- **THEN** the measure is DOUBLE

#### Scenario: An unknown function falls back to the input type
- **WHEN** a model defines `custom_pick` as `MY_UDF({value})` and queries select it over a DATE column, over a BOOLEAN column and over `*`
- **THEN** the measures are DATE, BOOLEAN and INT respectively

#### Scenario: A model formula overriding a built-in is typed from its formula
- **WHEN** a model defines `sum` with formula `SUM({value}) / 2.0` and a query selects `sum(orders.quantity)` over an INT column
- **THEN** the measure is DOUBLE

#### Scenario: The facade agrees
- **WHEN** the SQL facade lists the metrics of a model with the `trues` and `my_avg` aggregations above
- **THEN** each metric's data type equals the type the query plan assigns the same column and aggregation

#### Scenario: An invalid declared datatype is refused at save
- **WHEN** a dataset is saved with an aggregation whose `datatype` is not a dataset field type
- **THEN** the save is refused with a validation error naming the aggregation
