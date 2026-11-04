## ADDED Requirements

### Requirement: A model aggregation may take a library function name

A custom model aggregation SHALL be accepted when its name matches a Mode-B scalar or list function. Inside the model that defines it, a call whose value argument belongs to that model SHALL resolve to the aggregation in every spelling — functional (`upper(amount)`), colon (`amount:upper`) and method (`orders.upper(amount)`), with or without arguments and parameters, in queries, stored measures, saved queries and memories — and SHALL keep resolving to the library function wherever the owning model does not define that name or the call has no owning model. A call is matched to the owning model's definitions exactly by case first, then by a unique case-insensitive match; several case-insensitive matches and no exact one SHALL be rejected as ambiguous, naming every candidate. A call that is not a valid library call and whose owning model does not define the name SHALL fail with the library function's own error.

#### Scenario: Stored custom aggregation named like a function answers

- **WHEN** a stored model `t` defines the aggregation `upper` with formula `SUM({value})` and a query asks for `upper(a)`, `a:upper` and `t.upper(a)`
- **THEN** the model loads and all three measures return the sum of `a`

#### Scenario: Stored custom aggregation named like a list function answers

- **WHEN** a model stored by the previous release defines the aggregation `contains` with formula `SUM({value})` and a query asks for `contains(a)`
- **THEN** the model loads and the measure returns the sum of `a`

#### Scenario: Another model keeps the library function

- **WHEN** `orders` defines the aggregation `upper` and a query on `orders` asks for the dimension `upper(customers.name)` where `customers` does not define it
- **THEN** the dimension is the upper-cased customer name

#### Scenario: A literal argument keeps the library function

- **WHEN** a model defining the aggregation `upper` is queried for `upper('abc')`
- **THEN** the value is `'ABC'`

#### Scenario: Parameters that the library function would refuse

- **WHEN** a model defines the aggregation `round` with a parameter `n` and a query asks for `round(a, n=2)`
- **THEN** the measure is the custom aggregation with `n = 2`

#### Scenario: Case resolution

- **WHEN** a model defines the aggregation `upper` and a query asks for `UPPER(a)`
- **THEN** the measure is the custom aggregation

#### Scenario: Ambiguous case-insensitive match

- **WHEN** a model defines the aggregations `Upper` and `UPPER` and a query asks for `upper(a)`
- **THEN** the query is rejected with an error naming `Upper` and `UPPER`

#### Scenario: An undefined name keeps the library error

- **WHEN** a model that does not define `round` is queried for `round(a, n=2)`
- **THEN** the query fails with the same error the library function gives for a keyword argument

### Requirement: Shadowing a library function is announced every time

Saving or loading a model that defines an aggregation shadowing a library function, and every query response in which a call resolved to such an aggregation instead of the library function, SHALL carry a structured warning naming the model, the aggregation and the shadowed function, both as a Python warning and on the response's warnings. This SHALL hold for executed, dry-run and EXPLAIN queries and for a response served from the result cache, over the Python API, REST and MCP. A formula-backed definition of a built-in aggregation SHALL NOT carry this warning.

#### Scenario: Warning on every response

- **WHEN** a model defining the aggregation `upper` is saved, reloaded, and queried for `upper(a)` twice with caching on
- **THEN** the save, the load and both responses carry the shadow warning naming the model, `upper` and the library function

#### Scenario: Warning on dry run and over REST and MCP

- **WHEN** the same query runs as a dry run, through the REST API and through the MCP `query` tool
- **THEN** each response carries the shadow warning

#### Scenario: No warning without a shadowing call

- **WHEN** the same model is queried for `sum(a)`
- **THEN** the response carries no shadow warning

### Requirement: Transform and date-syntax names stay reserved

A custom model aggregation SHALL be rejected at model validation when its name is a transform that is not a built-in aggregation, or a date-part alias or `extract`.

#### Scenario: Aggregation named cumsum rejected

- **WHEN** a model defines a custom aggregation named `cumsum`
- **THEN** model validation fails with an error naming the reserved transform names
