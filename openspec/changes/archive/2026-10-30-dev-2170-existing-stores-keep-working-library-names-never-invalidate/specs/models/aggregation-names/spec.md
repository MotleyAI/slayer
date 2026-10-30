## ADDED Requirements

### Requirement: A model aggregation may take a library function name

A custom model aggregation SHALL be accepted when its name matches a Mode-B scalar or list function, or a function spelling the parser accepts (`strpos`, `date_trunc`, `json_extract`, …). Inside the model that defines it, a call whose value argument belongs to that model SHALL resolve to the aggregation in every spelling — functional (`reverse(amount)`), colon (`amount:reverse`) and method (`orders.reverse(amount)`), with or without arguments and parameters, in queries, stored measures, saved queries and memories — and SHALL keep resolving to the library function wherever the owning model does not define that name or the call has no owning model. A call is matched to the owning model's definitions exactly by case first, then by a unique case-insensitive match; several case-insensitive matches and no exact one SHALL be rejected as ambiguous, naming every candidate. A call that is not a valid library call and whose owning model does not define the name SHALL fail with the library function's own error.

#### Scenario: Stored custom aggregation named reverse answers

- **WHEN** a stored model `t` defines the aggregation `reverse` with formula `SUM({value})` and a query asks for `reverse(a)`, `a:reverse` and `t.reverse(a)`
- **THEN** the model loads and all three measures return the sum of `a`

#### Scenario: Another model keeps the library function

- **WHEN** `orders` defines the aggregation `reverse` and a query on `orders` asks for the dimension `reverse(customers.name)` where `customers` does not define it
- **THEN** the dimension is the reversed customer name

#### Scenario: A literal argument keeps the library function

- **WHEN** a model defining the aggregation `reverse` is queried for `reverse('abc')`
- **THEN** the value is `'cba'`

#### Scenario: A parser spelling is shadowed by its own name

- **WHEN** a model defines the aggregation `strpos` and a query asks for `strpos(a)`
- **THEN** the measure is the custom aggregation, and the shadow warning names `strpos`

#### Scenario: Parameters that the library function would refuse

- **WHEN** a model defines the aggregation `left` with a parameter `n` and a query asks for `left(a, n=2)`
- **THEN** the measure is the custom aggregation with `n = 2`

#### Scenario: Case resolution

- **WHEN** a model defines the aggregation `reverse` and a query asks for `REVERSE(a)`
- **THEN** the measure is the custom aggregation

#### Scenario: Ambiguous case-insensitive match

- **WHEN** a model defines the aggregations `Reverse` and `REVERSE` and a query asks for `reverse(a)`
- **THEN** the query is rejected with an error naming `Reverse` and `REVERSE`

#### Scenario: An undefined name keeps the library error

- **WHEN** a model that does not define `left` is queried for `left(a, n=2)`
- **THEN** the query fails with the same error the library function gives for a keyword argument

### Requirement: Shadowing a library function is announced every time

Saving or loading a model that defines an aggregation shadowing a library function, and every query response in which a call resolved to such an aggregation instead of the library function, SHALL carry a structured warning naming the model, the aggregation and the shadowed function, both as a Python warning and on the response's warnings. This SHALL hold for executed, dry-run and EXPLAIN queries and for a response served from the result cache. A formula-backed definition of a built-in aggregation SHALL NOT carry this warning.

#### Scenario: Warning on every response

- **WHEN** a model defining the aggregation `reverse` is saved, reloaded, and queried for `reverse(a)` twice with caching on
- **THEN** the save, the load and both responses carry the shadow warning naming the model, `reverse` and the library function

#### Scenario: No warning without a shadowing call

- **WHEN** the same model is queried for `sum(a)`
- **THEN** the response carries no shadow warning

### Requirement: Transform and date-syntax names stay reserved

A custom model aggregation SHALL be rejected at model validation when its name is a transform that is not a built-in aggregation, or a date-part alias or `extract`.

#### Scenario: Aggregation named cumsum rejected

- **WHEN** a model defines a custom aggregation named `cumsum`
- **THEN** model validation fails with an error naming the reserved transform names
