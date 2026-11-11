## MODIFIED Requirements

### Requirement: A model aggregation may take a library function name

A custom model aggregation SHALL be accepted when its name matches a Mode-B scalar or list function, or a function spelling the parser accepts (`strpos`, `date_trunc`, `json_extract`, …). Inside the model that defines it, a call whose value argument belongs to that model SHALL resolve to the aggregation in every spelling — functional (`reverse(amount)`) and method (`orders.reverse(amount)`), with or without arguments and parameters, in queries, stored measures, saved queries and memories — and SHALL keep resolving to the library function wherever the owning model does not define that name or the call has no owning model. A call is matched to the owning model's definitions exactly by case first, then by a unique case-insensitive match; several case-insensitive matches and no exact one SHALL be rejected as ambiguous, naming every candidate. A call that is not a valid library call and whose owning model does not define the name SHALL fail with the library function's own error.

#### Scenario: Stored custom aggregation named reverse answers

- **WHEN** a stored model `t` defines the aggregation `reverse` with formula `SUM({value})` and a query asks for `reverse(a)` and `t.reverse(a)`
- **THEN** the model loads and both measures return the sum of `a`

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

#### Scenario: A stored colon call to a shadowing aggregation still answers

- **WHEN** a stored model `t` defines the aggregation `reverse` and a stored measure written before the colon removal is `a:reverse`
- **THEN** it loads as `reverse(a)` and returns the custom aggregation's value

### Requirement: Built-in aggregation names resolve exactly

A call whose aggregation name is a built-in aggregation in any case SHALL resolve to the owning model's definition named exactly that built-in's lowercase name, when there is one, and otherwise to the built-in — in every spelling (functional, method), in queries, stored measures and saved queries. A model definition whose name differs from a built-in only by case SHALL NOT answer such a call, and SHALL NOT make it ambiguous; result keys SHALL keep the built-in's name. Case resolution of every other aggregation name is unchanged.

#### Scenario: A differently-cased built-in name is not an override
- **WHEN** model `t` defines the aggregation `Sum` with formula `SUM({value}) * 100` and a query asks for `sum(a)`, `SUM(a)`, `t.sum(a)` and `Sum(a)`
- **THEN** each measure is the built-in sum of `a` under the result key `t.a_sum`

#### Scenario: Stored measures and saved queries keep the built-in
- **WHEN** the same model has the stored measure `total` = `SUM(a)` and a saved query asks for `sum(a)`
- **THEN** both return the built-in sum of `a`

#### Scenario: An exact lowercase definition still overrides
- **WHEN** model `t` defines `sum` with formula `SUM({value}) * 100` and a query asks for `SUM(a)`
- **THEN** the measure is the model's `sum`

#### Scenario: Several case variants of a built-in
- **WHEN** model `t` defines `Sum` and `SUM` and a query asks for `sum(a)`
- **THEN** the measure is the built-in sum, with no ambiguity error

#### Scenario: Other names keep case resolution
- **WHEN** model `t` defines `upper` and `Total` and a query asks for `UPPER(a)` and `total(a)`
- **THEN** they resolve to `upper` and `Total`
