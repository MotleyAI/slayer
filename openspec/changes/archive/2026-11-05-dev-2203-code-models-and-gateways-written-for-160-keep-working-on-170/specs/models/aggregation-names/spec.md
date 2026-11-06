## ADDED Requirements

### Requirement: Built-in aggregation names resolve exactly

A call whose aggregation name is a built-in aggregation in any case SHALL resolve to the owning model's definition named exactly that built-in's lowercase name, when there is one, and otherwise to the built-in — in every spelling (functional, colon, method), in queries, stored measures and saved queries. A model definition whose name differs from a built-in only by case SHALL NOT answer such a call, and SHALL NOT make it ambiguous; result keys SHALL keep the built-in's name. Case resolution of every other aggregation name is unchanged.

#### Scenario: A differently-cased built-in name is not an override
- **WHEN** model `t` defines the aggregation `Sum` with formula `SUM({value}) * 100` and a query asks for `sum(a)`, `SUM(a)`, `a:sum`, `t.sum(a)` and `Sum(a)`
- **THEN** each measure is the built-in sum of `a` under the result key `t.a_sum`

#### Scenario: Stored measures and saved queries keep the built-in
- **WHEN** the same model has the stored measure `total` = `SUM(a)` and a saved query asks for `a:sum`
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
