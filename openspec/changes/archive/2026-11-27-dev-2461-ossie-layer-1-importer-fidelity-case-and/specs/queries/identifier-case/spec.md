## MODIFIED Requirements

### Requirement: Unquoted names fold, quoted names match exactly
An unquoted identifier SHALL match a declared name case-insensitively (Unicode case folding).

A double-quoted identifier SHALL match the declared name spelled exactly as written. When no declared name is spelled exactly so, it SHALL match the declared name whose Ossie normalized form equals the quoted text. The normalized form of a declared name is:
- the name upper-cased, when it can be written unquoted as the second part of a reference (as `order_id`, `été` and `from` can); a keyword that cannot open a reference unquoted (such as `alter`) is upper-cased too;
- the name as written, otherwise.

When several declared names have that normalized form and none is spelled exactly, the reference SHALL fail with the typed ambiguity error naming every candidate.

Scope:
- These rules SHALL hold in every namespace: model names in a datasource, stage names in a query list (where a stage overrides a model whose name it matches), a model's columns and saved measures (one namespace), join names, model aggregations, and a query's result names (measure and dimension names).
- They SHALL hold for native models and Ossie documents alike, and under strict (`ossie-l3`) binding.
- Quoting SHALL apply per path segment: in `"Orders".amount` the first segment is quoted and the second unquoted.
- Function names (scalar functions, aggregations, transforms, date-part aliases and `date_trunc` units) SHALL match case-insensitively.
- Mode-A SQL, physical source-column reads (`source."x"` and a row definition's bare source column) and `{variable}` names SHALL be unaffected.
- Stored expression text SHALL keep the author's quoting.

#### Scenario: Any case reaches a column
- **WHEN** a query rooted at `orders` has dimension `ORDERS.Status` and measure `SUM(Amount)`, and the model declares `status` and `amount`
- **THEN** it groups by `status` and sums `amount`

#### Scenario: Quoted names are exact
- **WHEN** model `orders` declares the column `amount` and a measure is `sum("Amount")`
- **THEN** the query fails with the unresolved-reference error, and `sum("amount")` binds

#### Scenario: Quoting per segment
- **WHEN** a query rooted at `orders` joined to `customers` (declaring `region`) has dimensions `"customers".REGION` and `CUSTOMERS."region"`
- **THEN** both resolve to `customers.region`, while `"Customers".region` fails as an unresolved reference

#### Scenario: Join, model and stage names fold
- **WHEN** a query list has a stage `Monthly` and its main query is rooted at `monthly`, and another query rooted at `orders` reads `CUSTOMERS.region` through the join named `customers`
- **THEN** the first reads the stage and the second walks the join

#### Scenario: Transform names fold
- **WHEN** a measure is `RANK(SUM(amount), direction = 'desc')` and another `CumSum(sum(amount))` in a monthly query
- **THEN** they equal `rank(sum(amount), direction = 'desc')` and `cumsum(sum(amount))`

#### Scenario: A quoted name in Ossie's normalized form matches
- **WHEN** model `orders` declares the column `amount`, and a query rooted at `orders` has dimension `"ORDERS"."STATUS"` and measure `sum("AMOUNT")`
- **THEN** it groups by `orders.status` and sums `amount`, with result keys `orders.status` and `orders.amount_sum`, exactly as `sum(amount)` grouped by `orders.status` does

#### Scenario: The exact spelling beats the normalized form
- **WHEN** a stored model declares the columns `AMOUNT` and `amount`, and the measures are `sum("AMOUNT")` and `sum("amount")`
- **THEN** each sums its exactly spelled column

#### Scenario: Two names with one normalized form are ambiguous
- **WHEN** a stored model declares the columns `Revenue` and `revenue`, and a measure is `sum("REVENUE")`
- **THEN** the query fails with the ambiguity error naming `Revenue` and `revenue`

#### Scenario: A name that must be quoted has no other form
- **WHEN** model `orders` declares the column `order total`, and the measures are `sum("order total")` and `sum("ORDER TOTAL")`
- **THEN** the first sums `order total` and the second fails with the unresolved-reference error

#### Scenario: Normalized forms follow Ossie's validator
- **WHEN** the declared names `order_id`, `été`, `from`, `alter`, `order total`, `a.b` and `say "hi"` are normalized
- **THEN** each normalized form equals the one upstream `validation/validate.py` computes for that declared name

#### Scenario: Strict binding uses the same rule
- **WHEN** a query bound with `conformance="ossie-l3"` has dimension `"CUSTOMERS".region` over a document dataset `customers`, and another has dimension `"Customers".region`
- **THEN** the first resolves to `customers.region`, and the second fails with `E_NAME_NOT_FOUND`

#### Scenario: A source column keeps the database's quoting
- **WHEN** a document field's expression reads the source column `"ID"` and the source table's column is `id`
- **THEN** the field reads whatever the database itself resolves `"ID"` to, with no rewriting by SLayer

#### Scenario: A quoted reference round-trips with its quotes
- **WHEN** a document whose dataset metric is `SUM("AMOUNT")` is imported, loaded, saved unchanged and exported, on the YAML and the SQLite store, and a native dataset metric `sum("AMOUNT")` is saved and read back
- **THEN** the document's text is unchanged byte for byte, the native metric's stored text still reads `sum("AMOUNT")`, and both return the sum of `amount`
