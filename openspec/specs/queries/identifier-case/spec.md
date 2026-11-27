# queries/identifier-case Specification

## Purpose
How Mode-B names match their declarations: unquoted identifiers and function names are case-insensitive, quoted identifiers match exactly, and names that differ only in case are kept apart at save, query and ingestion time.

## Requirements

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

### Requirement: The exact spelling wins, otherwise one match or a typed ambiguity
An unquoted name SHALL resolve to the declared name spelled exactly as written when one exists; otherwise to the unique declared name it matches case-insensitively. When several declared names match case-insensitively and none exactly, the reference SHALL fail with a typed ambiguity error naming every candidate and the remedy of quoting the intended name. The same rule SHALL apply to function names, which cannot be quoted.

#### Scenario: Exact spelling in a colliding model
- **WHEN** a stored model declares the columns `Revenue` and `revenue` and measures are `sum(Revenue)` and `sum(revenue)`
- **THEN** each sums its exactly-spelled column

#### Scenario: Ambiguous spelling
- **WHEN** the same model is queried with `sum(REVENUE)`
- **THEN** the query fails with the ambiguity error naming `Revenue` and `revenue` and suggesting `"Revenue"` or `"revenue"`

### Requirement: Declared spellings are canonical
Result keys, join aliases and result names SHALL carry the declared spelling of every name, whatever case the reference used, so spellings that differ only in case share one value and one key.

#### Scenario: Case-variant references share one key
- **WHEN** a query rooted at `orders` has measures `sum(AMOUNT)` and `sum(amount)`
- **THEN** they are one measure with key `orders.amount_sum`

#### Scenario: Dimension key uses the declared spelling
- **WHEN** a query rooted at `orders` has dimension `CUSTOMERS.REGION`
- **THEN** its result key is `customers.region`

### Requirement: Saves never introduce a case collision
A save of a model, datasource or saved query SHALL be refused with a typed error naming both names when it introduces two names in one namespace (model names in a datasource, a model's columns and saved measures, its join names, its model aggregations, the stage names of a query list, a query's result names) that are equal case-insensitively and are not both present in the persisted document of the same identity. A new document has no persisted collisions. A collision present in the persisted document SHALL keep loading and saving; it SHALL produce a structured warning naming both names on load, on save, and on every query response that reads the model.

#### Scenario: A new collision is refused
- **WHEN** a model declaring the column `revenue` is saved with an added saved measure `Revenue`
- **THEN** the save fails with the typed error naming `revenue` and `Revenue`

#### Scenario: A grandfathered collision keeps working
- **WHEN** a stored model already declares the columns `Revenue` and `revenue` and is saved unchanged, and again with an edited description
- **THEN** both saves succeed with the collision warning, and a query reading the model returns it among its warnings

#### Scenario: Removing and reintroducing a collision
- **WHEN** a grandfathered model is saved without `revenue`, and later saved with `revenue` added back
- **THEN** the first save succeeds and the second is refused as a new collision

#### Scenario: Model names in a datasource
- **WHEN** a datasource holds a model `orders` and a model `Orders` is created
- **THEN** the save fails with the typed error naming both models

### Requirement: A query's result names never collide by case
A query (or each query of a query list) whose result names, after resolution, include two that are equal case-insensitively but differ in spelling SHALL fail before SQL is generated, on every dialect, with a typed error naming both. Saved queries that already hold such names SHALL keep running.

#### Scenario: Two measures named by case
- **WHEN** a query has the measures `{"metric": "sum(amount)", "alias": "Total"}` and `{"metric": "count(*)", "alias": "total"}`
- **THEN** it fails with the typed error naming `Total` and `total`, on every dialect

### Requirement: Ingestion keeps case-only-distinct columns apart
When a table being ingested has physical columns whose names are equal case-insensitively, the first in physical column order SHALL keep its spelling as the logical name, and each later one SHALL get the smallest `<name>_<n>` (n ≥ 2) not equal case-insensitively to any persisted, incoming or already generated name, with `sql` naming the physical column, and a structured warning naming the physical column and the logical name. Re-ingesting the same table SHALL produce the same names.

#### Scenario: Two case variants
- **WHEN** a Postgres table with columns `"Foo"` and `foo` (in that order) is ingested
- **THEN** the model has columns `Foo` and `foo_2` (`sql` reading the physical `foo`), with a warning, and each returns its own physical column's values

#### Scenario: Suffix already taken
- **WHEN** a table with columns `Foo`, `foo` and `foo_2` is ingested, and then ingested again
- **THEN** the model has columns `Foo`, `foo_3` (reading `foo`) and `foo_2`, the same both times

### Requirement: A stored join target names its model's exact spelling
A stored join's `to` SHALL reach only the model of its datasource spelled exactly as written; it SHALL never carry a spelling that reaches a model only case-insensitively. Saving a model SHALL be refused with a typed error naming the model, the join, the written target and the declared spelling when a join target names no model of the datasource exactly but equals exactly one case-insensitively; when it equals several case-insensitively and none exactly, the save SHALL fail with the typed ambiguity error naming every candidate, also when the target is unchanged from the stored document. Saving a model SHALL be refused with a typed error naming the peer model, its join and the spelling to change when the model's name differs only in case from a join target of another model of its datasource; the remedy is to respell that join first. A target naming no model in any case SHALL still be accepted. Through a non-bypass access-filtered store, both checks SHALL read the caller's visible models, a rejection that only hidden models cause SHALL surface as the hidden-content conflict error naming no hidden model, and the case-only target error SHALL take precedence over the unknown-join-target error. On load, a stored model below schema version 25 SHALL have each join target that names no model exactly but equals exactly one stored model of its datasource case-insensitively (Unicode case folding) rewritten to that model's name before its join keys are repaired, and SHALL be persisted at the current version only by a save or `slayer storage upgrade`; an ambiguous or unmatched target SHALL be left unchanged. A query whose join walk follows a stored target that equals several models case-insensitively and none exactly SHALL fail with the typed ambiguity error naming every candidate.

#### Scenario: A case-only target is refused naming the declared spelling
- **WHEN** a datasource stores a model `Customers` and a model `orders` is saved with a join whose `to` is `customers`
- **THEN** the save fails with the typed error naming `orders`, the join, `customers` and `Customers`, and nothing is stored

#### Scenario: The target saved second is refused
- **WHEN** a model `orders` with a join to `customers` is stored while no model of that name exists, and a model `Customers` is then saved
- **THEN** the second save fails with the typed error naming `orders`, its join and the spelling `customers`, and saving `orders` with its join respelled to `Customers` first lets `Customers` save

#### Scenario: An ambiguous target is refused
- **WHEN** a datasource already stores both `Customers` and `CUSTOMERS` and a model `orders` is saved with a join to `customers`, including when its stored document already holds that join
- **THEN** the save fails with the ambiguity error naming `Customers` and `CUSTOMERS`

#### Scenario: A Unicode case fold is a case-only target
- **WHEN** a datasource stores a model `straße` and a model `orders` is saved with a join to `STRASSE`
- **THEN** the save fails with the typed error naming `STRASSE` and `straße`

#### Scenario: A dangling target is accepted
- **WHEN** a model `orders` is saved with a join to `suppliers` and no model of the datasource equals `suppliers` in any case
- **THEN** the save succeeds and the join is stored unchanged

#### Scenario: A stored case-only target is repaired on load
- **WHEN** a store written before schema version 25 holds `orders` joined to `customers` and a model `Customers`, and a query rooted at `orders` groups by `customers.name` and counts rows
- **THEN** `orders` loads with the join target `Customers`, the stored document is unchanged, the query returns one row per customer name, and a second load repairs it the same way

#### Scenario: Repair precedes join-key repair
- **WHEN** a store written before schema version 25 holds `orders` joined to `customers` through a physical key spelling that only the target's columns declare, and a model `Customers`
- **THEN** `orders` loads with the target `Customers`, its join keys rewritten to the target's column names and declared as on any legacy model, and a query through the join returns the joined rows on both the YAML and the SQLite store

#### Scenario: An ambiguous stored target fails closed at query time
- **WHEN** a store written before schema version 25 holds `orders` joined to `customers` and both `Customers` and `CUSTOMERS`, and a query rooted at `orders` reads `customers.name`
- **THEN** `orders` loads with the target unchanged and the query fails with the ambiguity error naming `Customers` and `CUSTOMERS`

#### Scenario: A hidden peer's case-only target is not revealed
- **WHEN** through an access-filtered store that hides `orders` (which joins `customers`), a caller saves a model `Customers`
- **THEN** the save fails with the hidden-content conflict error, which does not name `orders`
