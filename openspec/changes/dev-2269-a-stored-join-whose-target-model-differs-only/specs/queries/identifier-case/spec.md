## ADDED Requirements

### Requirement: A stored join target names its model's exact spelling
A stored join's `target_model` SHALL reach only the model of its datasource spelled exactly as written; it SHALL never carry a spelling that reaches a model only case-insensitively. Saving a model SHALL be refused with a typed error naming the model, the join, the written target and the declared spelling when a join target names no model of the datasource exactly but equals exactly one case-insensitively; when it equals several case-insensitively and none exactly, the save SHALL fail with the typed ambiguity error naming every candidate, also when the target is unchanged from the stored document. Saving a model SHALL be refused with a typed error naming the peer model, its join and the spelling to change when the model's name differs only in case from a join target of another model of its datasource; the remedy is to respell that join first. A target naming no model in any case SHALL still be accepted. Through a non-bypass access-filtered store, both checks SHALL read the caller's visible models, a rejection that only hidden models cause SHALL surface as the hidden-content conflict error naming no hidden model, and the case-only target error SHALL take precedence over the unknown-join-target error. On load, a stored model below schema version 22 SHALL have each join target that names no model exactly but equals exactly one stored model of its datasource case-insensitively (Unicode case folding) rewritten to that model's name before its join keys are repaired, and SHALL be written back at the current version; an ambiguous or unmatched target SHALL be left unchanged. A query whose join walk follows a stored target that equals several models case-insensitively and none exactly SHALL fail with the typed ambiguity error naming every candidate.

#### Scenario: A case-only target is refused naming the declared spelling
- **WHEN** a datasource stores a model `Customers` and a model `orders` is saved with a join whose `target_model` is `customers`
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
- **WHEN** a store written before schema version 22 holds `orders` joined to `customers` and a model `Customers`, and a query rooted at `orders` groups by `customers.name` and counts rows
- **THEN** `orders` loads with the join target `Customers`, is written back at the current version, the query returns one row per customer name, and a second load reads the stored document unchanged

#### Scenario: Repair precedes join-key repair
- **WHEN** a store written before schema version 22 holds `orders` joined to `customers` through a physical key spelling that only the target's columns declare, and a model `Customers`
- **THEN** `orders` loads with the target `Customers`, its join keys rewritten to the target's column names and declared as on any legacy model, and a query through the join returns the joined rows on both the YAML and the SQLite store

#### Scenario: An ambiguous stored target fails closed at query time
- **WHEN** a store written before schema version 22 holds `orders` joined to `customers` and both `Customers` and `CUSTOMERS`, and a query rooted at `orders` reads `customers.name`
- **THEN** `orders` loads with the target unchanged and the query fails with the ambiguity error naming `Customers` and `CUSTOMERS`

#### Scenario: A hidden peer's case-only target is not revealed
- **WHEN** through an access-filtered store that hides `orders` (which joins `customers`), a caller saves a model `Customers`
- **THEN** the save fails with the hidden-content conflict error, which does not name `orders`
