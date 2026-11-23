## ADDED Requirements

### Requirement: A document whose text repeats a mapping key is refused
A stored document whose YAML or JSON text repeats an explicit mapping key, at any depth, SHALL be unloadable with a typed error naming the document and every repeated key with its line; repeated YAML merge directives SHALL be allowed. A scalar the YAML loader cannot build SHALL make the document unloadable naming the value and its line. A save or edit whose text repeats a key SHALL be refused with the same reasons, storing nothing. An unloadable document SHALL follow `models/document-isolation`.

#### Scenario: A hand-placed document with a repeated key
- **WHEN** a file in `semantic_models/shop/` declares the key `source` twice in dataset `orders`
- **THEN** loading the datasource reports the document as unloadable naming `source` and its line, none of its datasets are visible, and the datasource's other documents load

#### Scenario: A save with a repeated key
- **WHEN** a document's text saved through SLayer repeats the key `name` in a field
- **THEN** the save fails with a typed error naming `name` and its line, and the stored document is unchanged

## MODIFIED Requirements

### Requirement: Entities SLayer cannot evaluate yet still load
A field or metric of a valid document whose expression SLayer cannot evaluate yet — a window function, a model-scoped metric reading a field unqualified or outside an aggregate, or an expression whose runnable form SLayer's expression reader cannot parse (for example a subquery) — SHALL load with a structured load warning naming the entity and the reason, visible on `inspect`, `datasets_summary` and search results, and reported by `slayer import-osi`. Every query reading the entity SHALL fail with a typed error naming it and the reason; no other entity SHALL be affected.

#### Scenario: A windowed field loads and fails only when read
- **WHEN** a document declares a field `running_total` with expression `SUM(amount) OVER (ORDER BY order_date)` beside a field `amount`
- **THEN** the document loads, `inspect` shows the warning on `running_total`, a query of `sum(sales.amount)` succeeds, and a query reading `sales.running_total` fails with a typed error naming `running_total`

#### Scenario: An expression SLayer cannot parse loads not evaluable
- **WHEN** a document declares a field `flag` with expression `(SELECT 1)` beside a field `amount`
- **THEN** the document loads, `inspect` shows a warning on `flag` naming the parse failure, `slayer import-osi` reports `flag` as not evaluable, a query of `sum(sales.amount)` succeeds, and a query reading `sales.flag` fails with a typed error naming `flag`

#### Scenario: SQL keywords inside literals stay evaluable
- **WHEN** a document declares a field `kind` with expression `CASE WHEN status IN ('select', 'from') THEN 'where' END`
- **THEN** `kind` loads with no load warning and a query reading it succeeds
