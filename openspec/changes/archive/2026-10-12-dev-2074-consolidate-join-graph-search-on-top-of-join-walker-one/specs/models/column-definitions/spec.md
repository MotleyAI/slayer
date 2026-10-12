## ADDED Requirements

### Requirement: Revisits are frame-local
A revisit SHALL be judged within one spelled path alone: a query-typed path from the query root, and a
definition — a derived column's `sql` or `filter`, a model filter, a saved measure's formula — from its
declaring model. Composing a definition onto a path SHALL never be a revisit: each occurrence of a model on
a composed path is a dataset of its own, identified by its path from the query root, and SHALL keep its own
join, type, format, home and time-dimension ownership. A path typed in one frame that revisits a model SHALL
still be refused with the circular error.

#### Scenario: A derived column composes across a cycle
- **WHEN** `orders` joins `customers` by the many-to-one edge `customer`, `customers` joins `orders` by the
  many-to-one edge `last_order`, `customers.last_amount` has `sql` `last_order.amount`, and a query rooted at
  `orders` selects the dimensions `id` and `customer.last_amount` over orders `11` and `12` of a customer whose
  latest order is `12` (amount `20`) and order `21` (amount `5`) of a customer whose latest order is `21`
- **THEN** the rows are `(11, 20)`, `(12, 20)` and `(21, 5)`, the latest order is joined as its own occurrence
  of `orders`, and `customer.last_amount` carries the type and format of `customers.last_amount`

#### Scenario: The same chain typed in one frame is refused
- **WHEN** the same query selects `customer.last_order.amount` instead
- **THEN** it fails with the circular error naming the hop `last_order` and the revisited `orders`

#### Scenario: Aggregating a composed derived column keeps its home
- **WHEN** the query aggregates `max(customer.last_amount)` grouped by `customer.name`
- **THEN** the aggregate is homed on `customers` and attributed per customer (`20` and `5`) with no broadcast
  warning

#### Scenario: Occurrences reached through different edges stay distinct
- **WHEN** `customers` also joins `orders` by a many-to-one edge `first_order` with
  `customers.first_amount` defined as `first_order.amount`, and a query rooted at `orders` selects the
  dimensions `amount`, `customer.first_amount` and `customer.last_amount`
- **THEN** the root, `customer.first_order` and `customer.last_order` are three occurrences of `orders`,
  each with its own join, and each value is read from its own occurrence

### Requirement: An ambiguous hop in a definition is rejected at save time
When a model is saved through the storage backend or through the engine, a reference in its own derived
`Column.sql`, `Column.filter` or model filters whose join path from the declaring model crosses a hop spanned
by two or more edges that the token does not disambiguate SHALL be rejected with an error naming the
column (or the model filter), its declaring model, the complete reference as spelled, and every candidate
edge, plus the remedy to spell the hop by an edge name. Ambiguity and revisits are judged in path order, and
both take precedence over a fanning verdict on the same path. The engine's save door and the storage door
SHALL reach the same verdict.

#### Scenario: An ambiguous derived reference is rejected at both doors
- **WHEN** `orders` declares two unnamed joins to `addresses` and is saved with a derived column `city`
  whose `sql` is `addresses.city`
- **THEN** the save is rejected, through the storage backend and through the engine alike, with the
  ambiguous-hop error naming `city`, `orders`, `addresses.city` and both candidate edges

#### Scenario: A revisit after an ambiguous hop is no longer missed
- **WHEN** a derived reference's path crosses an ambiguous hop before a hop that revisits a model
- **THEN** the save is rejected with the ambiguous-hop error, never accepted

#### Scenario: A named hop through the same pair saves
- **WHEN** the two `orders`→`addresses` edges are named `billing_address` and `shipping_address` and the
  derived column's `sql` is `billing_address.city`
- **THEN** the save succeeds

### Requirement: An edge-set change warns about the definitions it breaks
When a save changes a model's join edges relative to its stored version, every other model's derived
`Column.sql`, `Column.filter` and model filter in the datasource whose reference resolved before the save and
no longer resolves after it — because a hop became ambiguous or an edge it crossed was removed or renamed —
SHALL produce one warning naming that model, its column or model filter, the reference, the affected pair,
and the remedy. The save SHALL succeed. A save that does not change the model's join edges SHALL NOT
re-examine other models.

#### Scenario: A new parallel edge warns about a dependent derived column
- **WHEN** `shipments.city` has `sql` `orders.addresses.city` while `orders`↔`addresses` has one unnamed edge,
  and `orders` is then saved with a second unnamed join to `addresses`
- **THEN** the save succeeds with a warning naming `shipments`, `city`, `orders.addresses.city`, the pair
  `orders`↔`addresses` and the remedy to spell the hop by an edge name

#### Scenario: A removed edge warns about a dependent derived column
- **WHEN** `orders` is saved without its join to `addresses`
- **THEN** the save succeeds with a warning naming `shipments.city` as no longer resolvable

#### Scenario: An unchanged edge set re-examines nothing
- **WHEN** `orders` is saved with only a column change
- **THEN** no other model's definitions are re-examined and no dependent warning is raised

## MODIFIED Requirements

### Requirement: Unproven references are accepted with a save-time warning and a query-time backstop

When a derived `Column.sql` / `Column.filter` reference's path crosses a hop
that is neither provably many-to-one nor provably fanning — an undeclared hop,
or a hop whose only proof is that its reverse orientation covers the source's
unique key — the model save SHALL succeed and SHALL emit a warning naming the
column and the hop, once per column and hop however many of the column's
references cross it (a `sql` and a `filter` crossing the same hop warn once). Such a column is refused at query time by the input-safety
gate when it is aggregated as a column of its declaring model, and equally when
it ranks a `first`/`last` as the model's `default_time_dimension` (the permanent
backstop). A reference whose target model is not loaded or does not resolve SHALL be skipped at save time
(no error, no warning), leaving the query-time gate as the sole check; an ambiguous reference is rejected
(see "An ambiguous hop in a definition is rejected at save time").

#### Scenario: reverse-PK-covered undeclared hop warns and saves
- **WHEN** a model with an undeclared join whose reverse orientation covers the source's primary key is saved with a derived column referencing the target
- **THEN** the save succeeds and a warning names the column and the hop

#### Scenario: fully undeclared hop warns and saves
- **WHEN** a model with an undeclared join, provable in neither orientation, is saved with a derived column referencing the target
- **THEN** the save succeeds and a warning names the column and the hop

#### Scenario: sql and filter across the same unproven hop warn once
- **WHEN** a model with an undeclared join is saved with a column whose `sql` and `filter` both reference the join's target
- **THEN** the save succeeds and exactly one warning names the column and the hop, with no `sql`/`filter` label

#### Scenario: aggregating an unproven-hop column is still refused at query time
- **WHEN** a saved derived column across an unproven hop is aggregated as a column of its declaring model
- **THEN** the query fails closed with the input-safety error naming the hop and the cross-model remedy

#### Scenario: an unloaded target is skipped at save time
- **WHEN** a derived column references a target model that is not loaded at save time
- **THEN** the save succeeds with no error and no warning, and the query-time gate remains the only check

#### Scenario: a default_time_dimension across an unproven hop is refused when it ranks a first/last
- **WHEN** a model's `default_time_dimension` names a saved derived column across an unproven hop and a query selects `last(amount)` on that model with no explicit ranking column, temporal dimension or time dimension
- **THEN** the query fails closed with the input-safety error naming the column and the hop, exactly as the explicit `last(amount, <column>)` spelling does
