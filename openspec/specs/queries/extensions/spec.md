# queries/extensions Specification

## Purpose
Query-scoped extensions: per-stage columns, measures and joins added to any model a query stage reads, and the population meaning of a query-declared INNER join.

## Requirements

### Requirement: A query extends any model it reads

A query object SHALL accept `extensions`, a list of extensions each naming the model it extends (`source_name`) and carrying optional `columns`, `measures` and `joins`.
- An extension SHALL apply to the named model wherever the query object reads it: as its population, as a join target, or on any join path.
- Its columns and measures SHALL be addressable exactly like the model's own: by bare name from that model, and by dotted path from elsewhere.
- Its joins SHALL be edges of the query object's join graph, merged with stored edges by the join-traversal rules.
- A query object SHALL carry at most one extension per `source_name`; a second one SHALL be rejected at construction, naming the model.
- An extension object carrying any key other than the four SHALL be rejected at construction.
- Query refinements SHALL NOT accept `extensions`.

#### Scenario: Extending a joined model adds a dimension
- **WHEN** a query rooted at `orders` extends `customers` with a column `tier_label` and selects the dimension `customers.tier_label` and `sum(amount)`
- **THEN** the query executes, grouped by the new column, with the same values as the equivalent column stored on `customers`

#### Scenario: Extending a joined model adds a measure
- **WHEN** a query rooted at `orders` extends `customers` with a measure `credit_total` defined as `sum(credit)` and selects `customers.credit_total`
- **THEN** the value equals `sum(customers.credit)` computed over each customer once

#### Scenario: Extending the root
- **WHEN** a query sets `source_model` to `orders` and extends `orders` with a join to `regions`
- **THEN** it behaves exactly as the same extension written in the legacy root-extension form

#### Scenario: Duplicate extension of one model
- **WHEN** a query object carries two extensions of `customers`
- **THEN** construction fails naming `customers`

#### Scenario: Refinements carry no extensions
- **WHEN** a saved query is run with a `refine` object carrying `extensions`
- **THEN** the refinement is rejected as carrying an unexpected key

### Requirement: Extensions are scoped to their query object

An extension SHALL be visible only inside the query object that declares it.
- A later stage reading that stage SHALL see only the stage's output schema.
- Other stages SHALL NOT see the extension.
- Stored definitions SHALL NOT see it: stored joins, stored columns, saved measures, and the stages of a stored query-backed model.

An extension's `source_name` and each of its joins' `target_model` SHALL resolve, in order, to:
1. a sibling stage of the query list;
2. else the query object's own inline source model;
3. else a stored model of the query's datasource;
4. else fail with a model-not-found error.

#### Scenario: Extension invisible to the next stage
- **WHEN** a stage extends `customers` with a column it does not project, and a later stage of the same list rooted at `orders` references `customers.<column>`
- **THEN** the later stage fails with an unknown reference

#### Scenario: Stage shadows a same-named model
- **WHEN** a query list has a stage named `customers` and another stage extends `customers`
- **THEN** the extension applies to the stage's output, not to the stored model

#### Scenario: Unknown extension target
- **WHEN** a query extends a name that is no stage, inline source or stored model
- **THEN** it fails with a model-not-found error naming that name

### Requirement: Legacy root extensions convert

A query whose `source_model` is an extension object SHALL be converted to `source_model` naming the extension's base model, plus that extension as an `extensions` entry.
- A stored query (one carrying a version below the current one) SHALL be converted silently.
- A fresh query without a version, or a query constructed in Python with an extension instance as its source, SHALL be converted and SHALL emit a deprecation warning stating that the form is removed in 3.0 and naming `extensions` as its replacement.
- A query carrying the legacy form together with an `extensions` entry for the same model SHALL be rejected.
- A query at the current version carrying an extension object in `source_model` SHALL be rejected, naming `extensions`.
- The query's JSON schema and documentation SHALL NOT advertise the extension object for `source_model`.

#### Scenario: Fresh legacy input converts with a deprecation warning
- **WHEN** a query is submitted without a version with `source_model` = `{"source_name": "orders", "joins": [...]}`
- **THEN** it executes with `source_model` `orders` and one extension of `orders`, and a deprecation warning names `extensions` and removal in 3.0

#### Scenario: Legacy form conflicting with an extensions entry
- **WHEN** a fresh query carries `source_model` = `{"source_name": "orders", ...}` and also an `extensions` entry for `orders`
- **THEN** construction fails naming `orders`

#### Scenario: Current-version query cannot use the legacy form
- **WHEN** a query carrying the current version has an extension object in `source_model`
- **THEN** construction fails with an error naming `extensions`

#### Scenario: Internal producers use the current form
- **WHEN** column profiling or the BI facade builds a query that extends a model
- **THEN** no deprecation warning is emitted

### Requirement: Query-declared INNER joins restrict the population

Each INNER join declared by an extension of model `M` SHALL restrict the query object's population as the field filter "the join's target is matched".
- The filter SHALL be read from the population along the path a reference to `M` written in the same query object resolves to.
- It SHALL be evaluated like any field filter: inline on a provably to-one path, and by association (existence) across a fanning or unproven path.
- It SHALL never multiply rows, and it SHALL NOT change when measures are added or removed.
- It SHALL compose over multi-hop chains whatever the earlier hops' join types, including query-local edges and stage or query-backed targets, and it SHALL apply in raw-row mode.
- A declared join type SHALL NOT change how the edge is traversed: a stored edge keeps its stored type, and a new query-local edge is traversed as LEFT.
- A LEFT declaration SHALL NOT restrict. A stored INNER edge SHALL keep restricting when traversed, as before. An INNER declaration that is a stored INNER edge SHALL still add its restriction.
- When the population does not reach `M` by any route, the query SHALL fail, naming the INNER declaration. When the reference to `M` is ambiguous, the query SHALL fail with the ambiguity error, naming the INNER declaration.
- In a query without `source_model`, population inference SHALL read the restriction as a field filter.

#### Scenario: INNER on the root keeps matched rows only
- **WHEN** a query rooted at `customers` extends `customers` with an INNER join to `vip_list` and selects `customers.name`
- **THEN** only customers with a `vip_list` match are returned, with no duplicates for customers with several matches

#### Scenario: INNER over a stored LEFT edge restricts without a parallel edge
- **WHEN** a query rooted at `orders` extends `orders` with an INNER join to `customers` on the stored LEFT edge's pairs, named `c`, and selects `c.name` and `customers.region`
- **THEN** orders without a customer are excluded, `customers` is joined once, and both references read the same rows

#### Scenario: INNER across a fanning path restricts by existence
- **WHEN** a query rooted at `customers` extends `orders` with an INNER join to `vip_orders` and selects `customers.name` and `count(orders.id)`
- **THEN** only customers having at least one order with a `vip_orders` match are returned, and `count(orders.id)` counts those matching orders, each once

#### Scenario: Multi-hop chain through a LEFT hop
- **WHEN** rooted at `orders`, the query extends `customers` with an INNER join to `regions` while `orders → customers` is a stored LEFT edge
- **THEN** exactly the orders whose customer has a matching region are kept

#### Scenario: Adding a measure never changes the rows
- **WHEN** a query with a query-declared INNER join is run with and without an extra measure over another model
- **THEN** both results have the same rows and the same values for the shared columns

#### Scenario: INNER to a stage target
- **WHEN** a stage extends `customers` with an INNER join to a sibling stage `big_spenders` on the customer key
- **THEN** only customers present in `big_spenders` are kept

#### Scenario: Target model filters count as unmatched
- **WHEN** the INNER target model's own filters exclude every row a population row would match
- **THEN** that population row is excluded

#### Scenario: Composite keys with partial NULLs
- **WHEN** an INNER join has two key pairs and a population row's key holds NULL in one component
- **THEN** that row is excluded, in either declared orientation

#### Scenario: Unreachable INNER declaration fails
- **WHEN** a query rooted at `orders` extends `events`, which no path reaches from `orders`, with an INNER join to `users`
- **THEN** the query fails, naming the INNER join on `events`

#### Scenario: LEFT declaration never restricts
- **WHEN** the same query declares the join as LEFT
- **THEN** every population row is kept

#### Scenario: Rootless query reads the restriction
- **WHEN** a query without `source_model` selects `customers.name` and extends `customers` with an INNER join to `vip_list`
- **THEN** the inferred population and its restriction equal those of the same query rooted at `customers`

### Requirement: Rejected extension targets

An extension of the time spine SHALL be rejected with a typed error naming the time spine.

#### Scenario: Extending the time spine
- **WHEN** a query extends `time_spine`
- **THEN** it fails with a typed error naming `time_spine`

### Requirement: Stored stages with failing extensions are refused at save

Saving a query-backed model SHALL be refused, naming the stage and the extension, when one of its stages carries an extension that every query reading the model would fail on: an unresolvable `source_name` or join target, an edge-merge conflict, or an INNER declaration the stage's population cannot reach.

#### Scenario: Save refuses an edge conflict
- **WHEN** a query-backed model is saved whose stage extends `orders` with a join named like an existing `orders` edge but on different key pairs
- **THEN** the save fails, naming the stage and the join
