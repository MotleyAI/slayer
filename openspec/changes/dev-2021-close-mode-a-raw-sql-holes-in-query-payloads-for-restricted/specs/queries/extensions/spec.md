## ADDED Requirements

### Requirement: Queries carry no Mode A

No field of a query object SHALL be read as SQL of the datasource's dialect, and no field SHALL name a physical table or column directly. Every expression a query carries SHALL be Mode B, bound through the models it reads. Mode A SHALL be authored only in a stored model's definition. A value a query supplies to a stored model's Mode A SHALL enter it only as a literal. This holds for every caller.
- An extension carrying `columns` SHALL be rejected at construction (`queries/extensions` › A query extends any model it reads).
- A `source_model` that is an object without `source_name` SHALL be rejected at construction (`queries/population` › A population is named, and typed at construction).
- A query variable SHALL bind into a stored model's Mode-A text only as a literal (`queries/variables` › Mode-A surfaces bind variables as literals).
- A query-side string aggregation argument SHALL be read as Mode B (`aggregations/formula-templates` › Aggregation parameters bind once).

#### Scenario: Each route is refused
- **WHEN** queries over a datasource holding the unmodelled table `secrets` submit, in turn:
  - an extension column whose `sql` is `(SELECT group_concat(pw) FROM secrets)`;
  - an extension column whose `filter` is `EXISTS (SELECT 1 FROM secrets)`;
  - a sql-less extension column naming a physical column the model does not declare;
  - an inline `source_model` with `sql_table: secrets`, and one with `sql: SELECT pw FROM secrets`;
  - `variables` binding `(SELECT count(*) FROM secrets)` into a stored `Column.sql` `amount * {k}`;
  - `weighted_avg(amount, weight='length(sqlite_version())')`
- **THEN** each fails with a typed error before any SQL executes, and no value derived from `secrets` or the database's own functions is returned

#### Scenario: The Mode-B spellings still run
- **WHEN** the same queries use an expression dimension, a literal variable bound into the stored `amount * {k}`, and `weighted_avg(amount, weight='id * 2')`
- **THEN** each executes with the hand-computed values, on SQLite and DuckDB

## MODIFIED Requirements

### Requirement: A query extends any model it reads

A query object SHALL accept `extensions`, a list of extensions each naming the model it extends (`source_name`) and carrying optional `measures` and `joins`.
- An extension SHALL apply to the named model wherever the query object reads it: as its population, as a join target, or on any join path.
- Its measures SHALL be addressable exactly like the model's own: by bare name from that model, and by dotted path from elsewhere.
- Its joins SHALL be edges of the query object's join graph, merged with stored edges by the join-traversal rules.
- A query object SHALL carry at most one extension per `source_name`; a second one SHALL be rejected at construction, naming the model.
- An extension object carrying any key other than the three SHALL be rejected at construction; `columns` is rejected with an error stating that extension columns were removed and that a per-query column is written as an expression in the position that uses it.
- Query refinements SHALL NOT accept `extensions`.

#### Scenario: Extension columns are refused
- **WHEN** a query rooted at `orders` extends `customers` with `columns` = `[{"name": "tier_label", "sql": "upper(tier)"}]`, in `extensions` or in the legacy `source_model` form
- **THEN** construction fails with an error stating that extension columns were removed and that the expression `upper(customers.tier)` is written where it is used, and nothing executes

#### Scenario: The expression replaces the column
- **WHEN** the same query instead selects the dimension `{"expression": "upper(customers.tier)", "name": "tier_label"}` and `sum(amount)`
- **THEN** it executes, grouped by the upper-cased tier

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
2. else a stored model of the query's datasource;
3. else fail with a model-not-found error.

#### Scenario: Extension invisible to the next stage
- **WHEN** a stage extends `customers` with a measure it does not project, and a later stage of the same list rooted at `orders` references `customers.<measure>`
- **THEN** the later stage fails with an unknown reference

#### Scenario: Stage shadows a same-named model
- **WHEN** a query list has a stage named `customers` and another stage extends `customers`
- **THEN** the extension applies to the stage's output, not to the stored model

#### Scenario: Unknown extension target
- **WHEN** a query extends a name that is no stage or stored model
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
- **WHEN** the BI facade builds a query that extends a model
- **THEN** no deprecation warning is emitted
