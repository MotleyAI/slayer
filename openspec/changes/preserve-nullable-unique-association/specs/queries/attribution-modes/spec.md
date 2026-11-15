## MODIFIED Requirements

### Requirement: Per-query row identity for keyless homes
A home dataset whose model declares no primary key that needs association SHALL be
deduplicated by a per-query row identity, including when the model declares a standalone
unique column that may allow NULLs: each of its rows — after its model-level
filters, before any join — is told apart from every other, so each physical row counts
exactly once per cell. Two identical physical rows count as two rows; the copies a join
fan-out makes of one row count once. The identity is query-local, determines every
column of its own row, and never appears in results. It applies in every mode that
associates; an association over list children only keeps its key-free evaluation.

#### Scenario: Duplicate physical rows count separately, fan-out copies once
- **WHEN** a keyless `customers` model holds one customer's row twice, another customer
  has two `completed` orders, and a query rooted at `orders` omitting the mode selects
  `count(customers.*)` by `status`
- **THEN** each physical customer row counts once per status cell — the duplicated
  customer twice, the two-order customer once — by executed values on SQLite and DuckDB

#### Scenario: Fan-out under a joined filter still counts each row once
- **WHEN** a keyless home row reaches several rows of a joined dataset that a query
  filter reads, and the query associates an aggregate of that home by a dimension across
  the fanning hop
- **THEN** that home row counts once per cell it reaches, by executed values

#### Scenario: The row identity renders on every dialect
- **WHEN** a keyless-home association query is rendered for every supported dialect
- **THEN** each statement numbers the home rows with the dialect's accepted window
  syntax, executed on the in-process and locally runnable engines

#### Scenario: A nullable unique column preserves each associated row
- **WHEN** an aggregate associates over a fanning dimension, the home model has no primary
  key, and two real home rows have NULL in a standalone unique column
- **THEN** both home rows contribute once per cell they reach, even when a home row
  matches multiple joined rows in the cell, on SQLite and DuckDB

#### Scenario: A numbered home determines row-valued parameters
- **WHEN** the same home supplies both the value and weight for an associated weighted
  average
- **THEN** each cell's average uses the distinct home rows that reach it and their own
  weights, in default and explicit association modes
