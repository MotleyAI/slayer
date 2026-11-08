## MODIFIED Requirements

### Requirement: A derived column may reference only provably to-one targets

A derived `Column.sql` or `Column.filter` reference SHALL be a function of its
declaring model's row. When such a reference's join path from the declaring
model provably crosses a fanning hop — a hop that is not provably many-to-one
and whose declared cardinality in its traversal orientation is `one_to_many` or
`many_to_many` — the model save SHALL be rejected with an error naming the
column, the hop, and the remedy (aggregate the target column as `<hop>.<column>:<aggregation>`,
or filter by it; declare a to-one cardinality or a covering unique key if the
hop is really to-one). A hop that is provably many-to-one — its declared
cardinality is `many_to_one`/`one_to_one`, or its target-side join columns cover
a unique key of the target — SHALL be accepted with no error and no warning,
even if its declared cardinality is contradictorily `one_to_many` (the proof
takes precedence over the declaration).

Such a reference SHALL never revisit a model already on its join path from the
declaring model — the declaring model itself included. A revisiting path is circular: the model save SHALL be
rejected with a circular-definition error naming the column, the kind (`sql` or
`filter`), the complete reference as spelled, the revisited model, the hop that
revisits it and the model that hop leaves, plus the remedy (reference the column
on the revisited model directly if this row's value is meant, or declare the
aggregate on the model the hop leaves, which reaches the revisited model
forward). The circular rejection takes precedence over a fanning verdict on an
earlier hop of the same path, and applies identically whether the model is saved
through the storage backend or through the engine. A revisit is judged once
every model on the path up to it is loaded: a known-but-unloaded intermediate
model leaves the reference skipped at save time, as for any unloaded target,
with the query-time door refusing it as unresolvable.

#### Scenario: sql reference across a declared one-to-many hop is rejected
- **WHEN** a model with a join declared `one_to_many` to a loaded target is saved with a derived column whose `sql` references a column on that target
- **THEN** the save is rejected with an error naming the column, the hop, and the aggregate-or-filter remedy

#### Scenario: filter reference across a declared one-to-many hop is rejected
- **WHEN** a model with a join declared `one_to_many` to a loaded target is saved with a column whose `filter` references a column on that target
- **THEN** the save is rejected with the same error

#### Scenario: to-one reference is accepted silently
- **WHEN** a model is saved with a derived column referencing a target across a hop that is provably many-to-one (declared `many_to_one`/`one_to_one`, or the target-side join columns cover a unique key)
- **THEN** the save succeeds with no error and no warning

#### Scenario: proof beats a contradictory to-many declaration
- **WHEN** a hop is declared `one_to_many` but its target-side join columns cover a unique key of the target
- **THEN** the hop is treated as provably to-one and a derived column crossing it is accepted with no error

#### Scenario: sql reference that revisits a model on its path is rejected
- **WHEN** `customers` (joined to-one to `regions`) is saved with a derived column whose `sql` is `regions.customers.spend`
- **THEN** the save is rejected with the circular-definition error naming the column, kind `sql`, reference `regions.customers.spend`, revisited model `customers`, hop `customers` leaving `regions`, and the remedy

#### Scenario: filter reference that revisits a model on its path is rejected
- **WHEN** `customers` is saved with a column whose `filter` is `regions.customers.spend > 0`
- **THEN** the save is rejected with the same error naming kind `filter`

#### Scenario: a revisit declared on the querying model is rejected
- **WHEN** `orders` (joined to-one to `customers`, itself joined to-one to `regions`) is saved with a derived column whose `sql` is `customers.regions.customers.spend`
- **THEN** the save is rejected with the circular-definition error naming revisited model `customers` and hop `customers` leaving `regions`

#### Scenario: a leading declaring-model qualifier keeps the full spelling
- **WHEN** `customers` is saved with a derived column whose `sql` is `customers.regions.customers.spend`
- **THEN** the save is rejected with the circular-definition error whose reference is the complete spelling `customers.regions.customers.spend`, with revisited model `customers`, hop `customers` leaving `regions`

#### Scenario: a to-one round trip is still circular
- **WHEN** two models are joined `one_to_one` in both orientations and one is saved with a derived column that walks to the other and back (`b.a.y` declared on `a`)
- **THEN** the save is rejected with the circular-definition error, not accepted as to-one

#### Scenario: a revisit takes precedence over an earlier fanning hop
- **WHEN** `orders` with a join declared `one_to_many` to `line_items` is saved with a derived column whose `sql` is `line_items.orders.amount`
- **THEN** the save is rejected with the circular-definition error, not the fanning error

#### Scenario: the engine's save door rejects the same definition identically
- **WHEN** the `regions.customers.spend` definition is saved through the engine's `save_model`
- **THEN** the save is rejected with the same circular-definition error, naming the same column, kind, reference, revisited model, hop and remedy

#### Scenario: a known-but-unloaded intermediate model is skipped at save time
- **WHEN** `customers` is saved with a derived column whose `sql` is `regions.customers.spend` while `regions` is known to the datasource but cannot be loaded
- **THEN** the save succeeds with no circular error (the query-time door remains the check)

## ADDED Requirements

### Requirement: A stored circular definition is refused at query time

A derived column whose definition revisits a model on its path and was stored
without save-time validation SHALL be refused whenever a query references it —
as an aggregate input from the declaring model or from a model reaching it over
a to-one hop, as a dimension, in a query filter, in raw-row mode, through
another derived column whose definition names it, and for a `filter`-kind
definition — with the circular error naming the reference and the revisited
model, raised before any SQL executes and never with a value. The query-typed
spelling of the same path SHALL fail with the same error class. A stored
definition whose path reaches a model absent from the query's resolved models
SHALL be refused as unresolvable, never emitted verbatim.

#### Scenario: aggregated across a to-one hop
- **WHEN** `customers.revisit_spend` (stored with `sql` `regions.customers.spend`) is queried from `orders` as `customers.revisit_spend:sum`
- **THEN** the query fails with the circular error naming reference `regions.customers.spend` and revisited model `customers`, and returns no rows

#### Scenario: aggregated on the declaring model
- **WHEN** the same column is queried from `customers` as `revisit_spend:sum`
- **THEN** the query fails with the same circular error

#### Scenario: as a dimension, in a filter, and in raw-row mode
- **WHEN** the same column is used as a query dimension, in a query filter, or as a raw-row (`distinct_dimension_values=False`) dimension
- **THEN** each query fails with the same circular error before any SQL executes, never a `no such column` or schema-drift error and never the literal reference text as a value

#### Scenario: through a derived chain
- **WHEN** a derived column `chain` with `sql` `revisit_spend * 2` is aggregated
- **THEN** the query fails with the circular error naming reference `regions.customers.spend` and revisited model `customers`

#### Scenario: a filter-kind definition
- **WHEN** a stored column with `sql` `spend` and `filter` `regions.customers.spend > 0` is aggregated
- **THEN** the query fails with the same circular error

#### Scenario: the query-typed spelling fails with the same error class
- **WHEN** `customers.regions.customers.spend:sum` is queried from `orders`
- **THEN** the query fails with the circular error, the same error class the stored definition raises, whose message names the circular join and the revisited model

#### Scenario: a path through an unresolved model is refused as unresolvable
- **WHEN** a stored definition's path walks into a model absent from the query's resolved models before it could revisit
- **THEN** the query fails with the unresolvable-join error naming that model, never emitting the reference verbatim
