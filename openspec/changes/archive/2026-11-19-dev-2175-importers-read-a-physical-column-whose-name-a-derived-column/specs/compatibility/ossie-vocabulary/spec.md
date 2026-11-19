## MODIFIED Requirements

### Requirement: Datasets are written in Ossie's vocabulary

A dataset SHALL hold its fields under `fields`, its metrics under `metrics` and its relationships under `relationships`. A field and a metric SHALL carry their type under `datatype`, a field, a metric and a custom aggregation their definition under `expression`, and a relationship its target dataset under `to` and its keys under `from_columns` and `to_columns`. A dataset's `filters`, `aggregations`, `sql`, `sql_table` and `source_queries` SHALL keep their names; a field's `sql` is a deprecated spelling of its `expression` (`models/field-expressions` › A field's sql converts to its expression).

#### Scenario: A dataset is dumped
- **WHEN** a dataset with a field, a metric, a custom aggregation and a relationship is serialized
- **THEN** it carries `fields`, `metrics`, `relationships`, `datatype`, `expression` (on the field and on the metric), `to`, `from_columns` and `to_columns`, and none of `columns`, `measures`, `joins`, `type`, `formula`, `target_model`, `join_pairs` or a field-level `sql`

#### Scenario: Kept keys are unchanged
- **WHEN** a dataset with row filters is serialized
- **THEN** it carries `filters` under that name

### Requirement: The Ossie export payload carries the vocabulary

The `SLAYER` vendor payload of an exported Ossie document SHALL be written at payload version 3, in the Ossie vocabulary and shapes, with a field's definition carried under `expression`. The importer SHALL read payload versions 1, 2 and 3 with the same result: a version-1 payload's fingerprint SHALL be checked against its content as written, and its pre-2.0 keys and shapes SHALL then convert with no deprecation warning; a field `sql` in a version-1 or version-2 payload SHALL convert as a stored field `sql` does (`models/field-expressions` › A field's sql converts to its expression), with the import's datasource dialect.

#### Scenario: An export uses the new keys
- **WHEN** a dataset with a relationship, a SLayer-only field and a SLayer-only metric is exported to Ossie
- **THEN** every `SLAYER` payload is version 3 and carries no pre-2.0 key or shape and no field-level `sql`, and every relationship carries `from_columns` and `to_columns`

#### Scenario: A pre-2.0 export still imports
- **WHEN** an Ossie document exported before this change (payload version 1) is imported
- **THEN** every payload's fingerprint matches, no `FutureWarning` is emitted, and the imported datasets equal those imported from the same datasets exported at version 3

#### Scenario: A version-2 export still imports
- **WHEN** an Ossie document exported at payload version 2, whose SLayer-only field `amount_x2` carries `sql: amount * 2`, is imported into a DuckDB datasource
- **THEN** `amount_x2` carries the DuckDB dialect entry `amount * 2`, no `FutureWarning` is emitted, and it returns twice each order's `amount`
