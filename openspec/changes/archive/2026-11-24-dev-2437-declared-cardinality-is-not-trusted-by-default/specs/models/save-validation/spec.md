## ADDED Requirements

### Requirement: Enforced keys name declared keys of a table-backed dataset
Saving a dataset SHALL refuse an `enforced_keys` entry that is not one of its declared key
sets (its primary key, or a unique field), one naming a field that is not a base column,
and any `enforced_keys` on a dataset backed by `sql` or a backing query. A save that changes
a dataset's `sql_table`, `sql`, backing query or datasource SHALL drop its `enforced_keys`;
one that makes a field an enforced key names no longer a base column, or a different one,
SHALL drop that key.

#### Scenario: An undeclared enforced key is refused
- **WHEN** a dataset with primary key `id` is saved with `enforced_keys: [[email]]` and `email`
  is not unique
- **THEN** the save fails naming the entry

#### Scenario: A query-backed dataset cannot carry enforced keys
- **WHEN** a dataset backed by `sql` is saved with an enforced key
- **THEN** the save fails

#### Scenario: Repointing drops enforced keys
- **WHEN** a dataset recording `[customer_id]` as enforced is saved with a different
  `sql_table`
- **THEN** the stored dataset has no enforced keys

#### Scenario: An enforced key on a computed field is refused
- **WHEN** a dataset is saved with `enforced_keys: [[customer_id]]` and field `customer_id`
  has the expression `lower(email)`
- **THEN** the save fails naming the entry

#### Scenario: Re-pointing a key field drops its enforced key
- **WHEN** a dataset recording `[customer_id]` as enforced is saved with field `customer_id`
  reading another column, or a computed expression
- **THEN** the stored dataset no longer records `[customer_id]` as enforced
