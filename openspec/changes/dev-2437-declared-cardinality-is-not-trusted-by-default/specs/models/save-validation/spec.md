## ADDED Requirements

### Requirement: Enforced keys name declared keys of a table-backed dataset
Saving a dataset SHALL refuse an `enforced_keys` entry that is not one of its declared key
sets (its primary key, or a unique field), and any `enforced_keys` on a dataset backed by
`sql` or a backing query. A save that changes a dataset's `sql_table`, `sql`, backing query
or datasource SHALL drop its `enforced_keys`.

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
