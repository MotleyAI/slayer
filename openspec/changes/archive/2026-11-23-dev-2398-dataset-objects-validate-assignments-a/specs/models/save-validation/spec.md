## ADDED Requirements

### Requirement: Saves re-validate the whole dataset or datasource

Saving a dataset, saving a document dataset's SLayer extensions, and saving a datasource SHALL first validate the object in full, every nested part included, with the checks construction applies, before any canonicalization, dialect conversion, peer loading or write. An object these checks refuse SHALL be refused with the construction error and nothing SHALL be written. Runtime-only state the object carries SHALL take part in the check as it does at construction, and the saved object SHALL NOT be modified by the check.

#### Scenario: A dataset changed in place is refused on save
- **WHEN** a dataset is saved after a field duplicating another's name was appended to its fields, after a key was appended to one relationship's `from_columns` only, after a non-field value was appended to its fields, or after `"bogus"` was appended to one field's `allowed_aggregations`
- **THEN** each save is refused with the error construction raises for that state, and the store's write was never called

#### Scenario: Document dataset extensions are re-validated
- **WHEN** a document-declared dataset whose fields gained a duplicate name in place has its SLayer extensions saved
- **THEN** the save is refused with the construction error and the document is unchanged

#### Scenario: A datasource changed in place is refused on save
- **WHEN** a datasource is saved after a value that is not a granularity was appended to its granularities
- **THEN** the save is refused with the construction error and the store's write was never called

#### Scenario: A valid dataset with runtime-only state saves as before
- **WHEN** a dataset carrying a metric that loaded with a load warning, and a document-declared dataset, are saved unchanged
- **THEN** both saves succeed exactly as before this change
