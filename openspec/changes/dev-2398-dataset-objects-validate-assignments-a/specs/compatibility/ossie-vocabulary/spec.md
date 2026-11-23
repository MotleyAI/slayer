## ADDED Requirements

### Requirement: Dataset objects validate assignments

A dataset, a datasource, and every field, metric, relationship, aggregation, aggregation parameter, source-dataset origin, extension note, number format and expression object reachable from them SHALL validate a value assigned to one of their attributes after construction with the checks construction applies, the object's cross-attribute checks included. A refused assignment SHALL raise and SHALL leave the object exactly as it was, including any list it holds. Each such object SHALL offer `assign(**changes)`, which applies several attribute changes together, validates the final state once, and on refusal leaves every attribute unchanged. Normalizations construction applies SHALL keep their results, and placing a prebuilt object in a dataset SHALL NOT modify that object.

#### Scenario: An invalid assignment is refused and leaves the object unchanged
- **WHEN** a field is assigned `label = 5`, a relationship with one `to_columns` entry is assigned `from_columns = ["a", "b"]`, a dataset is assigned two fields of the same name, and a datasource is assigned `port = "abc"`
- **THEN** each assignment raises, and each object keeps every attribute value it had before

#### Scenario: A valid assignment applies
- **WHEN** a field is assigned a new label and a dataset a new description
- **THEN** both apply and a query over the dataset runs with them

#### Scenario: Linked attributes change together
- **WHEN** a table-backed dataset calls `assign(sql="SELECT ...", sql_table=None)` and a relationship calls `assign(from_columns=["a", "b"], to_columns=["c", "d"])`
- **THEN** both apply, although each attribute alone would leave the object invalid

#### Scenario: A refused multi-attribute change leaves every attribute unchanged
- **WHEN** a dataset calls `assign` with one valid and one invalid change
- **THEN** it raises and neither attribute has changed

#### Scenario: The deprecated key-pairs attribute writes both key lists
- **WHEN** a relationship's deprecated `join_pairs` attribute is assigned two pairs, in place of one
- **THEN** both key lists hold the two pairs' sides, with the deprecation warning

#### Scenario: Editing a dataset's source switches it in every direction
- **WHEN** the MCP `edit_dataset` tool switches a dataset from `sql_table` to `sql`, from `sql` to `sql_table`, from `sql_table` to `source_queries`, and from `source_queries` to `sql_table`
- **THEN** each edit applies, and an edit the final state refuses leaves the dataset unchanged

#### Scenario: Construction normalizations keep their results
- **WHEN** a field is built with the deprecated `sql` naming one column, a currency number format is built without a symbol, and an sql-mode dataset is built with a list field that has no element type from a prebuilt field object
- **THEN** the field reads that column as its expression and is marked as written with `sql`, the format carries the default currency symbol, the dataset's list field has element type `Opaque`, and the prebuilt field object is unchanged

#### Scenario: Assigning a deprecated sql after construction stays as written
- **WHEN** a built field is assigned `sql = "amount"`
- **THEN** the field holds `sql` as written, as before this change
