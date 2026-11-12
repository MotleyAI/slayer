## MODIFIED Requirements

### Requirement: Current-spec models expose their declared fields

For a current-spec document, each dataset's model SHALL expose exactly its declared fields as visible columns (plus any hidden helper columns the import needs: metric operands). Source columns that no field declares SHALL NOT appear as visible columns of the model. A field expression SHALL still be able to read any column of the dataset's source, with the source's meaning, unless another field holds that column's name, in which case the reading field is dropped with a report. A dataset that declares no fields SHALL still import, with a warning that it exposes no columns. Within a dataset, declared field names and dataset-scoped metric names SHALL be unique, compared case-insensitively as Ossie compares unquoted identifiers. A document that breaks this SHALL be refused: the import SHALL fail naming the dataset and the clashing names, and SHALL import nothing.

#### Scenario: Renamed field hides its source column
- **WHEN** dataset `orders` has source columns `o_orderkey` and `o_totalprice` and declares fields `order_id: o_orderkey` and `total_amount: o_totalprice`
- **THEN** model `orders` has columns `order_id` and `total_amount` and no column `o_orderkey` or `o_totalprice`, and querying `total_amount` returns the values of `o_totalprice`

#### Scenario: Derived field over an undeclared source column
- **WHEN** a dataset declares only `total_usd: total_cents / 100.0` and `total_cents` is a source column
- **THEN** model column `total_usd` imports and returns `total_cents / 100.0`

#### Scenario: Field reading a source column another field's name has taken
- **WHEN** a dataset declares fields `status: state` and `status_upper: UPPER(status)` over source columns `status` and `state`
- **THEN** `status_upper` is dropped with a report naming `status`, and the visible columns are exactly `status`

#### Scenario: Dataset without fields
- **WHEN** a current-spec dataset declares no fields
- **THEN** its model imports with no visible columns and the report holds a warning for that dataset

#### Scenario: Names repeated within a dataset
- **WHEN** dataset `orders` declares fields `amount` and `Amount`, or a field `total` and a dataset-scoped metric `TOTAL`, or two dataset-scoped metrics named `tot`
- **THEN** the import fails naming `orders` and the clashing names, and no model is imported

### Requirement: Identifier case in metric references

An unquoted identifier in a metric expression SHALL match a declared field or dataset name case-insensitively; a double-quoted identifier SHALL match exactly. The stored measure SHALL reference fields by their declared names.

#### Scenario: Case-insensitive match
- **WHEN** dataset `orders` declares field `amount` and a dataset-scoped metric is `SUM(Amount)`
- **THEN** the measure imports and its formula references `amount`

#### Scenario: Quoted exact match
- **WHEN** dataset `orders` declares field `Amount` and a dataset-scoped metric is `SUM("amount")`
- **THEN** the metric is reported as unconverted

#### Scenario: Case-only ambiguity
- **WHEN** dataset `orders` declares fields `id` and `ID`
- **THEN** the import fails naming `orders`, `id` and `ID`

### Requirement: Dataset-scoped metrics become measures on their dataset

Each metric in `datasets[].metrics` SHALL become a measure on that dataset's model. A dataset-scoped metric whose name collides with another column or measure on that model — a hidden helper column, a SLayer-only column, or a model-scoped metric's measure — SHALL be reported as unconverted; a collision with a declared field or another dataset-scoped metric of the same dataset refuses the document (Current-spec models expose their declared fields).

#### Scenario: Dataset-scoped metric
- **WHEN** dataset `orders` declares field `amount` and metric `total_amount: SUM(amount)`
- **THEN** model `orders` has measure `total_amount` and querying it returns the sum of `amount`

#### Scenario: Name collision with a field
- **WHEN** dataset `orders` declares field `revenue` and dataset-scoped metric `revenue: SUM(amount)`
- **THEN** the import fails naming `orders` and `revenue`
