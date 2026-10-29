## MODIFIED Requirements

### Requirement: Current-spec models expose their declared fields

For a current-spec document, each dataset's model SHALL expose exactly its declared fields as visible columns (plus any hidden helper columns the import needs: metric operands). Source columns that no field declares SHALL NOT appear as visible columns of the model. A field expression SHALL still be able to read any column of the dataset's source, with the source's meaning, unless another field holds that column's name, in which case the reading field is dropped with a report. A dataset that declares no fields SHALL still import, with a warning that it exposes no columns.

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

### Requirement: Aggregate operands

A non-bare aggregate operand (arithmetic, `CASE`, scalar functions, or, in a model-scoped metric, an operand combining fields of several datasets) SHALL be carried in the measure's formula when the formula language can express it. Otherwise the importer SHALL create a hidden helper column holding the operand on its single owning dataset, typed from the database. An operand that is neither expressible nor confined to one dataset SHALL make the metric reported as unconverted. A string literal in a metric SHALL keep its value exactly, including any `'` (written `''`) and backslash characters.

#### Scenario: Arithmetic operand
- **WHEN** a dataset-scoped metric is `SUM(quantity * amount)`
- **THEN** the measure returns the sum of `quantity * amount` and the model gains no hidden column for it

#### Scenario: Cross-dataset operand
- **WHEN** a model-scoped metric is `SUM(orders.amount - customers.discount)` and `orders` joins `customers` many-to-one
- **THEN** the measure imports and returns the sum over orders of `amount - discount` of each order's customer

#### Scenario: Operand needing a database-specific function
- **WHEN** a dataset-scoped metric aggregates a function the formula language does not offer
- **THEN** the model gains a hidden column holding the operand and the measure aggregates it

#### Scenario: Quote and backslash in a literal
- **WHEN** a dataset-scoped metric is `COUNT(*) FILTER (WHERE note = 'O''Brien\x')` and one row's `note` is `O'Brien\x`
- **THEN** the measure imports and returns 1

## ADDED Requirements

### Requirement: List aggregation inputs

An aggregate over a list-valued field other than `COUNT(field)` (for example `COUNT(DISTINCT tags)`, `MAX(tags)`, `MIN`, `SUM` or `AVG` over a list) SHALL make the metric reported as unconverted, naming the aggregate and the list field; it SHALL NOT be rewritten to an aggregate over the list's elements. `COUNT(tags)` SHALL import and count the rows whose list is not NULL.

#### Scenario: Maximum of a list
- **WHEN** a dataset-scoped metric is `MAX(tags)` and `tags` is a list field
- **THEN** the metric is reported as unconverted, naming `MAX` and `tags`

#### Scenario: Distinct count of a list
- **WHEN** a dataset-scoped metric is `COUNT(DISTINCT tags)`
- **THEN** the metric is reported as unconverted

#### Scenario: Count of a list
- **WHEN** a dataset-scoped metric is `COUNT(tags)` over the rows `['a', 'b']`, `['b']` and NULL
- **THEN** the measure returns 2

### Requirement: Array functions map to the list library

In metrics and in field expressions, `ARRAY_AGG(DISTINCT x)` SHALL import as `collect(x)`, `ARRAY_CONTAINS(a, v)` and `v = ANY(a)` as `contains(a, v)`, and `ARRAY_LENGTH(a)` and `CARDINALITY(a)` as `length(a)`, with SLayer's NULL conventions: `collect` drops NULL inputs and yields an empty list for a cell without inputs; `contains(a, v)` is NULL when `a` or `v` is NULL and FALSE when `v` is absent, even if `a` holds a NULL element; `length` counts non-NULL elements and is NULL for a NULL list. A field expression using these functions, unless it is the target's own dialect and kept verbatim, SHALL be stored as the SQL SLayer itself emits for the library function on the target datasource; a field whose list element type the target needs but the import cannot determine SHALL be reported.

#### Scenario: Membership in a metric
- **WHEN** a dataset-scoped metric is `COUNT(*) FILTER (WHERE 'b' = ANY(tags))` over the rows `['a', 'b']`, `['b']` and NULL
- **THEN** the measure returns 2

#### Scenario: Membership in a field
- **WHEN** a field is `has_a: ARRAY_CONTAINS(tags, 'a')` and the target is a DuckDB datasource
- **THEN** grouping by `has_a` returns TRUE for one row, FALSE for one row and NULL for one row

#### Scenario: Length in a field
- **WHEN** a field is `n_tags: CARDINALITY(tags)`
- **THEN** it returns 2, 1 and NULL

#### Scenario: Distinct values collected
- **WHEN** a model-scoped metric is `ARRAY_AGG(DISTINCT orders.status)` over statuses `paid`, `paid`, `open`
- **THEN** the measure returns `['open', 'paid']`
