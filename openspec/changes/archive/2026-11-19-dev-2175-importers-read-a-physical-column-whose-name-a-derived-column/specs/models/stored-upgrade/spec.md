## ADDED Requirements

### Requirement: Stored field sql converts to its expression

A stored dataset written before this version SHALL load with each field's `sql` converted to its `expression` by the rules of `models/field-expressions` › A field's sql converts to its expression, with no deprecation warning, and every query SHALL return the values it returned before. The conversion SHALL run in two phases: a context-free version step that turns a bare or double-quoted identifier into its shorthand (into its path when it reads a sibling field) and leaves any other `sql` in place, then a load step with the store's datasource and its datasets that writes each sibling reference as its path and tags the text with the datasource's own dialect. A dataset whose store holds no config for its datasource SHALL load with that `sql` unconverted (`models/field-expressions` › A field's sql waits for its dialect) and SHALL be converted on a later load that has it. A converted dataset SHALL be written back as every other upgraded document is; one left with unconverted `sql` SHALL NOT be written back.

#### Scenario: A stored dataset of every shape loads
- **WHEN** a store holding a DuckDB datasource config and a dataset `orders` written with fields `amount` (`sql: amount`), `label` (`sql: '"Label"'`), `doubled` (`sql: "amount * 2"`) and `ratio` (`sql: "doubled / qty"`) is opened
- **THEN** the fields load as `expression: amount`, `expression: '"Label"'`, `{dialects: [{dialect: DUCKDB, expression: "amount * 2"}]}` and `{dialects: [{dialect: DUCKDB, expression: "orders.doubled / qty"}]}`, no `FutureWarning` is emitted, the document is written back at the current version, and every field returns its former values

#### Scenario: A stored dataset without its datasource config
- **WHEN** the same dataset is opened from a store that holds no datasource config
- **THEN** `amount` and `label` load converted, `doubled` and `ratio` keep their `sql`, the document is not written back, and opening it again after the datasource config is added converts and writes it back

#### Scenario: Direct validation of a stored document
- **WHEN** a stored dataset document of the previous version is validated directly, with no store
- **THEN** its bare-identifier fields carry the shorthand and its other fields keep `sql`

### Requirement: Stored filters keep their sibling references

In a stored dataset written before this version, each bare name in a field's `filter` or a dataset's `filters` that today reads a sibling field SHALL be rewritten on load to that field's path from the dataset (`<dataset>.<field>`), so every query returns the values it returned before. The rewrite SHALL run in the context-free version step. Documents of this version or later SHALL never be rewritten this way.

#### Scenario: A filter reading a renamed field
- **WHEN** a stored `orders` written before this version declares fields `status` (`sql: state`) and `paid_amount` (`sql: amount`, `filter: "status = 'paid'"`) and the dataset filter `status <> 'x'`
- **THEN** it loads with `filter: "orders.status = 'paid'"` and the dataset filter `orders.status <> 'x'`, and `paid_amount` returns NULL and 20 for ids 1 and 2

### Requirement: Stored source-qualified paths keep their meaning

In a stored dataset written before this version, a field's `expression`, a field's `filter`, a dataset's `filters` or a metric's `expression` that reads `source.<name>` through a relationship or a dataset named `source` SHALL be rewritten on load to the same path spelled by the target dataset's name, when that spelling resolves to the same hop. When it does not, the document SHALL fail to load through the stored-document boundary, untouched on disk, with a warning naming the dataset and the definition. Documents of this version or later SHALL never be rewritten this way.

#### Scenario: A relationship named source
- **WHEN** a stored `orders` written before this version relates to `leads` through relationship `source` and declares metric `sum(source.amount)`
- **THEN** it loads with metric `sum(leads.amount)`, which returns the sum of `leads.amount` as before

#### Scenario: No unique spelling
- **WHEN** a stored `orders` written before this version relates to `leads` through relationships `source` and `referrer`, and a field reads `source.name`
- **THEN** `orders` fails to load with a warning naming `orders` and that field, and the other datasets load

#### Scenario: A current document is not rewritten
- **WHEN** a dataset saved at this version relates to `leads` through relationship `source` and its metric is `sum(source.amount)`
- **THEN** the metric sums the dataset's own source column `amount`
