## REMOVED Requirements

### Requirement: Deterministic dialect selection
**Reason**: The importer no longer chooses one form per expression: it stores the whole expression object, and the form that runs is chosen on each datasource (`models/field-expressions` › The datasource's own dialect entry runs first). A selection order remains only for translating an expression that has no form SLayer can run.
**Migration**: See "Expression objects are stored as written" and "An expression with no runnable form is translated".

## ADDED Requirements

### Requirement: Expression objects are stored as written

A field's and a metric's Ossie expression SHALL be stored as the field's or metric's `expression` with its portable `sql` and every `dialects` entry kept, so that the form that runs on the target datasource follows `models/field-expressions`. A dialect entry for the target datasource's own dialect, or an entry from a translation, SHALL be stored verbatim apart from the identifier folding of "Imported SQL reads with the source's identifier folding" (`importers/meaning-preservation`).

#### Scenario: Portable form and a vendor dialect kept together
- **WHEN** a field has `sql: "amount"` and a `SNOWFLAKE` entry `amount::NUMBER`, and the target is a Snowflake datasource
- **THEN** the imported field carries both forms, and querying it on Snowflake runs the `SNOWFLAKE` entry

#### Scenario: ANSI and target-native kept together
- **WHEN** a field has an `ANSI_SQL` entry and a `POSTGRES` entry, and the target is a Postgres datasource
- **THEN** the imported field carries both entries, and querying it runs the `POSTGRES` entry

#### Scenario: Datasource type aliases
- **WHEN** the target datasource type is `postgresql` and a field's only entries are `MDX` and `POSTGRES`
- **THEN** the `POSTGRES` entry is the one that runs on the target, and no translation is made

### Requirement: An expression with no runnable form is translated

When a field's expression has no form SLayer can run on the target datasource — no portable `sql` or `OSSIE_SQL_2026` entry that SLayer reads, and no entry for the target's own dialect — the importer SHALL select one candidate, taking the first that exists and is readable in this order: the dialect named by the `--dialect` option; the top-level `sql`; an `OSSIE_SQL_2026` entry; an `ANSI_SQL` entry; any other readable SQL dialect entry. Within one step the first entry in document order SHALL win. A readable dialect is `OSSIE_SQL_2026`, `ANSI_SQL`, or a token naming a SQL dialect the importer can parse, matched case-insensitively. The selected candidate SHALL be rendered in the target datasource's dialect and added to the expression as the entry for that dialect, and the report SHALL hold an informational entry naming the dialect translated from. When `--dialect` is not given, no override step applies.

#### Scenario: Foreign vendor dialect as last resort
- **WHEN** a field's only entry is `BIGQUERY` and the target is a DuckDB datasource
- **THEN** the field imports with its `BIGQUERY` entry and an added `DUCKDB` entry translated from it, and the report holds an informational entry naming `BIGQUERY`

#### Scenario: Override first
- **WHEN** the import runs with `--dialect SNOWFLAKE`, the target is a DuckDB datasource, and a field has a `SNOWFLAKE` entry and a `BIGQUERY` entry
- **THEN** the added `DUCKDB` entry is translated from the `SNOWFLAKE` entry

#### Scenario: Unreadable override falls through
- **WHEN** the import runs with `--dialect MDX`, the target is a DuckDB datasource, and a field has an `MDX` entry and an `ANSI_SQL` entry
- **THEN** the added `DUCKDB` entry is translated from the `ANSI_SQL` entry

#### Scenario: Portable sql SLayer cannot read
- **WHEN** a field's only form is a top-level `sql` that is valid Ossie SQL but outside what SLayer reads, and the target is a DuckDB datasource
- **THEN** the field imports with its `sql` and an added `DUCKDB` entry translated from it, and the report names the translation

### Requirement: A metric's entry for the target's dialect is not imported

A dataset-scoped metric whose expression carries an entry for the target datasource's own dialect SHALL import without that entry, with a report naming the metric and the dialect, when the expression also has a portable `sql`; without a portable `sql`, that entry SHALL be translated into the metric's portable expression, with a report naming the dialect. Entries for other dialects SHALL be kept.

#### Scenario: Dropped beside portable sql
- **WHEN** metric `total` has `sql: "SUM(amount)"` and a `DUCKDB` entry `sum(amount)`, and the target is a DuckDB datasource
- **THEN** `total` imports with its `sql` and no `DUCKDB` entry, returns the sum of `amount`, and the report names `total` and `DUCKDB`

#### Scenario: Translated when it is the only form
- **WHEN** metric `total`'s only entry is `DUCKDB` `sum(amount)` and the target is a DuckDB datasource
- **THEN** `total` imports with a portable expression returning the sum of `amount`, and the report names `DUCKDB`

## MODIFIED Requirements

### Requirement: Selected expressions are rendered for the target datasource

An expression SHALL be read with the conventions of its own dialect, where `OSSIE_SQL_2026` and `ANSI_SQL` read Ossie's portable function set (including `IFF`, `ZEROIFNULL`, `DATEADD`, `STARTSWITH` and `||`), and its unquoted identifiers SHALL be read with the target datasource's identifier folding. A field's portable `sql` SHALL mean on the target what Ossie defines; every raw-SQL definition the import adds by translation (a field's dialect entry, a field's filter) SHALL be rendered in the target datasource's SQL dialect. An expression whose dialect is the target datasource's own SHALL be stored verbatim, apart from the identifier folding of "Imported SQL reads with the source's identifier folding" (`importers/meaning-preservation`). An expression none of whose forms can be parsed SHALL be reported for its entity.

#### Scenario: Portable function rendered for Postgres
- **WHEN** a field is `sql: "IFF(amount > 0, amount, 0)"` and the target is a Postgres datasource
- **THEN** querying the field returns `amount` for positive amounts and `0` otherwise

#### Scenario: Concatenation on MySQL
- **WHEN** a field is `sql: "first_name || ' ' || last_name"` and the target is a MySQL datasource
- **THEN** querying the field on MySQL returns the concatenated strings

#### Scenario: Target-native kept verbatim
- **WHEN** a field's `SNOWFLAKE` entry is `amount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the imported field's `SNOWFLAKE` entry is exactly `amount::NUMBER(10,2)`

#### Scenario: Target-native mixed-case identifier folded
- **WHEN** a field's `SNOWFLAKE` entry is `OrderAmount::NUMBER(10,2)` and the target is a Snowflake datasource
- **THEN** the imported field's `SNOWFLAKE` entry is exactly `orderamount::NUMBER(10,2)`

#### Scenario: Portable expression folded by the target
- **WHEN** a field is `sql: "IFF(OrderAmount > 0, OrderAmount, 0)"` and the target is a Postgres datasource
- **THEN** querying the field reads the physical column `orderamount`

#### Scenario: Unparseable expression
- **WHEN** a field's only form is `amount +* 2`
- **THEN** that field is reported and the dataset's other fields import

### Requirement: Current-spec models expose their declared fields

For a current-spec document, each dataset's model SHALL expose exactly its declared fields as visible fields (plus any hidden helper fields the import needs: metric operands). Source columns that no field declares SHALL NOT appear as visible fields of the model. A field expression SHALL be able to read any column of the dataset's source, with the source's meaning, including a column whose name another field holds. A dataset that declares no fields SHALL still import, with a warning that it exposes no columns. Within a dataset, declared field names and dataset-scoped metric names SHALL be unique, compared case-insensitively as Ossie compares unquoted identifiers. A document that breaks this SHALL be refused: the import SHALL fail naming the dataset and the clashing names, and SHALL import nothing.

#### Scenario: Renamed field hides its source column
- **WHEN** dataset `orders` has source columns `o_orderkey` and `o_totalprice` and declares fields `order_id: o_orderkey` and `total_amount: o_totalprice`
- **THEN** model `orders` has fields `order_id` and `total_amount` and no field `o_orderkey` or `o_totalprice`, and querying `total_amount` returns the values of `o_totalprice`

#### Scenario: Derived field over an undeclared source column
- **WHEN** a dataset declares only `total_usd: total_cents / 100.0` and `total_cents` is a source column
- **THEN** field `total_usd` imports and returns `total_cents / 100.0`

#### Scenario: Field reading a source column another field's name has taken
- **WHEN** a dataset declares fields `status: state` and `status_upper: UPPER(status)` over source columns `status` and `state`
- **THEN** both import, the visible fields are exactly `status` and `status_upper`, and `status_upper` returns the physical `status` upper-cased

#### Scenario: Dataset without fields
- **WHEN** a current-spec dataset declares no fields
- **THEN** its model imports with no visible fields and the report holds a warning for that dataset

#### Scenario: Names repeated within a dataset
- **WHEN** dataset `orders` declares fields `amount` and `Amount`, or a field `total` and a dataset-scoped metric `TOTAL`, or two dataset-scoped metrics named `tot`
- **THEN** the import fails naming `orders` and the clashing names, and no model is imported

### Requirement: Source columns in dataset-scoped metrics

In a dataset-scoped metric of a current-spec document, a two-part reference whose qualifier is `source`, matched case-insensitively whether quoted or not, SHALL read the named column of that dataset's source (a table's columns, or a query source's output columns), matched with the target datasource's identifier folding (a quoted name exactly, an unquoted name as the database folds it), whatever fields the dataset declares. The measure SHALL return the same values as aggregating that physical column, in every position of the metric: aggregate arguments, `FILTER` predicates and percentile orderings. The metric SHALL be stored with the reference as written, and its import SHALL add no field; the model's visible fields SHALL remain exactly its declared fields. The metric SHALL be reported as unconverted, naming the reason, when:

- the dataset declares a field whose name matches `source`, which takes precedence over the qualifier;
- the name matches no column of the source, or several;
- the reference appears in a model-scoped metric.

#### Scenario: Source column no field declares
- **WHEN** dataset `orders` declares fields `order_id` and `amount` and metric `units: SUM(source.quantity)` over rows with `quantity` 2 and 3
- **THEN** measure `units` returns 5 and the visible fields of `orders` are exactly `order_id` and `amount`

#### Scenario: Metric named like the source column
- **WHEN** dataset `orders` declares metric `quantity: SUM(source.quantity)` and no field `quantity`
- **THEN** measure `quantity` imports and returns the sum of the physical `quantity`

#### Scenario: Source column a field passes through
- **WHEN** dataset `orders` declares field `amount: amount` and metric `SUM(source.amount)`
- **THEN** the measure returns the sum of `amount` and the model gains no hidden field

#### Scenario: Qualifier case
- **WHEN** a dataset-scoped metric is `SUM(SOURCE.quantity)`
- **THEN** it imports and returns what `SUM(source.quantity)` returns

#### Scenario: Source column inside an operand
- **WHEN** dataset `orders` declares field `price` and metric `SUM(source.quantity * price)` over rows (2, 10), (3, 5)
- **THEN** the measure returns 35

#### Scenario: Source column in a FILTER predicate
- **WHEN** dataset `orders` declares field `amount` but no field reading `status`, and metric `SUM(amount) FILTER (WHERE source.status = 'completed')` over rows (completed, 10), (open, 5), (completed, 7)
- **THEN** the measure returns 17

#### Scenario: Filtered row count over a source column
- **WHEN** the same dataset declares metric `COUNT(*) FILTER (WHERE source.status = 'completed')`
- **THEN** the measure returns 2

#### Scenario: Percentile over a source column
- **WHEN** a dataset-scoped metric is `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY source.quantity)` over `quantity` values 1, 2, 3
- **THEN** the measure returns 2

#### Scenario: Query source output column
- **WHEN** a dataset's source is a query selecting `qty AS units` and its metric is `SUM(source.units)`
- **THEN** the measure returns the sum of `qty`

#### Scenario: Folded source column
- **WHEN** the target is a Postgres datasource whose table has column `quantity` and a dataset-scoped metric is `SUM(source.Quantity)`
- **THEN** the measure returns the sum of `quantity`

#### Scenario: Quoted source column in the wrong case
- **WHEN** the source has column `quantity` and a dataset-scoped metric is `SUM(source."Quantity")`
- **THEN** the metric is reported as unconverted

#### Scenario: Unknown source column
- **WHEN** a dataset-scoped metric is `SUM(source.quantty)` and the source has no such column
- **THEN** the metric is reported as unconverted and the model gains no field

#### Scenario: Field named source
- **WHEN** dataset `orders` declares a field `source` and metric `SUM(source.quantity)`
- **THEN** the metric is reported as unconverted with a reason saying the field `source` takes precedence

#### Scenario: Source column behind a renamed field
- **WHEN** dataset `orders` has source columns `status` and `state`, declares field `status: state`, and its metric is `COUNT(*) FILTER (WHERE source.status = 'open')`
- **THEN** the metric imports and returns the number of rows whose physical `status` is `open`

#### Scenario: Source qualifier in a model-scoped metric
- **WHEN** a model-scoped metric is `SUM(source.quantity)` and no dataset is named `source`
- **THEN** the metric is reported as unconverted
