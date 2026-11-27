## MODIFIED Requirements

### Requirement: Fields without a datatype load with an unknown type
A field that declares no `datatype` SHALL load with an unknown type that no check or coercion reads. The engine SHALL resolve unknown types before binding a query that reads them, once per engine and storage fingerprint, without blocking the event loop, from the live type of the field's expression. The resolved type SHALL NOT be persisted. A save-time check that needs an unknown type SHALL be deferred to the query-time check.

On SQLite, whose values carry their own types, the live type SHALL be the one SLayer's SQLite ingestion gives, from one classification shared with ingestion:
- A field that reads one source column SHALL get the column's declared type, refined by the bounded sample of its stored values exactly as ingestion refines it. The declared type is kept when the sample is empty, all NULL or saturated, or the probe fails.
- Any other field SHALL get its type from the same bounded census over a sample of its values. With no non-NULL value in the sample it SHALL be String.

#### Scenario: An untyped numeric field aggregates
- **WHEN** a document field `amount` with expression `amount` declares no `datatype` and its source column is a decimal
- **THEN** `sum(orders.amount)` returns the column's sum and the document's text is unchanged

#### Scenario: An untyped SQLite column aggregates
- **WHEN** on SQLite a document declares the fields `amount` (expression `amount`, over a REAL column), `qty` (expression `qty`, over an INTEGER column) and `doubled` (expression `amount * 2`), none with a `datatype`
- **THEN** `sum(orders.amount)`, `sum(orders.qty)` and `sum(orders.doubled)` return the sums the source table gives, and the document's text is unchanged

#### Scenario: SQLite typing agrees with ingestion
- **WHEN** on SQLite an untyped document field reads one source column, and that table is also ingested, for each of these columns: declared INTEGER with integer values, declared INTEGER holding a real value, declared REAL in an empty table, declared INTEGER holding only NULLs, declared TEXT holding numeric text, declared TEXT holding non-numeric text, and holding a blob
- **THEN** the field's resolved type equals the type ingestion gives that column in every case

#### Scenario: An untyped SQLite expression over no rows is String
- **WHEN** on SQLite an untyped document field has expression `amount * 2` and the source table has no rows
- **THEN** the field resolves to String, as it does today

### Requirement: Every construct the 1.x importer converted keeps its meaning natively
Each Ossie construct `slayer import-osi` converted in 1.x SHALL evaluate natively, from the stored expression, with the meaning the 1.x conversion gave it. These constructs are:
- `agg(x) FILTER (WHERE p)` for every supported aggregation (including `COUNT(*)`, `COUNT(DISTINCT x)` and percentiles), as the aggregation over the rows where `p` holds, never removing result groups;
- `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`, `CORR`, `COVAR_SAMP`, `COVAR_POP` and `APPROX_COUNT_DISTINCT`;
- `ARRAY_AGG(DISTINCT x)` as `collect(x)`, `ARRAY_CONTAINS(a, v)` and `v = ANY(a)` as `contains(a, v)`, `ARRAY_LENGTH(a)` and `CARDINALITY(a)` as `length(a)`, all with SLayer's NULL conventions. So `ARRAY_AGG(DISTINCT x)` drops NULL inputs, returns an empty list for a cell with no input, and sorts its elements. On a database without lists it is refused with the typed list error;
- non-bare aggregate operands;
- `COUNT(list_field)` counting non-NULL lists;
- date literals compared with temporal fields as instants (a date-only literal at its midnight);
- `source.column` in dataset-scoped metrics.

A field or metric expression with no form SLayer can run on the datasource SHALL be translated in memory at load from the first readable candidate, and SHALL NOT be written back. The candidates are tried in this order: top-level `sql`, an `OSSIE_SQL_2026` entry, an `ANSI_SQL` entry, then any readable SQL dialect entry, in document order within each step.

A construct the 1.x importer reported as unconverted SHALL load per "Entities SLayer cannot evaluate yet still load".

#### Scenario: A FILTER aggregate keeps every group
- **WHEN** a dataset metric `paid` has expression `SUM(amount) FILTER (WHERE status = 'paid')` and a query groups it by `orders.region` where one region has no paid orders
- **THEN** that region's row is present with NULL for `orders.paid`

#### Scenario: An array function maps to the list library
- **WHEN** a field `tag_count` has expression `CARDINALITY(tags)`
- **THEN** it returns the count of non-NULL elements of `tags`, and NULL for a NULL list

#### Scenario: A metric written only as a dialect entry runs
- **WHEN** a document's model-scoped metric `gross_revenue` has only the entry `{dialect: ANSI_SQL, expression: SUM(orders.total_amount)}`
- **THEN** a query naming `gross_revenue` returns the sum of `orders.total_amount`, and the document's text is unchanged

#### Scenario: ARRAY_AGG DISTINCT follows collect's NULL conventions
- **WHEN** an imported dataset metric `statuses` has expression `ARRAY_AGG(DISTINCT status)` over the statuses `paid`, `paid`, NULL and `open`, and a query reads it once with no dimension and once grouped by a field that gives one group no rows with a non-NULL status
- **THEN** on DuckDB it returns `['open', 'paid']` with no NULL element, and the empty group gets `[]`; on SQLite the query fails with the typed list error

## ADDED Requirements

### Requirement: Scalar functions and CASE around aggregates evaluate as written
An Ossie document's metric whose expression applies a scalar function or `CASE` to aggregates SHALL load with no load warning, SHALL NOT be reported as not evaluable by `slayer import-osi`, and SHALL return the value the same SQL returns over the source rows of each result group. This includes:
- `IFF`, `IF`, `ZEROIFNULL`, `NVL`, `NVL2`, `NULLIFZERO`, `COALESCE`, `NULLIF`, `ROUND` and `ABS` applied to aggregates or arithmetic over them;
- `CASE` with or without `ELSE`, whose conditions or branches are aggregates;
- the same around an aggregate with `FILTER (WHERE …)`.

This SHALL hold for dataset-scoped and model-scoped metrics alike. A field whose expression is a `CASE` or `IFF` over its row's values SHALL likewise load without a warning and return the row's value.

#### Scenario: Scalars around aggregates match SQL per group
- **WHEN** a document's dataset `orders` has the metrics `IFF(SUM(amount) > 0, SUM(amount), 0)`, `ZEROIFNULL(SUM(amount))`, `NVL2(MAX(amount), 1, 0)`, `NULLIFZERO(COUNT(amount))`, `ROUND(AVG(amount), 1)`, `CASE WHEN COUNT(*) > 1 THEN SUM(amount) END` and `ZEROIFNULL(SUM(amount) FILTER (WHERE status = 'paid'))`, and a group has only NULL amounts
- **THEN** grouped by `orders.status`, on DuckDB and on SQLite, each metric equals the same expression evaluated as SQL over that group's rows, and the import reports nothing for them

#### Scenario: A model-scoped CASE over aggregates evaluates
- **WHEN** a document has the model-scoped metric `CASE WHEN SUM(orders.amount) > 15 THEN 'hi' ELSE 'lo' END`
- **THEN** a query naming it, with no dimension and grouped by `orders.status`, returns the label the same SQL gives per group on DuckDB and on SQLite

#### Scenario: A CASE field evaluates per row
- **WHEN** a document field `tier` has expression `CASE WHEN amount > 10 THEN 'big' WHEN amount IS NULL THEN 'none' ELSE 'small' END`
- **THEN** grouping by `orders.tier` returns the labels the same SQL gives over the source rows, on DuckDB and on SQLite

#### Scenario: Imported first and last windows keep their window form
- **WHEN** a document declares the fields `FIRST_VALUE(id) OVER (ORDER BY at)` and `LAST_VALUE(id) OVER (ORDER BY at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`, each also written with `DESC` and with an explicit `NULLS FIRST`, over rows whose `at` includes NULL
- **THEN** the stored text keeps every window form, the import reports none as not evaluable, and each field returns the value the window gives under Ossie's NULL placement on DuckDB and on SQLite, which differs from `first(id, at)` / `last(id, at)` on the NULL-keyed row

### Requirement: The corpus import reports only known unevaluable entities
Importing each Ossie corpus document into its own empty datasource SHALL report as not evaluable exactly the entities a committed manifest lists, each with its reason category. Any entity the import newly reports as not evaluable, or no longer reports, SHALL fail the corpus test until the manifest is updated.

#### Scenario: A new not-evaluable entity fails the corpus test
- **WHEN** every corpus document is imported
- **THEN** the set of (document, entity, reason category) the import reports as not evaluable equals the committed manifest
