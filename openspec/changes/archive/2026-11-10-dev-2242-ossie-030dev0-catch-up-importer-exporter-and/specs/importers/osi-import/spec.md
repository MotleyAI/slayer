## MODIFIED Requirements

### Requirement: Current-spec documents import

The importer SHALL accept a flat Ossie document that holds one semantic model at its root (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `custom_extensions`). Each such document SHALL be imported as one semantic model with the current-spec rules, whatever version it declares. The versions `1.0`, `0.1.0`, `0.1.1`, `0.2.0.dev0` and `0.3.0.dev0` SHALL parse without a warning; any other declared version SHALL be warned about and still parsed.

#### Scenario: Flat document imports end to end
- **WHEN** a file holds `version: 0.3.0.dev0`, `name: shop`, two datasets `orders` and `customers` with declared fields, a relationship `orders → customers`, and a model-scoped metric `SUM(orders.amount)`
- **THEN** the import produces models `orders` and `customers`, a many-to-one join from `orders` to `customers`, and the measure on `orders`, with no unconverted entries and no version warning

#### Scenario: Earlier flat document keeps importing
- **WHEN** the same document declares `version: 0.2.0.dev0`
- **THEN** it imports with the same models, join and measure, and no version warning

#### Scenario: Unknown version
- **WHEN** a flat document declares `version: 0.4.0`
- **THEN** a warning names the version and the document imports

### Requirement: Legacy wrapper documents keep importing

A document whose root holds a `semantic_model` key and that does not declare `0.3.0.dev0` SHALL be treated as a legacy document. When `semantic_model` is an array, each element SHALL be imported as its own semantic model; when it is a single object, it SHALL be imported as one semantic model. No model in the wrapper SHALL be dropped.

#### Scenario: Legacy array with two models
- **WHEN** a file holds `semantic_model: [ {name: a, datasets: [...]}, {name: b, datasets: [...]} ]`
- **THEN** the datasets of both `a` and `b` are imported

#### Scenario: Legacy object-valued wrapper
- **WHEN** a file holds `semantic_model: {name: a, datasets: [...]}`
- **THEN** the datasets of `a` are imported

#### Scenario: Wrapper without a version
- **WHEN** a file holds `semantic_model: [...]` and no `version`
- **THEN** its models are imported as legacy and are not reported as declaring any version

### Requirement: Invalid document shapes are skipped with a warning

A file SHALL be skipped with a logged warning, and the rest of the import SHALL continue, when its root mixes both shapes (a `semantic_model` key together with a root `datasets` key), holds neither, holds a `semantic_model` value that is empty, null, or not an array or object, or holds a `semantic_model` key while declaring `version: 0.3.0.dev0`, which admits only flat documents.

#### Scenario: Mixed shape
- **WHEN** a file holds both `semantic_model: [...]` and `datasets: [...]` at its root
- **THEN** that file is skipped with a warning naming it, and other files in the same import are imported

#### Scenario: Empty legacy wrapper
- **WHEN** a file holds `semantic_model: []`
- **THEN** that file is skipped with a warning

#### Scenario: Wrapped 0.3 document
- **WHEN** a file declares `version: 0.3.0.dev0` and holds `semantic_model: [...]`, and another file in the same import is a valid flat document
- **THEN** the wrapped file is skipped with a warning naming it and saying `0.3.0.dev0` documents are flat, and the flat file imports

#### Scenario: Ontology document
- **WHEN** a file is an Ossie ontology or ontology-mapping document (no root `datasets` and no `semantic_model`)
- **THEN** that file is skipped with a warning naming it

### Requirement: Metric name resolution

For a current-spec document, a model-scoped metric SHALL reference fields only as `dataset.field`, and a dataset-scoped metric SHALL reference unqualified fields of its own dataset and, as `source.column`, columns of its own dataset's source (see "Source columns in dataset-scoped metrics"). A reference that is unqualified in a model-scoped metric, qualified or foreign in a dataset-scoped metric (other than a `source.column` reference), names an undeclared field, or names a source column without the `source` qualifier SHALL make the metric reported as unconverted.

#### Scenario: Unqualified reference in a model-scoped metric
- **WHEN** a model-scoped metric is `SUM(amount)` and only `orders` declares `amount`
- **THEN** the metric is reported as unconverted

#### Scenario: Source column in a metric
- **WHEN** a model-scoped metric is `SUM(orders.o_totalprice)` and `orders` declares `total_amount: o_totalprice` but no field `o_totalprice`
- **THEN** the metric is reported as unconverted

#### Scenario: Unqualified source column in a dataset-scoped metric
- **WHEN** dataset `orders` declares `total_amount: o_totalprice` but no field `o_totalprice`, and its metric is `SUM(o_totalprice)`
- **THEN** the metric is reported as unconverted

#### Scenario: Foreign reference in a dataset-scoped metric
- **WHEN** dataset `orders` declares metric `SUM(net_amount) / COUNT(DISTINCT customers.id)`
- **THEN** the metric is reported as unconverted

#### Scenario: Metric reading a renamed field
- **WHEN** dataset `orders` declares `total_amount: o_totalprice` and a model-scoped metric is `SUM(orders.total_amount)`
- **THEN** the measure imports and returns the sum of `o_totalprice`

### Requirement: Legacy documents keep lenient resolution

For a legacy-wrapper document, each model SHALL expose all introspected source columns plus its fields, and metric, key and relationship names SHALL resolve against both, with an unqualified metric reference resolving when exactly one dataset of its semantic model owns that name. Expression, dialect, `FILTER`, datatype and dataset-scoped metric handling SHALL be the same as for current-spec documents, except that a `source.column` reference SHALL NOT be read as a source column.

#### Scenario: Legacy unqualified metric
- **WHEN** a legacy document's metric is `SUM(amount)` and only `orders` has a column `amount`
- **THEN** the measure imports on `orders`

#### Scenario: Legacy relationship on a source column
- **WHEN** a legacy document's relationship uses `from_columns: [customer_id]`, a source column of `orders` that no field declares
- **THEN** the join is created

#### Scenario: Legacy source qualifier
- **WHEN** a legacy document's dataset `orders` declares metric `SUM(source.amount)` and no dataset is named `source`
- **THEN** the metric is reported as unconverted

## ADDED Requirements

### Requirement: Source columns in dataset-scoped metrics

In a dataset-scoped metric of a current-spec document, a two-part reference whose qualifier is `source`, matched case-insensitively whether quoted or not, SHALL read the named column of that dataset's source (a table's columns, or a query source's output columns), matched with the target datasource's identifier folding (a quoted name exactly, an unquoted name as the database folds it). The measure SHALL return the same values as aggregating that physical column, in every position of the metric: aggregate arguments, `FILTER` predicates and percentile orderings. The model's visible columns SHALL remain exactly its declared fields; a metric whose import adds a column for the reference SHALL add it hidden, and a metric reported as unconverted SHALL add no column. The metric SHALL be reported as unconverted, naming the reason, when:

- the dataset declares a field whose name matches `source`, which takes precedence over the qualifier;
- the name matches no column of the source, or several;
- a field of the dataset holds the source column's name but does not read that column unchanged, so the source column cannot be read past it;
- the reference appears in a model-scoped metric.

#### Scenario: Source column no field declares
- **WHEN** dataset `orders` declares fields `order_id` and `amount` and metric `units: SUM(source.quantity)` over rows with `quantity` 2 and 3
- **THEN** measure `units` returns 5 and the visible columns of `orders` are exactly `order_id` and `amount`

#### Scenario: Metric named like the source column
- **WHEN** dataset `orders` declares metric `quantity: SUM(source.quantity)` and no field `quantity`
- **THEN** measure `quantity` imports and returns the sum of the physical `quantity`

#### Scenario: Source column a field passes through
- **WHEN** dataset `orders` declares field `amount: amount` and metric `SUM(source.amount)`
- **THEN** the measure returns the sum of `amount` and the model gains no hidden column

#### Scenario: Qualifier case
- **WHEN** a dataset-scoped metric is `SUM(SOURCE.quantity)`
- **THEN** it imports as `SUM(source.quantity)` does

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
- **THEN** the metric is reported as unconverted and the model gains no column

#### Scenario: Field named source
- **WHEN** dataset `orders` declares a field `source` and metric `SUM(source.quantity)`
- **THEN** the metric is reported as unconverted with a reason saying the field `source` takes precedence

#### Scenario: Source column behind a renamed field
- **WHEN** dataset `orders` has source columns `status` and `state`, declares field `status: state`, and its metric is `COUNT(*) FILTER (WHERE source.status = 'open')`
- **THEN** the metric is reported as unconverted with a reason naming the field `status`

#### Scenario: Source qualifier in a model-scoped metric
- **WHEN** a model-scoped metric is `SUM(source.quantity)` and no dataset is named `source`
- **THEN** the metric is reported as unconverted

### Requirement: Duplicate output labels in query sources

When a dataset's source is a query whose output holds two or more columns with the same label, every field, and every `source.column` reference, reading that label SHALL be reported (the field as dropped, the metric as unconverted) as reading an ambiguous source column. The dataset's other fields and metrics SHALL import.

#### Scenario: Field reading a duplicated label
- **WHEN** a dataset's source is `SELECT o.id, c.id, o.amount FROM o JOIN c ON …` and it declares fields `id: id` and `amount: amount`
- **THEN** field `id` is reported as reading an ambiguous source column and field `amount` imports

#### Scenario: Source column reading a duplicated label
- **WHEN** the same dataset declares metric `COUNT(DISTINCT source.id)`
- **THEN** the metric is reported as unconverted

### Requirement: Upstream examples import

Every file of the upstream Ossie `examples/` directory at the pinned commit SHALL be vendored unchanged. Each core semantic-model example SHALL validate against the pinned schema and import into tables matching its sources with the expected models; its metrics SHALL all import, except those using window functions, which SHALL be reported as unconverted. Each ontology or ontology-mapping example SHALL be skipped with a warning naming it.

#### Scenario: Renamed-fields example
- **WHEN** `renamed_fields.yaml` is imported
- **THEN** models `orders` and `customers` import with no unconverted entries, and `gross_revenue` returns the sum of `o_totalprice`

#### Scenario: TPC-DS example
- **WHEN** `tpcds_semantic_model.yaml` is imported
- **THEN** its five datasets import, and the unconverted entries are exactly its three window-function metrics

#### Scenario: Flights semantic model
- **WHEN** `flights.semantic_model.yaml` is imported
- **THEN** its six datasets import with no unconverted entries

#### Scenario: Ontology examples
- **WHEN** `flights.yaml`, `flights.ontology.yaml` and `flights.mapping.yaml` are imported
- **THEN** each is skipped with a warning naming it
