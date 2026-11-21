## Purpose

Defines model-scoped metrics — metrics of a datasource's semantic model rather than of one dataset — how a query names them by bare name, how they evaluate, and how `AGG(dataset.metric)` evaluates a dataset metric in every position.

## ADDED Requirements

### Requirement: A bare name resolves as alias, then model-scoped metric
A bare name in a query's metric position SHALL resolve, in order, to a query alias, then to a model-scoped metric of the query's datasource, then by the legacy conversion of a query that names a population, and otherwise SHALL fail with the unknown-metric error suggesting qualified names. The same order SHALL apply in binding, population inference and `recommend_population`. Matching SHALL follow `queries/identifier-case`. A model-scoped metric SHALL be usable wherever a query metric is: as a metric entry, inside a metric expression (`revenue_per_customer * 100`), in a measure-typed filter and as an order key.

#### Scenario: A bare model-scoped metric in a root-less query
- **WHEN** on Appendix B.1 data a query asks for the metric `revenue_per_customer`, defined as `AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)` with `orders.total_revenue = SUM(amount)`
- **THEN** the result is one row with `revenue_per_customer` = 162.5 (650 / 4)

#### Scenario: An alias shadows a model-scoped metric
- **WHEN** a query aliases `sum(orders.amount)` as `revenue_per_customer` and also names `revenue_per_customer`
- **THEN** the bare name resolves to the alias

#### Scenario: Neither alias nor metric
- **WHEN** a query names the bare metric `nope` and no alias or model-scoped metric has that name
- **THEN** the query fails with the unknown-metric error suggesting qualified names

### Requirement: A model-scoped metric evaluates as its expression in place
A model-scoped metric SHALL evaluate exactly as its expression written in its place in the query: each aggregate call SHALL be one component, homed as the same aggregate written in a query (semantics Axiom 2.4); an aggregate reading several datasets or filtering on another dataset, which Ossie rejects, SHALL evaluate as it does in a query, SLayer being a superset of Ossie, and the scalar expression SHALL be evaluated from the component values after they are combined. Its result SHALL be keyed and named by its bare name unless the query aliases it. It SHALL return the same values as the equivalent inline expression in every query, whatever row rule the query follows.

#### Scenario: Inline equality
- **WHEN** one query names `revenue_per_customer` by `customers.region` and another names `sum(orders.amount) / count(distinct customers.customer_id)` by `customers.region` on the same data
- **THEN** both return the same value in every row

#### Scenario: An aggregate reading two datasets evaluates
- **WHEN** a document's model-scoped metric `net` has expression `SUM(orders.amount - customers.discount)` over the to-one relationship `orders → customers`
- **THEN** a query naming `net` returns the same value as the inline `sum(orders.amount - customers.discount)`

### Requirement: References inside a model-scoped metric read like a query
Inside a model-scoped metric's expression, a dotted reference `d.f` SHALL be a field anchored at dataset `d` of the same document, resolved by the query reference rules; a bare name SHALL be a model-scoped metric; `AGG(d.m)` SHALL evaluate the dataset metric `d.m`. A bare name that is no model-scoped metric, a reference to a source column (including `source.x` unless a dataset is named `source`), a reference to a dataset of another document, and a bare dataset metric reference without `AGG` SHALL be refused when the metric is saved through SLayer, each with a typed error. A model-scoped metric referring to another model-scoped metric, which Ossie does not define, SHALL be stored only in the document's `SLAYER` extension and SHALL NOT appear in the document's core `metrics`. A model-scoped metric SLayer authors whose aggregate reads several datasets or filters on another dataset SHALL likewise be stored only in the document's `SLAYER` extension.

#### Scenario: An unqualified field reference is refused at save
- **WHEN** `edit_semantic_model` adds a model-scoped metric with expression `SUM(amount)`
- **THEN** the save fails with a typed error stating that model-scoped metric references are qualified

#### Scenario: A metric over another metric stays SLayer-only
- **WHEN** `edit_semantic_model` adds the metric `arpu_pct` with expression `revenue_per_customer * 100`
- **THEN** the metric answers by bare name, and the document's core `metrics` do not contain `arpu_pct`

### Requirement: Model-scoped metric names and definitions are checked at save
Saving a model-scoped metric through SLayer SHALL be refused, with a typed error naming the metric, when its name collides with a reserved word, a library function name or a dataset name of the datasource, or when its definition is circular through other model-scoped metrics.

#### Scenario: A circular definition is refused
- **WHEN** model-scoped metrics `a` (`b + 1`) and `b` (`a + 1`) are saved together
- **THEN** the save fails with a typed error naming the cycle

#### Scenario: A name equal to a library function is refused
- **WHEN** a model-scoped metric named `sum` is saved
- **THEN** the save fails with a typed error naming `sum`

### Requirement: AGG evaluates a dataset metric as one component
`AGG(d.m)` SHALL evaluate the dataset metric `m` of dataset `d` as one component homed on `d`, returning the same values as the metric reference `d.m`. It SHALL be accepted in model-scoped metrics, in query expressions, and in stored dataset metrics, where its argument follows that definition's own reference rules (`AGG(m)` for a metric of the same dataset, `AGG(path.m)` across to-one relationships). Its argument SHALL be a single reference to a dataset metric; `AGG` with a `FILTER` clause, `AGG` of a field, `AGG` of a model-scoped metric and `AGG` of any other expression SHALL each be refused with a typed error, at save for a stored definition and before SQL generation for a query. A stored dataset metric using `AGG`, which Ossie does not define in that position, SHALL be stored only in its entity's `SLAYER` extension when it belongs to a document.

#### Scenario: AGG in a query equals the metric reference
- **WHEN** a query asks for `AGG(orders.total_revenue)` and another for `orders.total_revenue`, both by `customers.region`
- **THEN** both return the same values

#### Scenario: AGG of a field is refused
- **WHEN** a query asks for `AGG(orders.amount)` where `amount` is a field
- **THEN** the query fails with a typed error stating that AGG takes a dataset metric

#### Scenario: AGG with FILTER is refused
- **WHEN** a model-scoped metric `AGG(orders.total_revenue) FILTER (WHERE orders.status = 'paid')` is saved
- **THEN** the save fails with a typed error stating that AGG takes no FILTER clause

### Requirement: Inspect and summaries show model-scoped metrics
`inspect` SHALL accept a bare model-scoped metric name with `entity_type="metric"` and show its expression, description, `ai_context`, owning document and any load warning; `datasets_summary` SHALL list a datasource's model-scoped metrics.

#### Scenario: Inspecting by bare name
- **WHEN** `inspect(reference="revenue_per_customer", entity_type="metric")` is called
- **THEN** the response shows the metric's expression and its document `sales`
