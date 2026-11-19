## Purpose

How a query reference names what it reads: anchored at a dataset by its first segment and routed to it from the population, a bare name meaning a metric, result keys that never depend on the population, and how pre-2.0 root-relative spellings convert until 3.0.

Scenario seed data, unless a scenario says otherwise (Ossie Layer 3 Appendix B): `customers(customer_id, region)` C1 EAST, C2 EAST, C3 WEST, C4 NORTH; `orders(order_id, customer_id, amount, status)` O1 C1 100 completed, O2 C1 200 open, O3 C2 50 completed, O4 C3 300 open, joined many-to-one to `customers` by an unnamed relationship; `returns(return_id, customer_id, amount)` R1 C1 25, R2 C4 10, joined many-to-one to `customers`; the metric `orders.total_revenue = sum(amount)`; all in one datasource.

## ADDED Requirements

### Requirement: A query reference is anchored at its first segment

In every position of a query (dimensions, time dimensions, metrics and their aggregation arguments, `partition_by=` members, filters, order, `main_time_dimension`), a dotted reference's first segment SHALL name its **anchor**: a dataset of the reading dataset's datasource that is visible to the caller, a stage of the same query list (which precedes a same-named dataset), or the time spine. The remaining segments SHALL be an explicit route from the anchor, each hop spelled by an edge name or a target dataset name as `models/join-traversal` defines, ending at a field, a dataset metric, `*` or a list element. A first segment SHALL be matched by the identifier-case rule of `queries/identifier-case` (exact spelling first, then a unique case-insensitive match; a quoted segment only exactly). An edge name, a dataset of another datasource and a dataset hidden from the caller SHALL NOT be an anchor; such a first segment SHALL fail with a typed error naming it, which for an edge name suggests the spelling `<dataset>.<edge>.<rest>` for each dataset declaring or receiving that edge, and which never reveals a hidden dataset. A stored definition (a field's expression, a dataset filter, a saved metric, a custom aggregation's defaults) SHALL keep reading its references from its declaring dataset and SHALL NOT be anchored this way.

#### Scenario: An Ossie query runs with Ossie's reference meaning
- **WHEN** the query `{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}` runs
- **THEN** the EAST row carries 350 and the WEST row 300, under the keys `customers.region` and `orders.total_revenue`

#### Scenario: The population's own name anchors its own fields
- **WHEN** a query with `population: "orders"` selects `orders.status` and `count(orders.*)`
- **THEN** both read `orders`' own rows with no join

#### Scenario: A stage precedes a same-named dataset
- **WHEN** a query list has a stage named `customers` over `orders` projecting `orders.status`, and a later stage with `population: "customers"` selects `customers.status`
- **THEN** the reference reads the stage's column, not the stored `customers` dataset

#### Scenario: An edge name is not an anchor
- **WHEN** `orders` declares a relationship named `buyer` to `customers`, and a query without a population selects `buyer.region`
- **THEN** it fails with a typed error naming `buyer` and suggesting `orders.buyer.region`

#### Scenario: A dataset of another datasource is not an anchor
- **WHEN** a dataset `events` exists only in another datasource and a query with `population: "customers"` measures `count(events.*)`
- **THEN** it fails with the unknown-dataset error, never as an unrelated product

#### Scenario: A hidden dataset is reported as unknown
- **WHEN** the caller's access context hides `returns` and a query selects `returns.amount`
- **THEN** it fails exactly as for a dataset that does not exist

#### Scenario: First segments follow the identifier-case rule
- **WHEN** a query selects `Customers.region` unquoted, and another selects `"Customers".region` quoted, with only `customers` stored
- **THEN** the first reads `customers.region` and the second fails as an unknown dataset

#### Scenario: Stored definitions stay dataset-local
- **WHEN** `customers` has a field `region_label` with expression `upper(region)`, a dataset filter `region <> 'TEST'`, and a saved metric `aov = sum(orders.amount) / count(*)`, and a query with `population: "orders"` selects `customers.region_label` and `customers.aov`
- **THEN** `region` and `count(*)` inside the definitions read `customers`' own field and rows, and `orders.amount` crosses `customers`' own relationship, exactly as before this change

### Requirement: The anchor is reached by its only to-one path

A query SHALL reach a reference's anchor from its population as follows. When the anchor is the population, the path SHALL be empty. When exactly one path from the population to the anchor has every hop provably to-one on its traversal orientation, that path SHALL be taken, whatever other routes exist. When no such path exists and exactly one simple route exists, that route SHALL be taken. When two or more to-one paths exist, or none exists and two or more routes exist, the reference SHALL fail with a typed ambiguity error naming each executable explicit spelling; a direct relationship SHALL have no precedence over a longer path. When no route exists, the anchor SHALL be an unrelated dataset (`queries/unrelated-datasets`). Routes never cross the time spine. Binding, population inference, population recommendation, schema-drift attribution and the stored-query migration SHALL use this one rule, so they never disagree about which path a reference takes.

#### Scenario: One to-one path wins over fanning routes
- **WHEN** `orders → customers` and `orders → managers` are many-to-one, `managers → customers` is one-to-many, and a query with `population: "orders"` selects `customers.region`
- **THEN** it takes the direct to-one relationship and executes

#### Scenario: Two to-one paths are refused even with a direct relationship
- **WHEN** `orders → stores` and `stores → customers` are also many-to-one, and a query with `population: "orders"` selects `customers.region`
- **THEN** it fails with the ambiguity error naming `orders.customers.region` and `orders.stores.customers.region`

#### Scenario: The explicit route resolves the ambiguity
- **WHEN** the same query selects `orders.customers.region`
- **THEN** it takes the direct relationship and executes

#### Scenario: A unique fanning route is taken
- **WHEN** a query with `population: "customers"` measures `sum(orders.amount)` and the only route from `customers` to `orders` is the reversed `orders → customers` relationship
- **THEN** it takes that route and each customer's value is the sum of its own orders

#### Scenario: Several fanning routes are refused
- **WHEN** `customers` reaches `orders` both by the reversed `orders → customers` relationship and through `managers` with no to-one path, and a query with `population: "customers"` measures `sum(orders.amount)`
- **THEN** it fails with the ambiguity error naming each explicit spelling

#### Scenario: A directed cycle takes its to-one path
- **WHEN** `x → y`, `y → z` and `z → x` are many-to-one and a query with `population: "x"` measures `sum(z.amount)`
- **THEN** it sums the `z` row reached through `x → y → z`, and `sum(x.z.amount)` sums the `z` rows referencing each `x`

#### Scenario: Inference and binding agree
- **WHEN** a query without a population has the dimensions `orders.status` and `customers.region` over the two-path schema above
- **THEN** inference and binding report the same ambiguity, and with `orders.customers.region` in place of `customers.region` both choose `orders`

### Requirement: A bare name is a metric

A bare name in a query SHALL resolve, in order, to an alias declared by a metric or dimension entry of the same query, where such a name is referenceable today (filters, order, and a later stage's columns), and otherwise to the legacy conversion when the query names a population (see "Pre-2.0 root-relative spellings convert when a population is named"). Any other bare name SHALL fail with a typed "unknown metric" error that names it and suggests each qualified field and metric of that name in the reading dataset's datasource.

#### Scenario: An alias is referenced bare
- **WHEN** a query measures `{"metric": "orders.total_revenue", "alias": "revenue"}`, filters `revenue > 100` and orders by `revenue`
- **THEN** both resolve to that entry

#### Scenario: An unknown bare name suggests qualified names
- **WHEN** a query without a population measures `max(amount)`
- **THEN** it fails with the unknown-metric error naming `amount` and suggesting `orders.amount` and `returns.amount`

### Requirement: A row count names its dataset

A row count SHALL be written `count(<anchor>.*)` or with an explicit route, `count(<anchor>.<route>.*)`. In a query that names no population, `count(*)` SHALL fail with a typed error saying that `count(*)` names no dataset and suggesting `count(<dataset>.*)` for the dataset inference would pick, or naming a population, whether or not the query has dimensions.

#### Scenario: A qualified count runs without a population
- **WHEN** the query `{"dimensions": ["customers.region"], "metrics": ["count(orders.*)"]}` runs
- **THEN** the EAST row counts 3 orders and the WEST row 1

#### Scenario: An unqualified count is refused without a population
- **WHEN** queries without a population measure `count(*)`, once with the dimension `customers.region` and once with no dimension
- **THEN** both fail with the typed error, the first suggesting `count(customers.*)`

### Requirement: Result keys never depend on the population

A result key SHALL NOT contain the population unless the reference itself starts with it. An unaliased reference SHALL be keyed by its anchor as written, its route spelled canonically, and its leaf (a field or metric name, an aggregate's derived name such as `amount_sum`, or `_count` for `*`). An entry with an alias SHALL be keyed by the bare alias. An unaliased expression (an arithmetic composite, a transform or a computed dimension) SHALL be keyed by the longest common anchored prefix of its references followed by its sanitized text relative to that prefix (`queries/measure-naming`), and by its sanitized full text when its references share no anchor. Two entries whose anchors differ SHALL be two columns, even when they read the same column; two spellings of one route from the same anchor SHALL be one value (`models/join-traversal` › Paths resolve to one canonical spelling). Response columns, response metadata (including `attributes`), cached results and later stages SHALL all use these keys.

#### Scenario: The same query keys alike under every population
- **WHEN** a query measuring `sum(orders.amount)` and `orders.total_revenue`, grouped by `customers.region`, runs once with `population: "customers"` and once with `population: "orders"`
- **THEN** both responses key the columns `customers.region`, `orders.amount_sum` and `orders.total_revenue`

#### Scenario: A row count keys by its anchor
- **WHEN** a query with `population: "customers"` measures `count(orders.*)` and `count(customers.*)`
- **THEN** the keys are `orders._count` and `customers._count`

#### Scenario: An alias keys as itself
- **WHEN** a query with `population: "customers"` measures `{"metric": "sum(orders.amount)", "alias": "revenue"}`
- **THEN** the key is `revenue`, and `attributes.metrics` names `revenue`

#### Scenario: Expression keys carry their common anchor
- **WHEN** a query measures `sum(orders.amount) / count(orders.*)` and `sum(orders.amount) / count(customers.*)`, unaliased
- **THEN** the first key is `orders.` followed by the sanitized text of `sum(amount) / count(*)`, and the second key is the sanitized text of the whole expression with no prefix

#### Scenario: Different anchors are different columns
- **WHEN** a query with `population: "orders"` selects the dimensions `customers.region` and `orders.customers.region`
- **THEN** the response has the two columns `customers.region` and `orders.customers.region`, with equal values

#### Scenario: A role-playing route keys by its route
- **WHEN** `orders` has relationships named `buyer` and `seller` to `customers`, and a query selects `orders.buyer.region` and `orders.seller.region`
- **THEN** the keys are `orders.buyer.region` and `orders.seller.region`

### Requirement: A later stage reads a stage by its name and keeps its column names

A later stage SHALL reference an earlier stage's columns through the stage's name, as `<stage>.<column>`. A stage's column names SHALL be its result keys with its population's name removed from the front and the remaining dots flattened to `__`. They SHALL equal the column names the same stage had before this change, except for an unaliased expression whose references share no anchor and a reference whose anchor was reached by a route longer than its written path. When two entries of a stage that is read by name flatten to one column name, the stage SHALL fail with `NameCollisionError` naming both entries and suggesting an alias.

#### Scenario: Stage columns keep their names
- **WHEN** stage `s1` with `population: "orders"` selects `orders.status`, `customers.region`, `{"metric": "sum(orders.amount)", "alias": "tot"}` and `count(orders.*)`, and the final stage over `s1` selects `s1.status`, `s1.customers__region`, `s1.tot` and `s1._count`
- **THEN** each reference reads the matching stage column, and the columns are named as before this change

#### Scenario: Two anchors flattening to one stage column are refused
- **WHEN** stage `s1` with `population: "orders"` selects `customers.region` and `orders.customers.region`, and a later stage reads `s1`
- **THEN** it fails with `NameCollisionError` naming both entries and suggesting an alias

### Requirement: Pre-2.0 root-relative spellings convert when a population is named

In fresh input to a query or stage that names its population P, the following SHALL convert to the qualified spelling before anything else reads them, in every position:
- a bare name that is not an alias of the same query becomes `P.<name>`;
- `count(*)` becomes `count(P.*)`;
- a dotted reference whose first segment is not a dataset, a stage or the time spine (an edge of P, a list field of P) gets P prepended.

A dotted reference whose first segment names a dataset or a stage SHALL be read as qualified and SHALL NOT convert. Each distinct converted form SHALL emit exactly one `FutureWarning` and one structured `deprecated_input` warning per input document, naming the old spelling, its qualified twin and SLayer 3.0. A converted query SHALL behave, key its results, and be saved exactly as its qualified twin. A refinement and a cached result SHALL treat an entry and its converted twin as the same entry. A query that names no population SHALL NOT convert anything.

#### Scenario: Legacy bare names convert
- **WHEN** a query with `population: "orders"` selects the dimension `status` and measures `count(*)` and `sum(amount)`
- **THEN** it runs as the query selecting `orders.status` with `count(orders.*)` and `sum(orders.amount)`, with the same keys, and warns once for each of the three forms

#### Scenario: An edge-first path converts
- **WHEN** `orders` has a relationship named `buyer` to `customers` and a query with `population: "orders"` selects `buyer.region`
- **THEN** it runs as `orders.buyer.region`, with a `deprecated_input` warning

#### Scenario: A dataset-first path never converts
- **WHEN** a query with `population: "orders"` selects `customers.region`
- **THEN** it emits no deprecation warning and its key is `customers.region`

#### Scenario: A saved query stores the qualified twin
- **WHEN** a query with `population: "orders"` measuring `sum(amount)` is saved
- **THEN** the stored query measures `sum(orders.amount)`

#### Scenario: A refinement matches a converted entry
- **WHEN** a saved query with `population: "orders"` measuring `sum(orders.amount)` is refined by restating the metric `sum(amount)`
- **THEN** the restatement matches the saved entry: the response has one such column, with the same SQL and columns as the unrefined query, and one deprecation warning

### Requirement: A reference whose meaning changed announces it

When a fresh query that names its population reads a dataset-first reference whose anchor is the target of a relationship incident to the population and spelled with that dataset's name, and the path taken is not that relationship, the response SHALL carry one informational structured warning per reference naming the reference, the path taken and the explicit spelling of the relationship's path. It SHALL NOT be a `FutureWarning`, and it is marked for removal in SLayer 3.0 as `compatibility/removal-tripwire` marks a notice.

#### Scenario: The directed cycle is announced
- **WHEN** `x → y`, `y → z` and `z → x` are many-to-one and a query with `population: "x"` measures `sum(z.amount)`
- **THEN** the response carries the notice naming `z.amount`, the path `x → y → z` and the spelling `x.z.amount`

#### Scenario: An unchanged reference is silent
- **WHEN** a query with `population: "orders"` selects `customers.region` over the seed schema
- **THEN** no notice is emitted
