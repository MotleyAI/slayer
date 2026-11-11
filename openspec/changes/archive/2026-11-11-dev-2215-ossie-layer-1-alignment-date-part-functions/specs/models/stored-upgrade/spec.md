## MODIFIED Requirements

### Requirement: Library growth never invalidates a stored artefact

Every name the Mode-B library reserves — scalar and list functions, function spellings, date-part aliases, transforms, built-in granularities, built-in aggregations, and names reserved with no meaning (`agg`) — SHALL be pinned, so a newly reserved name is only accepted when it is a function or function spelling (which a model's own definition shadows) or when a stored-upgrade migration covers stored documents already using it. A stored model whose custom aggregation takes any reserved function or spelling name SHALL load and answer its aggregation, and a stored datasource whose custom granularity takes any reserved name SHALL load after migration and answer through the renamed granularity.

#### Scenario: Every reserved function name as a stored aggregation

- **WHEN** for each reserved function and spelling name a stored model defines a custom aggregation of that name with formula `SUM({value})`
- **THEN** each model loads and the aggregation returns the sum of its column

#### Scenario: Every reserved name as a stored granularity

- **WHEN** for each reserved name other than a built-in granularity (never storable as a custom one) a stored datasource defines a custom granularity of that name used by a stored model's time dimension
- **THEN** the datasource and model load and the time dimension buckets by the renamed granularity

#### Scenario: An unpinned reservation is caught

- **WHEN** the library reserves a name outside the pinned set that is neither a function nor a function spelling and no migration covers it
- **THEN** the library-growth check fails, naming the unpinned name

## ADDED Requirements

### Requirement: Stored bucket calls are upgraded to date_trunc
On load, a stored model, saved query or memory written before this change SHALL have every Mode-B call `<g>(x)` whose callee names a built-in granularity, or a custom granularity of the datasource it belongs to, rewritten to `date_trunc('<g>', x)` — in query measures, filters, dimensions, time dimensions, order keys and `main_time_dimension`, in saved-measure formulas, and in every nested saved query, inline source model, model extension and query-backed model — with the rest of the text byte-identical. Mode-A text (`Column.sql`, `sql_table`, model `filters`, custom-aggregation formulas and parameter defaults), structured time dimensions, transform units and `Column.granularity` SHALL be untouched. A call whose callee is a custom aggregation of the owning model and not a granularity SHALL be untouched. Every upgraded query SHALL return the rows and result keys it returned before. The custom-granularity rewrite SHALL use the datasource's settled granularity names (after any library-collision rename) and SHALL complete on the next load when a load is interrupted.

#### Scenario: Saved query bucketing by month
- **WHEN** a stored model's saved query has the dimension `month(created_at)`, the filter `year(created_at) >= '2024'` and the order key `month(created_at)`
- **THEN** after load they read `date_trunc('month', created_at)`, `date_trunc('year', created_at) >= '2024'` and `date_trunc('month', created_at)`, and the query returns the rows and result keys it returned before

#### Scenario: Custom granularity in a memory
- **WHEN** a stored datasource defines `fiscal_year` and a stored memory's query buckets `fiscal_year(order_date)`
- **THEN** after load the memory's query reads `date_trunc('fiscal_year', order_date)` and returns its earlier rows

#### Scenario: Custom aggregation is not a bucket
- **WHEN** a stored model defines the custom aggregation `fiscal_total` and a saved measure `fiscal_total(amount)`
- **THEN** the measure loads unchanged

#### Scenario: Mode-A text is untouched
- **WHEN** a stored column's `sql` is `month(created_at)` and a model filter is `year(created_at) > 2020`
- **THEN** both load unchanged

### Requirement: Stored deprecated spellings are upgraded
On load, a stored model, saved query or memory written before this change SHALL have each deprecated Mode-B spelling (`queries/expression-grammar` › Legacy spellings keep their meaning) replaced by its canonical twin in every Mode-B field and nested document, with the rest of the text byte-identical and no `FutureWarning`. The colon aggregation spelling is outside this requirement.

#### Scenario: Python spellings in a saved measure
- **WHEN** a stored saved measure is `sum(iif(status == 'paid' and region is not None, amount, None))`
- **THEN** it loads as `sum(iif(status = 'paid' and region IS NOT NULL, amount, NULL))` and returns the same value

#### Scenario: Method call and list after IN in a saved query
- **WHEN** a stored saved query has the measure `order_items.sum(price)` and the filter `region in ['EU', 'US']`
- **THEN** they load as `sum(order_items.price)` and `region in ('EU', 'US')`

### Requirement: Stored agg aggregations are renamed
On load, a stored model written before this change whose custom aggregation is named `agg` in any case SHALL have it renamed to `agg_aggregation`, and every Mode-B call `agg(` in any case in that model and in the saved queries, query-backed models and memories of the same store SHALL be rewritten to `agg_aggregation(`, with one logged warning naming the model and both names. When the model already defines `agg_aggregation`, the aggregation SHALL be left unrenamed with a logged warning, and calls to it fail per `models/aggregation-names` › agg is reserved for measure evaluation.

#### Scenario: Aggregation named agg
- **WHEN** a stored model defines the aggregation `agg` with formula `SUM({value}) * 2` and a saved query of the store measures `agg(amount)`
- **THEN** after load the aggregation is `agg_aggregation`, the saved query reads `agg_aggregation(amount)`, a warning names the rename, and the query returns twice the sum as before

#### Scenario: The new name is taken
- **WHEN** a stored model defines both `agg` and `agg_aggregation`
- **THEN** it loads with a warning, `agg` unrenamed, and a query calling `agg(amount)` fails with the reserved-name error
