## Purpose

Keeps the SLayer calls a known 1.8.4 integrator makes working on SLayer 2.x through deprecated forms. Each form warns with its replacement until 3.0, so the integrator can adopt 2.0 without its calls breaking.

## ADDED Requirements

### Requirement: Each kept 1.x call form warns with its replacement

Every call form this capability keeps SHALL emit exactly one deprecation warning of the shared deprecated-input kind when used. The warning SHALL state that the form is removed in SLayer 3.0 and name its replacement. Where the replacement is not a drop-in (a different result shape), it SHALL also name the migration-guide section that explains the change. Each form SHALL be marked for removal in 3.0, so the removal tripwire covers it. A kept form SHALL return, for the integrator's inputs, the value SLayer 1.8.4 returned, except where `docs/upgrading/2.0.md` lists the value as changed in 2.0.

#### Scenario: A kept form names its replacement
- **WHEN** `is_base_column_sql("amount")` is called
- **THEN** it returns `True` with exactly one warning naming `is_base_field_sql` and SLayer 3.0

### Requirement: Four moved modules keep the integrator's names

These old module paths SHALL import, and SHALL serve exactly the listed names, each through its deprecation:
- `slayer.core.formula`: `parse_formula`, `AggregatedMeasureRef`, `ArithmeticField`, `MixedArithmeticField`, `TransformField`.
- `slayer.engine.syntax`: `AggCall`, `Arith`, `ArrayLit`, `BoolOp`, `Cmp`, `DottedRef`, `Literal`, `Placeholder`, `Ref`, `ScalarCall`, `StarSource`, `TransformCall`, `TupleLit`, `UnaryOp`, `parse_filter_expr`.
- `slayer.engine.schema_drift`: `EditModelDelete`, `LiveTable`, `WholeModelDelete`, `compute_datasource_drops`, `data_type_bucket`, `diff_sql_table_model`, `retype_findings`.
- `slayer.sql.column_expansion`: `resolve_ref_target`.

The node classes served from `slayer.engine.syntax` SHALL be the classes the 2.0 parser builds, so an `isinstance` check against them holds for the trees `parse_filter_expr` returns.

#### Scenario: The integrator's imports resolve
- **WHEN** every listed name is imported from its old module
- **THEN** each import succeeds, with one deprecation warning per name naming its new home

### Requirement: Formulas parse into the 1.x result shape

`parse_formula(formula, extra_agg_names=…, named_measures=…)` SHALL parse a saved-measure formula written in the functional spelling into the 1.8.4 result. The four result classes carry the attributes the integrator reads: an aggregated reference's `measure_name` and `aggregation_name`, an arithmetic result's `agg_refs` and `sub_transforms`, and a transform's `inner`. A bare name that `named_measures` defines SHALL be expanded into that measure's constituents, recursively. A name in `extra_agg_names` SHALL be accepted as an aggregation. A formula 1.8.4 refused SHALL raise `ValueError`, and a cyclic sibling chain SHALL raise `RecursionError`. A colon spelling (`amount:sum`) SHALL raise the colon-spelling error, a `ValueError`, because colon spellings were removed in 2.0. Parsing SHALL use SLayer's one Mode-B parser.

#### Scenario: A sibling ratio expands into its constituents
- **WHEN** `aov` = `revenue / order_count` is parsed with `named_measures` `{revenue: "sum(amount)", order_count: "count(*)"}`
- **THEN** the result is an arithmetic result whose aggregated references are `amount` with `sum` and `*` with `count`

#### Scenario: A custom aggregation is accepted
- **WHEN** `spread(amount)` is parsed with `extra_agg_names` `{"spread"}`
- **THEN** the result is an aggregated reference to `amount` with `spread`

#### Scenario: A colon spelling is refused as a ValueError
- **WHEN** `amount:sum` is parsed
- **THEN** a `ValueError` naming the functional spelling `sum(amount)` is raised

### Requirement: Filters parse into the 1.x tree shape

`parse_filter_expr(text)` SHALL parse a filter 1.8.4 accepted into a tree that a recursive walk over each node's declared fields reads using only the fourteen node classes listed for `slayer.engine.syntax`. A reference SHALL carry `Ref.name`, and a dotted reference `DottedRef.parts`, as in 1.8.4. Constructs 2.0 represents with other node classes SHALL appear in the tree in the shape 1.8.4 gave them. Examples are named arguments, temporal literals, casts and `date_trunc`. A filter 1.8.4 refused SHALL raise `ValueError`.

#### Scenario: The integrator's filters walk to their references
- **WHEN** the filters `status = 'paid'`, `customers.country IN ('DE', 'FR')`, `sum(amount) > 100`, `count(customers.id) > 1`, `contains(tags, 'dbt')`, `tags.value = 'dbt'` and `rank(sum(amount), partition_by=customers.email) <= 1` are parsed and walked
- **THEN** every node is one of the fourteen classes, and the references found are those 1.8.4 found

### Requirement: Removed helpers and renamed keywords keep working

These forms SHALL work with their 1.8.4 results:
- `slayer.core.query.has_variable_syntax(text)`;
- `slayer.core.models.is_base_column_sql(sql)`;
- `is_identifier(column=, columns=)`;
- `resolve_ref_target(qualifiers=, source_model=, models_by_name=)`.

#### Scenario: Variable syntax is detected as in 1.8.4
- **WHEN** `has_variable_syntax` is called on `status = {s}`, `status = '{s}'` and `status = 'paid'`
- **THEN** it returns what 1.8.4 returned for each

#### Scenario: A list element resolves through its old keywords
- **WHEN** `resolve_ref_target(qualifiers=("orders", "tags"), source_model=orders, models_by_name=…)` is called
- **THEN** it resolves to the `tags` list column of `orders`

### Requirement: Drift entry points take their 1.x keywords and results

The drift entry points SHALL accept their 1.8.4 keywords:
- `diff_sql_table_model(model=, live_table=, available_models_in_ds=)`;
- `compute_datasource_drops(models=, sql_table_diffs=, sql_diffs=)`;
- `retype_findings(model=, live_table=)`.

Their results SHALL answer the 1.8.4 attribute names:
- a drop's `model_name`;
- a removal's `columns` and `measures`;
- `join_edges`, whose entries dump with the keys `target_model`, `join_pairs` and `name`;
- a re-type finding's `column`.

`LiveTable(columns=…)`, `data_type_bucket` and the `isinstance` checks against `EditModelDelete` and `WholeModelDelete` SHALL work as in 1.8.4.

#### Scenario: A dropped column's removal reads in 1.x names
- **WHEN** the integrator's fixture dataset is diffed against a live table without `amount`, and the datasource drops are computed with the 1.x keywords
- **THEN** the edit drop's `model_name` is `orders`, its removal's `columns` include `amount` and `paid_amount`, its `measures` include `revenue`, `aov` and `paid_revenue`, and the same removal set as 1.8.4 is reported

#### Scenario: A re-type finding reads its column
- **WHEN** a live table types `amount` as text
- **THEN** `retype_findings(model=…)` returns a finding whose `column` is `amount`

### Requirement: Legacy DataType names resolve to their 2.0 members

`DataType.INT`, `DataType.DOUBLE`, `DataType.TEXT` and `DataType.TIMESTAMP` SHALL resolve to the members the stored-document migration maps those 1.8.4 types to. They SHALL not be members: iteration, `__members__`, validation, the generated schema and pickling of `DataType` SHALL be as without them. Each lookup SHALL warn once.

#### Scenario: A legacy name is the 2.0 member
- **WHEN** `DataType.INT` is looked up
- **THEN** it is `DataType.INTEGER`, with one warning naming `DataType.INTEGER`, and `INT` is not in `DataType.__members__`

### Requirement: Queries build from column-reference objects

A time dimension built as `TimeDimension(dimension=ColumnRef(name=…), granularity=…)` SHALL be accepted with a deprecation, as the reference text of the column-reference object. So SHALL an order item built as `OrderItem(column=ColumnRef(name=…), direction=…)`. This holds inside `SlayerQuery(time_dimensions=…, order=…)` and in a distinct-values query with a granularity.

#### Scenario: Every granularity renders
- **WHEN** the integrator's query buckets `created_at` by each of `second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter` and `year`, ordered by a saved measure descending, and is rendered as a dry run on DuckDB
- **THEN** each renders, and the result columns are `orders.created_at` and the measure's `orders.<name>`

### Requirement: Dry-run rendering through a 1.x read-only backend

A storage backend written against the 1.8.4 hook names, refusing every write and holding validated v15 models, SHALL let `SlayerQueryEngine(storage, policy=None, clock=…)` render the integrator's queries with `execute_sync(query, dry_run=True)`. The queries cover saved measures by name, root-dataset grouping, filters, order, distinct values, and `tags.value` with `to_many_handling="associate"`. Rendering SHALL work on DuckDB, BigQuery, Snowflake, Postgres and Redshift. `response.sql` SHALL carry the statement, and `response.columns` the result columns named `model.column`, which BigQuery's `emit_alias` spells `model___column`. Executed on DuckDB and Postgres, each statement SHALL return the expected rows. A list-element query on Redshift SHALL be refused with the list-type error, as in 1.8.4.

#### Scenario: The query set renders on every supported dialect
- **WHEN** the integrator's query set is rendered as a dry run for each of the five dialects
- **THEN** every query renders except the Redshift list-element query, which is refused, and every result column is named as in 1.8.4

#### Scenario: Rendered statements return the expected rows
- **WHEN** the DuckDB and Postgres statements are executed against the fixture data
- **THEN** each returns the expected rows

### Requirement: Model objects read through their 1.x names

A v15 model document SHALL validate through `SlayerModel.model_validate` into the 2.0 dataset, upgraded in memory. Its parts SHALL answer the 1.8.4 names the integrator reads, each with its deprecation:
- `columns`, `measures`, `joins` and `get_column`;
- `formula`, `type`, `target_model` and `join_pairs`;
- `awaits_columns`;
- `join_key_error(model=, target=, key=, side=, columns=)`.

These SHALL keep their 1.8.4 results:
- `normalize_aggregation_name`, `PRIMARY_KEY_AGGREGATIONS`, `TimeGranularity` and `physical_column_sql`;
- `DatasourceConfig(name=, type=)`;
- `dialect_for_ds_type`;
- the `slayer.sql.dialects` and `slayer.sql.scope` imports.

#### Scenario: A v15 model reads through its old names
- **WHEN** the fixture's v15 `orders` document is validated with `SlayerModel.model_validate` and its joins, columns and measures are read
- **THEN** each read returns the 1.8.4 value with its deprecation warning, and the join's `target_model` is `customers`

#### Scenario: Upgraded files pass a version and key check
- **WHEN** the fixture's v15 documents are upgraded with the entry point for documents SLayer does not own
- **THEN** each upgraded document's `version` equals `CURRENT_VERSIONS["SlayerModel"]`, and a recursive walk finds no key the dataset model does not declare at its level
