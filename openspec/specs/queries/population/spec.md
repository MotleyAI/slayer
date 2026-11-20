# queries/population Specification

## Purpose
The query population — the quantifier deciding which dimension combinations exist as
result rows: its explicit declaration via `population`, the dimension-determined
inference default when it is omitted, the fail-closed ambiguity rules, response
reporting, and the aligned root-recommendation surface.

## Requirements

### Requirement: Population may be omitted

`SlayerQuery.population` SHALL be optional. A query naming it — as a stored model or stage name,
with or without `extensions` — is **anchored**: it SHALL return the rows and values it returned
before the row-supply rule, its aggregates reading their grain dimensions and filters from their
homes (`queries/references`), and the named population MAY be a bridge model owning none of the
queried items. A query omitting it is **inferred**: its rows SHALL be its row supply ("Rows of an
inferred query are the row supply"). `population_inferred: true` beside a named `population`
SHALL make the query inferred with that dataset as its dimension anchor; `population_inferred:
true` without `population` SHALL fail with a typed validation error.

#### Scenario: Explicit population unchanged

- **WHEN** a query names `population` explicitly
- **THEN** it returns the rows and values it returned before the row-supply rule, and no inference runs

#### Scenario: Omitted population infers

- **WHEN** a query omits `population`
- **THEN** its rows are its row supply and its dimension anchor is inferred from its dimensions

#### Scenario: A pinned anchor keeps the rows inferred

- **WHEN** a query has `population: "customers"`, `population_inferred: true`, the dimension
  `customers.region` and the metric `orders.total_revenue`, over the seed of "Rows of an inferred
  query are the row supply"
- **THEN** the rows are EAST 350 and WEST 300, exactly as the same query without `population`

#### Scenario: The inferred marker needs a population

- **WHEN** a query sets `population_inferred: true` without `population`
- **THEN** it fails with a typed validation error naming `population_inferred`

### Requirement: Dimension-determined default

For a query omitting `population`, the **dimension anchor** SHALL be the model with the
fewest total routed join hops among viable candidates, where a candidate is viable
iff every determination item binds from it under the prevailing reference grammar
along a routed chain whose every oriented hop is provably to-one (declared
many-to-one/one-to-one, or unique-key covered) or of unknown cardinality, an unknown hop
being read by association; in raw-row mode an unknown hop SHALL NOT count.
Viability probing SHALL apply, per candidate, the same anchor routing binding
applies after selection (`queries/references` › The anchor is reached by its only
to-one path): an item anchored at the candidate has an empty path; otherwise the
candidate's routes to the item's anchor are enumerated with no precedence for a
relationship incident to the candidate; every hop of a selected route, and of the
item's explicit route after its anchor, MUST satisfy the hop rule above, and hops are
counted along the full path. An item spelled by its anchor and the same item spelled as
its full path from a candidate SHALL yield the same dimension anchor, or fail closed
identically.
Determination items SHALL be the query's dimensions and time dimensions only (for
computed dimensions: their row-valued references and `partition_by` references,
never references inside an aggregation). Filters, measures, references resolving to
saved measures, order entries, and model-level filters SHALL contribute nothing to the
dimension anchor. A reference introduced only by a variable value SHALL NOT participate.
Time dimensions on `time_spine.timestamp` SHALL NOT be determination items: they factor out
of the population as its spine factor (`queries/time-spine`), and this rule
infers only the factor P from the remaining items.
When no candidate is viable and the determination items fall into groups with no join path
between any two of them, the dimension anchor SHALL be the product of each group's own
dimension anchor. With no determination items the dimension anchor SHALL be the unit, one row
with no columns. Tied minimal candidates, or connected items no candidate determines, leave the
dimension anchor undefined.

#### Scenario: Canonical default picks the coarse side

- **WHEN** `dimensions=[customers.region]` and `metrics=[sum(orders.total)]` are
  queried without `population`, with orders joined many-to-one to customers
- **THEN** the dimension anchor is `customers`, while the rows are the regions the orders reach:
  a region with no orders is not a row

#### Scenario: A field filter is not a determination item

- **WHEN** a query without `population` has `dimensions=[customers.region]` and a
  field-typed filter on `orders.status`
- **THEN** the filter is not a determination item: the dimension anchor stays `customers`, and the
  filter restricts `customers` by association

#### Scenario: Measure-typed filter does not participate

- **WHEN** a query without `population` adds a filter referencing an aggregation
  or a saved measure
- **THEN** the dimension anchor is the same as without that filter

#### Scenario: Population invariant under measures

- **WHEN** a measure is added to or removed from a query without `population`
- **THEN** the dimension anchor is unchanged (or the added measure fails to bind — never a
  different anchor)

#### Scenario: A to-many path is not determination

- **WHEN** the only route from a candidate to a queried dimension crosses a hop
  whose oriented cardinality is to-many
- **THEN** that candidate is not viable

#### Scenario: Short-form dimension infers like its full path

- **WHEN** a query without `population` has `dimensions=[regions.name,
  orders.status]` where `regions` is reachable from `orders` only through
  `customers`
- **THEN** inference routes `regions.name` per candidate and picks the same
  dimension anchor as the query spelled with `customers.regions.name`

#### Scenario: Literal resolution beats short-form reinterpretation

- **WHEN** a candidate reaches a determination item's anchor both by a direct
  many-to-one relationship and by another to-one path
- **THEN** the item is ambiguous from that candidate, which is not viable for it;
  the direct relationship has no precedence

#### Scenario: Unique but fanning route is not determination

- **WHEN** the only route from a candidate to an item's anchor is unique in the
  join graph but crosses a to-many hop
- **THEN** that candidate is not viable, even though binding would route the same
  reference for other purposes

#### Scenario: Raw-row mode infers from the dimensions without unproven hops

- **WHEN** a `distinct_dimension_values=false` query omits `population`
- **THEN** the dimension anchor is inferred from the dimensions alone, an unknown-cardinality hop
  not counting, and the result has one row per row of the anchor; a field filter on a finer
  dataset restricts those rows and never changes the anchor

#### Scenario: Spine time dimensions factor out

- **WHEN** a query without `population` groups by `customers.region` and a month
  time dimension on `time_spine.timestamp`, with orders and returns each joined
  many-to-one to customers and each wired to the spine
- **THEN** P is `customers` (not `orders` or `returns`, which also determine the
  spine), and the population is `time_spine × customers`

#### Scenario: A directed cycle needs an explicit population

- **WHEN** `x`, `y` and `z` are joined many-to-one in a directed cycle `x → y → z → x`
  and a query without `population` has `dimensions=[x.v, y.v, z.v]`
- **THEN** the dimension anchor is undefined and the dimension-only query fails closed,
  because `x`, `y` and `z` each determine every item along their to-one paths with the same
  number of hops; the same query with `population="x"` executes, reading `z.v` through
  `x → y → z`

#### Scenario: Unconnected dimension groups take a product

- **WHEN** a query without `population` has `dimensions=[customers.region, tickets.priority]`,
  no join path connects `tickets` to `customers`, and it has no metrics
- **THEN** the dimension anchor is `customers × tickets` and the rows are every region paired
  with every priority

#### Scenario: An unknown-cardinality hop counts by association

- **WHEN** `orders` joins `customers` by a relationship with no declared cardinality and no
  covering unique key, and a query without `population` has `dimensions=[orders.status,
  customers.region]` and no metrics
- **THEN** the dimension anchor is `orders` and the rows are the (status, region) pairs the
  orders reach; with `distinct_dimension_values=false` the same query fails closed

### Requirement: Inference fails closed

A query omitting `population` SHALL raise a typed population-inference error — with a stable
message prefix and the reason, candidates, and datasources in its payload — when it needs an
undefined dimension anchor: several viable candidates tie at the minimum (named), connected
determination items no candidate determines (per-candidate reasons), or a determination item's
join path is ambiguous from every otherwise-viable candidate. A query needs its dimension anchor
when it has no metrics, when a metric has no full-grain aggregate operand, or when it lists raw
rows. A query whose every metric has a full-grain operand SHALL execute whatever its dimension
anchor, reporting `population` as `null` when that anchor is undefined. Datasource scoping finding
zero or several candidate datasources SHALL fail closed (named). A query with metrics and no
dimensions SHALL NOT raise an error for lack of determination items: its rows are the unit. A
query whose only dimensions are on the spine SHALL NOT raise the nothing-to-infer error: its P is
the one-row unit. A stage query omitting `population` whose dimensions, filters or metrics are anchored at sibling
stage names SHALL fail closed with a diagnostic naming the sibling.

#### Scenario: Tie names the candidates

- **WHEN** two viable candidates tie at the minimal total routed hops and the query has no
  metrics
- **THEN** the error names both and asks for an explicit `population`

#### Scenario: Nothing to infer from

- **WHEN** a query without `population` has only measures (no dimensions)
- **THEN** no error is raised and the result is exactly one row

#### Scenario: Spine-only query has the unit as P

- **WHEN** a query without `population` has only a spine time dimension with a
  `date_range` and measures
- **THEN** no inference error is raised and the population is `time_spine`

#### Scenario: Sibling-anchored stage refs

- **WHEN** a stage in a multi-stage query omits `population` and its dimensions, filters or metrics
  are anchored at a sibling stage's name
- **THEN** the error tells the author to name that sibling as the stage's
  `population`

#### Scenario: An undefined anchor a query does not need

- **WHEN** a query without `population` has dimensions whose minimal candidates tie, and only
  metrics whose homes determine every dimension
- **THEN** it executes, its rows are the metrics' cells, and it reports `population` `null`

### Requirement: Datasource scoping for root-less queries

For a query omitting `population`, the candidate datasources SHALL be those
containing every referenced anchor name that matches a saved model — the anchors of its
dimensions, field-typed filters and metrics, a saved metric counted by the datasets its
definition reads; an explicit execution-level datasource argument SHALL pin scoping to that
datasource before anchor voting. A filter reference that resolves to a saved measure —
including one reached through a named join — SHALL NOT contribute an anchor through the filter
to datasource scoping or to sibling-stage detection. Exactly one candidate datasource SHALL
remain, else inference fails closed naming them.

#### Scenario: Anchor intersection resolves the datasource

- **WHEN** the referenced model names all exist together in exactly one datasource
- **THEN** inference proceeds within that datasource

#### Scenario: Ambiguous datasources fail closed

- **WHEN** two datasources both contain every referenced model name and no
  datasource argument is given
- **THEN** the error names both datasources

#### Scenario: Saved-measure filter does not steer datasource scoping

- **WHEN** a rootless query's only reference to a foreign model is a filter that
  resolves to a saved measure reached via a named join
- **THEN** that reference contributes no anchor and datasource scoping is the same
  as without the filter

#### Scenario: Saved-measure name colliding with a sibling stage

- **WHEN** a stage query omitting `population` has a filter resolving to a saved
  measure whose model name equals a sibling stage's name, and its dimensions are
  anchored elsewhere
- **THEN** the sibling-stage diagnostic is not raised; inference proceeds from the
  dimensions' anchors

#### Scenario: A measures-only query scopes by its metrics

- **WHEN** a query without `population` and without dimensions measures `orders.total_revenue`,
  and `orders` exists in exactly one datasource
- **THEN** it runs in that datasource

#### Scenario: Metric homes in two datasources fail closed

- **WHEN** a query without `population` measures `orders.total_revenue` and `count(events.*)`,
  and no datasource holds both `orders` and `events`
- **THEN** it fails closed naming the datasources involved

### Requirement: Population is reported

Every successful model-rooted query response SHALL report the effective population and
whether it was inferred, uniformly across normal execution, dry-run, explain, cache hits,
refresh, and the Python client's response model. An anchored query SHALL report its named
population. An inferred query SHALL report its dimension anchor, written `a × b` for a product,
and `null` when the anchor is the unit or undefined. A spine population SHALL be reported as
`time_spine × <P>` when P is a model and as `time_spine` when P is the unit; the inferred flag
SHALL be true iff the query is inferred (a spine-only query counts as inferred unless it named
`population: time_spine`). Every response SHALL also carry `row_sources`, a sorted, deduplicated
list of canonical dataset names (a stage by its query-local name): for an inferred query, the
home of every full-grain aggregate supplying rows plus each dimension-anchor dataset whose cells
are supplied, and exactly `["unit"]` when the query has no dimensions; for an anchored query, its
population; for a spine query, P only. The MCP query tool SHALL print the population and the row
sources only when the query is inferred.

#### Scenario: Inferred population reported

- **WHEN** a query's population was inferred
- **THEN** the response carries the dimension anchor and an inferred flag set true

#### Scenario: Explicit population reported

- **WHEN** a query named its population explicitly
- **THEN** the response carries that model name with the inferred flag false, and `row_sources`
  is that model alone

#### Scenario: Spine population reported

- **WHEN** a query groups by `customers.region` and a spine month without
  `population`, and another names `population: customers` for the same shape
- **THEN** both report population `time_spine × customers`, the first with the
  inferred flag true and the second false, and both report `row_sources` `["customers"]`

#### Scenario: Row sources of an inferred query

- **WHEN** over the seed of "Rows of an inferred query are the row supply", a query without
  `population` groups by `customers.region` and measures `orders.total_revenue` and
  `returns.total_returns`; and another measures only `orders.total_revenue` with no dimensions
- **THEN** the first reports `population` `customers` and `row_sources` `["orders", "returns"]`;
  the second reports `population` `null` and `row_sources` `["unit"]`

#### Scenario: A product anchor is reported by its parts

- **WHEN** the query of "Unconnected dimension groups take a product" runs
- **THEN** it reports `population` `customers × tickets` and `row_sources`
  `["customers", "tickets"]`

#### Scenario: Every surface carries the row sources

- **WHEN** an inferred query runs normally, as a dry run, with explain, from the cache, after a
  refresh, over REST and through the Python client
- **THEN** every response carries the same `population`, `population_inferred` and `row_sources`

### Requirement: Root-less queries reach every query surface

The REST query endpoint and the MCP query tool SHALL accept a query without
`population` and run inference end to end.

#### Scenario: REST rootless query

- **WHEN** a REST query body omits `population`
- **THEN** the query executes and the response reports the inferred population

#### Scenario: MCP rootless query

- **WHEN** the MCP query tool is called without `population`
- **THEN** the query executes and the response reports the inferred population

### Requirement: Root recommendation follows the population rule

`recommend_population` SHALL classify each resolved item by entity type: columns
are determination items (determined, counting routed hops); saved measures,
aggregation-suffixed items and row counts `count(<dataset>.*)` are attachments
requiring reachability only and reported as such, a row count being an attachment
on its anchor. An unqualified `count(*)` SHALL be refused with the typed error of
`queries/references` › A row count names its dataset, and a bare name with the
unknown-metric error, each suggesting the qualified spellings. Selection SHALL use
the same rule as the dimension anchor so the two
surfaces cannot disagree whenever the anchor is defined: a column item is determined
from a candidate exactly when the dimension-anchor probe finds it viable from that
candidate. Only when no candidate is viable and an item's probe failed solely because
a hop or its route is ambiguous — never fanning or unreachable — SHALL
recommendation resolve that item along its tie-break-best fan-out-free route and emit
that full canonical path, so the recommended root executes once named as
`population`. When the items include at least one determination item, an attachment
on a dataset that no join path connects to a candidate SHALL NOT count against that
candidate; once a root is chosen, each such attachment SHALL be reported with the
spelling a query rooted there uses (its anchor, `queries/unrelated-datasets`)
and flagged `unrelated`, and SHALL never steer the choice.
Each item's path SHALL be a qualified reference (`queries/references`) that a query naming the
recommended root as `population` executes with the item's meaning: the item as written when it
resolves from the root along the chosen path, else its explicit route from the root.
Without determination items,
attachments steer selection by join-path reachability alone. Every other item
SHALL carry `unrelated` false. A feasible `root_hint` (determines all determination
items under either rule, reaches all attachments, by a join path or as unrelated
datasets when determination items exist) SHALL override selection; an infeasible one
falls back with a warning; a malformed one raises. The no-common-root coverage output
SHALL be retained with the per-item criterion updated to match. A pair whose
unaddressable edges keep an item from being reached SHALL be named in the response
warnings with the remedy to name its edges.

#### Scenario: Saved measure does not steer the recommendation

- **WHEN** items mix columns with a suffix-less saved measure
- **THEN** the saved measure is reported as an attachment and does not change the
  recommended root

#### Scenario: Recommendation matches inference

- **WHEN** the same dimension items are given to `recommend_population` and queried
  without `population`
- **THEN** the recommended root equals the reported dimension anchor

#### Scenario: Ambiguity is resolved by an explicit path

- **WHEN** `orders` reaches `regions` by two fan-out-free routes (via `customers` and
  via `warehouses`) and `recommend_population` is given `orders.status` and
  `regions.population`
- **THEN** the root is `orders` and the path for `regions.population` is
  `orders.customers.regions.population`, while
  `{"dimensions": ["orders.status", "regions.population"]}` without `population`
  fails with the ambiguous-path error

#### Scenario: Recommendation and inference agree on a directed cycle

- **WHEN** `recommend_population` is given `x.v`, `y.v` and `z.v` on the directed
  cycle `x → y → z → x`
- **THEN** no root is recommended, matching the undefined dimension anchor, and the coverage
  output reports which items each candidate determines

#### Scenario: An unaddressable pair is named in the warnings

- **WHEN** `orders` declares two unnamed joins to `addresses` and
  `recommend_population` is given `orders.order_id` and `addresses.city`
- **THEN** the response warnings name `orders`↔`addresses` and the remedy to name
  the edges

#### Scenario: An unrelated attachment is reported through the product hop

- **WHEN** `customers` and `events` share no join path and `recommend_population` is
  given `customers.city` and `count(events.id)`
- **THEN** the root is `customers`, `count(events.id)` is reported as an attachment
  flagged `unrelated` with the path `count(events.id)`, `customers.city` is flagged
  `unrelated` false, and querying that path rooted at `customers` succeeds

#### Scenario: An unrelated column stays unreachable

- **WHEN** `recommend_population` is given `customers.city` and `events.kind`
- **THEN** no root is recommended, exactly as before this change

#### Scenario: Attachments alone keep join-path selection

- **WHEN** `recommend_population` is given only `count(customers.id)` and
  `count(events.id)`
- **THEN** no root is recommended, exactly as before this change

#### Scenario: A row count is an attachment on its dataset

- **WHEN** `recommend_population` is given `customers.region` and `count(orders.*)`
- **THEN** the root is `customers` and `count(orders.*)` is reported as an
  attachment on `orders` with the path `count(orders.*)`

#### Scenario: An unqualified row count is refused

- **WHEN** `recommend_population` is given `customers.region` and `count(*)`
- **THEN** it fails with the typed error saying `count(*)` names no dataset and
  suggesting `count(<dataset>.*)`

### Requirement: A saved query's inferred populations are pinned at save

Saving a query-backed model — through `save_dataset`, `create_dataset_from_query`, or an edit of its
`source_queries` on any surface — SHALL store every stage that omits `population` with its
dimension anchor written as its `population` and `population_inferred: true`, so the stored stage
keeps the rows of its unsaved twin; stages naming `population` SHALL be stored as written. A stage
whose dimension anchor is the unit or undefined SHALL be stored without `population`. A stage whose
dimension anchor shares its name with a stage of the same saved query SHALL be refused at save with
an error naming the stage and advising to rename it. A stored stage naming `population` without
`population_inferred` (pinned before this rule) SHALL stay anchored. A stored query-backed model
with an unpinned stage SHALL, when run by name with a refinement, first pin that stage's dimension
anchor as inferred from the unrefined stage, refusing the same name collision; run without a
refinement it SHALL execute as before.

#### Scenario: Pinned on save

- **WHEN** a query-backed model is saved whose stages omit `population`
- **THEN** the stored stages carry their dimension anchors as `population` with
  `population_inferred: true`, and running it by name returns the same rows as before saving

#### Scenario: A refinement never moves the population

- **WHEN** a saved query whose stage omitted `population` runs with a refinement whose extra
  dimension would, written into an unpinned query, infer a different dimension anchor
- **THEN** the refined run keeps the pinned anchor and its rows stay inferred

#### Scenario: Name collision refused at save

- **WHEN** a saved query's stage omitting `population` infers a dimension anchor named like another
  stage of the same saved query
- **THEN** the save fails with an error naming the stage and advising to rename it

#### Scenario: Legacy unpinned model refined

- **WHEN** a stored query-backed model saved without pinned stages runs by name with a refinement
- **THEN** the dimension anchor is the one inferred from the unrefined stage
- **WHEN** it runs by name without a refinement
- **THEN** it returns the same result as before this change

#### Scenario: A stage pinned before this rule stays anchored

- **WHEN** a stored stage names `population: customers` without `population_inferred`, groups by
  `customers.region` and measures `orders.total_revenue`
- **THEN** it runs anchored: NORTH is a row with NULL revenue

### Requirement: A population is named, and typed at construction

A query's `population` SHALL accept exactly one form — a saved model or stage name (a
string) — or be omitted, and SHALL be validated when the query is constructed, before any
engine resolution. An object without `source_name` SHALL be rejected at construction with an
error stating that inline models are not accepted and that `population` names a stored
model or stage, with `extensions` for query-local measures and joins. An object carrying
`source_name` is a legacy root extension: it SHALL be converted into `extensions` as the
`queries/extensions` capability specifies. Any other value SHALL be rejected at construction. Validation errors SHALL name the
offending keys of the form the object was classified as. The query's JSON schema SHALL describe `population` as a
string or null.

#### Scenario: Extension object validates as a typed extension

- **WHEN** a query is constructed without a version with `population` =
  `{"dataset": "orders", "metrics": [{"name": "double_amount", "expression": "sum(amount * 2)"}]}`
- **THEN** the query's `population` is `orders`, its `extensions` hold one typed extension
  of `orders` whose `metrics` are typed objects, and the query executes exactly as before

#### Scenario: Inline model object is rejected

- **WHEN** a query is constructed with `population` =
  `{"name": "ad_hoc", "sql_table": "things", "data_source": "ds", "fields": [...]}`, as a
  dict or as a `SlayerDataset` instance, directly, via REST or via the MCP query tool
- **THEN** construction fails with the inline-model error, and nothing executes

#### Scenario: Copies keep the name form

- **WHEN** a query is copied (without re-validation) with a replacement `population` string
- **THEN** `population_name` follows the replacement

#### Scenario: Unknown extension key is rejected

- **WHEN** a query is constructed with an extension
  `{"dataset": "orders", "filters": ["subtotal > tax_paid * 5"]}`, in `extensions` or
  in the legacy `population` form
- **THEN** construction fails with a validation error naming `filters` as an unexpected
  key, and nothing is silently dropped

#### Scenario: Object with source_name plus model keys is an extension, never a model

- **WHEN** a query is constructed with `population` =
  `{"dataset": "orders", "name": "x", "sql_table": "t", "data_source": "ds"}`
- **THEN** construction fails with a validation error naming `name`, `sql_table`, and
  `data_source` as unexpected extension keys

#### Scenario: Non-spec value is rejected

- **WHEN** a query is constructed with `population` = `42` or `[1, 2]`
- **THEN** construction fails with a validation error

#### Scenario: Legacy inline shapes still migrate before validation

- **WHEN** a stored version-1 query carries an extension object spelled with `dimensions`
  (the pre-v2 key), directly or inside a stored query-backed model's stages
- **THEN** the migrations rewrite it to `columns`, move it into `extensions`, and inline its
  columns as `models/stored-upgrade` › Stored extension columns are inlined specifies, and
  the query validates with a typed extension

#### Scenario: Typed spec round-trips through serialization

- **WHEN** a query holding extensions is dumped (python or JSON mode) and validated again
- **THEN** the result equals the original; dumping with none-valued fields excluded adds
  no null-valued keys, and an extension carrying only non-defaulted values dumps back to
  exactly the object the author supplied

#### Scenario: Schema advertises a name or null

- **WHEN** the query's JSON schema is generated
- **THEN** `population` is described as a string or null, with no `SlayerDataset`
  reference, `extensions` as a list of `DatasetExtension` references — and the MCP query tool's
  input schema carries the same definitions

#### Scenario: REST body rejects a malformed spec at validation

- **WHEN** `POST /query` carries a `population` that is neither a name nor a legacy
  extension, or an extension with an unknown key
- **THEN** the request is rejected with HTTP 422 by body validation, before the query
  reaches the engine; an inline-model body is rejected the same way, and well-formed
  extension bodies still execute

### Requirement: Rows of an inferred query are the row supply

A query omitting `population` SHALL list its **row supply**, before measure-typed filters,
`order` and `limit`. An aggregate grouped by the query's dimensions whose home determines or
associates each of them over a join path is **full-grain**; it supplies its cells — the dimension
tuples its filtered home rows reach along the home's own null-extending paths, so a home row with
no related row lands in the NULL tuple. Any other aggregate (an explicit `partition_by=`, an
aggregate homed on an unrelated dataset, whose cell is the one empty tuple, or one that falls back
to broadcast) has cells on a coarser grain; they SHALL extend to the cells of the dimension anchor
agreeing with them there. A metric SHALL supply the union of its full-grain aggregate operands'
cells, through combinations and transforms, and with none, the union of its operands' extended
cells. The rows SHALL be the NULL-safe union of every metric's supply; with no metrics, the
dimension anchor's cells (the rows of the query anchored at that dataset); with no dimensions,
exactly one row. Aggregates appearing only in filters, order entries or computed dimensions SHALL
supply nothing. Each term SHALL attach to the rows on its own grain, a cell it lacks taking its
empty value. Field filters SHALL restrict every supply before its tuples are taken and SHALL NEVER
choose the dimension anchor or a supply. Rows a row-level security policy or a model filter
removes SHALL NOT exist for any of this: a home row reaching only such a row lands in the NULL
tuple. Spine queries are governed by `queries/time-spine` and keep `time_spine × P`.

Scenario seed data (Ossie Layer 3 Appendix B), executed on SQLite and DuckDB: `customers(customer_id
PK, region, segment)` C1 EAST retail, C2 EAST corporate, C3 WEST retail, C4 NORTH retail;
`orders(order_id PK, customer_id, amount, status)` O1 C1 100 completed, O2 C1 200 open, O3 C2 50
completed, O4 C3 300 open; `returns(return_id PK, customer_id, amount)` R1 C1 25, R2 C4 10;
`visits(visit_id PK, customer_id)` V1 C4, V2 C4, V3 C3; `orders`, `returns` and `visits` each
joined many-to-one to `customers`; `tickets(ticket_id PK)` T1, T2, T3 with no join path to any of
them. The **orphan variant** adds order O5 C9 70 open. Saved metrics `customers.customer_count =
count(*)`, `orders.total_revenue = sum(amount)`, `returns.total_returns = sum(amount)`,
`visits.visit_count = count(*)`, `tickets.ticket_count = count(*)`. No query below names
`population`.

#### Scenario: A dimension value no metric reaches is not a row

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue`, and another
  measures `returns.total_returns`
- **THEN** the first returns EAST 350 and WEST 300; the second EAST 25 and NORTH 10

#### Scenario: An orphan home row lands in the NULL tuple

- **WHEN** over the orphan variant a query groups by `customers.region` and measures
  `orders.total_revenue`, once with no filter and once with the filter `orders.amount > 60`
- **THEN** the first returns EAST 350, WEST 300 and NULL 70; the second EAST 300, WEST 300 and
  NULL 70, keyed `customers.region` and `orders.total_revenue` in both

#### Scenario: Two metrics union their rows

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue` and
  `returns.total_returns`
- **THEN** the rows are EAST (350, 25), WEST (300, NULL) and NORTH (NULL, 10)

#### Scenario: A count reads 0 on a row another metric supplies

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue` and
  `visits.visit_count`
- **THEN** the rows are EAST (350, 0), WEST (300, 1) and NORTH (NULL, 2)

#### Scenario: A metrics-only query returns one row

- **WHEN** a query measures `sum(orders.amount) / count_distinct(customers.customer_id)`; another
  measures `orders.total_revenue` and `tickets.ticket_count`; another measures
  `orders.total_revenue` with the filter `1 = 0`
- **THEN** the first returns one row 162.5 (180 over the orphan variant); the second one row
  (650, 3); the third one row NULL

#### Scenario: Dimension-only rows come from the anchor

- **WHEN** a query groups by `customers.region` with no metrics, once with no filter and once
  with the filter `orders.amount > 100`
- **THEN** the first returns EAST, WEST and NORTH; the second EAST and WEST, keyed
  `customers.region`, the customers restricted by association

#### Scenario: A filter never brings rows back

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue` with the
  filter `orders.amount > 100`, and again with `orders.order_id` as a second dimension
- **THEN** the first returns EAST 200 and WEST 300; the second (O2, EAST) and (O4, WEST); and with
  the filter `1 = 0` the grouped query returns no rows

#### Scenario: A filter restricts every home

- **WHEN** a query groups by `customers.region`, measures `orders.total_revenue` and
  `returns.total_returns`, and filters `orders.amount > 100`
- **THEN** the rows are EAST (200, 25) and WEST (300, NULL), the returns restricted to customers
  with such an order, with a `semi_join_pushed` warning naming `returns.total_returns`

#### Scenario: A cross-dataset filter

- **WHEN** a query groups by `customers.region`, measures `orders.total_revenue` and filters
  `orders.status = 'completed' and customers.region = 'EAST'`
- **THEN** the only row is EAST 150

#### Scenario: An unrelated metric supplies the anchor's cells

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue` and
  `tickets.ticket_count`
- **THEN** the rows are EAST (350, 3), WEST (300, 3) and NORTH (NULL, 3), with `associated` and
  `unrelated_dataset` warnings naming `tickets.ticket_count`

#### Scenario: An associated metric supplies the tuples its home rows reach

- **WHEN** a query groups by `orders.status` and measures `customers.customer_count`
- **THEN** the rows are completed 2, open 2 and NULL 1 (C4 has no orders), with an `associated`
  warning

#### Scenario: A partitioned metric supplies its own cells extended by the anchor

- **WHEN** a query groups by `customers.region` and `customers.segment` and measures
  `sum(orders.amount, partition_by=[customers.region])`
- **THEN** the rows are (EAST, retail) 350, (EAST, corporate) 350 and (WEST, retail) 300; NORTH,
  which no order reaches, is not a row

#### Scenario: A filtered-out dimension row does not exist

- **WHEN** `customers` carries the model filter `region <> 'WEST'`, and separately a row-level
  security policy hides C3, and a query groups by `customers.region` and measures
  `orders.total_revenue`
- **THEN** in both cases the rows are EAST 350 and NULL 300: O4, whose customer does not exist for
  the query, lands in the NULL tuple

#### Scenario: Ossie's home routing answers a query the population could not route

- **WHEN** `orders → customers → regions`, `regions → managers` and `orders → managers` are
  many-to-one, and a query groups by `regions.name` and measures `orders.total_revenue`
- **THEN** it executes, each order reaching its region through `customers`, never refused as
  ambiguous

#### Scenario: Producer forms supply rows alike

- **WHEN** a query groups by `customers.region` and measures `returns.total_returns` beside, in
  turn, a ranked aggregate `last(orders.amount, orders.order_id)`, a windowed aggregate, a
  time-shifted transform, a second-order aggregate, an aggregate over a list child, and an
  aggregate homed on a keyless dataset, each over the orphan variant
- **THEN** every result's rows are the union of both metrics' supplies, the orphan landing in the
  NULL tuple where its metric is full-grain, and each value equals the value of that metric alone
  in its own rows

#### Scenario: An absent cell and an all-NULL cell are alike

- **WHEN** a query groups by `customers.region`, measures `orders.total_revenue` and
  `sum(returns.amount)`, and filters `sum(returns.amount) is null`, with one customer's only
  return carrying a NULL amount
- **THEN** a region whose returns are all NULL and a region with no returns both survive the
  filter, with the same NULL value

#### Scenario: Key types agree across row sources

- **WHEN** a query whose rows union several homes groups by a nullable decimal, a timestamp and a
  text dimension, one home supplying only NULL tuples
- **THEN** it executes on every dialect with each key column of one type
