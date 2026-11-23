## MODIFIED Requirements

### Requirement: Stored legacy filter literals are repaired on load

On load of a stored query, a string literal in a stored filter SHALL be repaired with the same offset and slash rules, before its whole-day upgrade and its qualified-reference upgrade, when both of these hold:
- It is a direct comparison operand (`=`, `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, either operand order, or any element of an all-literal `IN` or `NOT IN` list).
- The other side is a column reference that the binder reads, from the query's population by the reading its qualified-reference upgrade keeps, as a DATE or TIMESTAMP column: its own qualifier dropped, each hop over a join declared on either model, a lone unmatched hop routed by a unique short-form route, the query's extensions applied, a column an extension declares typed by its declared type, and a double-quoted segment matched only by its exact spelling. A memory's query is read in the datasource its qualified-reference upgrade reads (the datasource priority applied); a population naming an earlier stage reads only the columns that stage's extension joins name, untyped.

A literal whose column cannot be typed, or that compares a non-temporal column, SHALL be left unchanged; the qualified-reference upgrade then keeps or refuses the document.

#### Scenario: An offset literal against a timestamp runs

- **WHEN** a stored query filters `ordered_at >= '2024-01-01T00:00:00Z'` where `ordered_at` is a TIMESTAMP column
- **THEN** it loads and runs, filtering on `2024-01-01 00:00:00`

#### Scenario: Every operand layout is repaired

- **WHEN** stored filters read `'2024/01/01' <= ordered_at`, `ordered_at >= '2024-01-01T00:00:00Z' and ordered_at <= '2024-06-30T00:00:00+02:00'`, `ordered_at IN ('2024/01/01', '2024/02/01')` and `customers.signed_up_at >= '2024-01-01T00:00:00Z'` (a joined TIMESTAMP column)
- **THEN** every legacy literal in them is repaired and each filter runs

#### Scenario: Non-temporal and unresolvable comparisons stay verbatim

- **WHEN** stored filters read `status = '2024/01/01'` (a TEXT column) and `ordered_at IN ('2024/01/01', status)`
- **THEN** both are left unchanged

#### Scenario: A short-form reference routed over two hops is repaired

- **WHEN** a stored query on `orders` filters `regions.opened_on >= '2024/01/01'`, where `opened_on` is a DATE column of `regions`, reached only by the route `orders → customers → regions`
- **THEN** it loads as `orders.customers.regions.opened_on >= '2024-01-01'` and runs, returning the rows of that route

#### Scenario: A hop over a join declared on the peer is repaired

- **WHEN** a stored query on `orders` filters `customers.signed_up_at >= '2024-01-01T00:00:00Z'`, where the only join between them is declared on `customers`
- **THEN** the literal is repaired and the query runs

#### Scenario: A column a query extension declares on a routed model is repaired

- **WHEN** a stored query on `orders` extends `regions` with a DATE column `founded_on` reading `opened_on`, and filters `regions.founded_on >= '2024/01/01'`, `regions` reached by short-form routing
- **THEN** it loads with the column inlined as a DATE cast compared with `'2024-01-01'`, and runs

#### Scenario: A routed non-temporal column stays verbatim

- **WHEN** a stored query on `orders` filters `regions.label = '2024/01/01'`, where `label` is a TEXT column of `regions` reached by short-form routing
- **THEN** its literal is left unchanged and the query keeps its rows

#### Scenario: Quoted segments are matched by their exact spelling

- **WHEN** a stored query on `orders` filters `"Regions"."Opened_On" >= '2024/01/01'`, where `Regions` holds the DATE column `Opened_On`, and other stored queries spell the hop `"REGIONS"` or the leaf `"OPENED_ON"`
- **THEN** the first loads repaired and runs, and neither of the others has its literal repaired

#### Scenario: A stage population types only its extension joins

- **WHEN** a stored query-backed model's second stage reads the first stage, extends it with a join to `customers`, and filters `ordered_at >= '2024/01/01'` and `customers.signed_up_at >= '2024-01-01T00:00:00Z'`
- **THEN** the first literal is left unchanged and the second is repaired

#### Scenario: A population inferred for a root-less query types its references

- **WHEN** a stored query with no population, whose references the qualified-reference upgrade reads from `orders`, filters `regions.opened_on >= '2024/01/01'` reached by short-form routing
- **THEN** the literal is repaired as from `orders`

#### Scenario: An unloadable model does not block an unrelated repair

- **WHEN** the datasource also stores a model that fails to load, and a stored query's routed DATE comparison does not touch it
- **THEN** the literal is repaired and the query runs

#### Scenario: A memory's query is repaired against its priority datasource

- **WHEN** a stored memory's query names a model that two datasources hold, only one of which routes to the filtered DATE column, and the datasource priority puts that one first
- **THEN** the literal is repaired and the query runs there; with the other datasource first, it is left unchanged

### Requirement: Stored date-only comparisons keep their whole-day meaning

On load, a stored model, saved query or memory written before this change — judged by each nested document's own stored version, so a current document holding an older nested query, extension or memory query is upgraded where it nests — SHALL have every Mode-B comparison of a temporal operand with a date-only literal `D` rewritten to the comparison it meant before: `x <= 'D'` → `x < '<D+1>'`; `x > 'D'` → `x >= '<D+1>'`; `x = 'D'` → `x in 'D'`; `x != 'D'` → `x not in 'D'`; `x BETWEEN 'A' AND 'B'` → `(x >= 'A' and x < '<B+1>')` and `x NOT BETWEEN 'A' AND 'B'` → `(x < 'A' or x >= '<B+1>')`, each with only date-only bounds rewritten; a literal on the left mirrored. On the last representable day `9999-12-31`, `< '<D+1>'` SHALL be `<= 'D 23:59:59.999999'` and `>= '<D+1>'` SHALL be `> 'D 23:59:59.999999'`. `x >= 'D'` and `x < 'D'` SHALL be untouched. This covers query measures, filters, dimensions, time dimensions, order keys and `main_time_dimension`, saved-measure formulas, Mode-B column filters, and every nested saved query, model extension, query-backed model and memory query, with the rest of the text byte-identical and no warning on the response. Whether an operand is temporal SHALL be decided as the binder reads it, with the models of the datasource the document runs in, the built-in time spine included: a query's operand from the query's population by the reading its qualified-reference upgrade keeps (its own qualifier dropped, each hop over a join declared on either model, a lone unmatched hop routed by a unique short-form route, the query's extensions applied, a column an extension declares typed by its declared type, and a double-quoted segment matched only by its exact spelling), and a saved-measure formula or column filter from its own model; a memory's query SHALL use the datasource its qualified-reference upgrade reads (the datasource priority applied): an unquoted model name spelt `time_spine` in any case SHALL name the spine unless a stored model has exactly that spelling, and SHALL be untypable when it also reaches a stored model in another case; a path SHALL reach the spine from any model, a query or extension on the spine SHALL reach the datasource's models, and a path that continues past the spine SHALL be untypable. A comparison with a non-temporal operand, an operand whose type cannot be decided (an undeclared column, a missing or unloadable model), a `BETWEEN` whose tested expression is the literal, and a comparison with a variable SHALL be left unchanged, each untypable one with a logged warning naming the document and the text. Mode-A text and `date_range` SHALL be untouched. The upgrade SHALL be idempotent and SHALL complete on the next load when a load is interrupted. Every upgraded query SHALL return the rows it returned before.

#### Scenario: Saved query with an inclusive upper bound

- **WHEN** a stored saved query on a TIMESTAMP `created_at` has the filter `created_at <= '2024-12-31'`
- **THEN** after load it reads `created_at < '2025-01-01'` and returns the rows it returned before

#### Scenario: Each operator in a memory query

- **WHEN** a stored memory's query has the filters `created_at > '2024-04-10'`, `created_at = '2024-04-10'`, `created_at != '2024-04-10'`, `'2024-04-10' >= created_at` and `created_at BETWEEN '2024-03-05' AND '2024-04-10'`
- **THEN** after load they read `created_at >= '2024-04-11'`, `created_at in '2024-04-10'`, `created_at not in '2024-04-10'`, `'2024-04-11' > created_at` and `(created_at >= '2024-03-05' and created_at < '2024-04-11')`, and the query returns its earlier rows

#### Scenario: Saved measure and column filter

- **WHEN** a stored model has the saved measure `count(*) FILTER (WHERE created_at <= '2024-04-10')` and a Mode-B column filter `created_at > '2024-04-10'`
- **THEN** after load they read `count(*) FILTER (WHERE created_at < '2024-04-11')` and `created_at >= '2024-04-11'`, and return their earlier values

#### Scenario: Unchanged comparisons

- **WHEN** a stored query has the filters `created_at >= '2024-01-01'`, `created_at < '2025-01-01'`, `code <= '2024-12-31'` on a TEXT column, `created_at <= '2024-12-31 00:00:00'` and `created_at <= '{cutoff}'`
- **THEN** each loads byte-identical

#### Scenario: Untypable operand is left and logged

- **WHEN** a stored query compares a column its model does not declare, or a column of a model that no longer exists, with `<= '2024-12-31'`
- **THEN** it loads byte-identical and a warning naming the document and the comparison is logged

#### Scenario: Current document with an older nested query

- **WHEN** a model stored at the current version holds a source query stored at the previous query version with the filter `created_at <= '2024-12-31'`
- **THEN** after load the nested filter reads `created_at < '2025-01-01'`

#### Scenario: Upgrade is idempotent

- **WHEN** an upgraded document is loaded again, and a document saved after this change holds `created_at <= '2024-12-31'`
- **THEN** neither is rewritten

#### Scenario: Spine operand in a query on a stored model

- **WHEN** a stored saved query on `orders` uses `time_spine.timestamp` as a time dimension and has the filters `orders.ordered_at <= '2024-01-31'` and `time_spine.timestamp <= '2024-01-31'`
- **THEN** after load they read `orders.ordered_at < '2024-02-01'` and `time_spine.timestamp < '2024-02-01'`, and the query returns Jan 31's bucket as before

#### Scenario: Spine spelt in another case

- **WHEN** a stored query on `orders` has the filters `TIME_SPINE.timestamp <= '2024-01-31'` and `Time_Spine.TIMESTAMP > '2024-01-31'`, and no stored model has either spelling
- **THEN** after load they read `TIME_SPINE.timestamp < '2024-02-01'` and `Time_Spine.TIMESTAMP >= '2024-02-01'`, every other byte unchanged

#### Scenario: Spine reached from a joined model, with or without a time axis of its own

- **WHEN** a stored query on `orders` has the filter `customers.time_spine.timestamp <= '2024-01-31'`, once with `customers` having a time column and once with none
- **THEN** after load it reads `customers.time_spine.timestamp < '2024-02-01'` both times

#### Scenario: Query rooted at the spine

- **WHEN** a stored query whose source model is `time_spine` (and another spelt `TIME_SPINE`) has the filters `timestamp <= '2024-01-31'`, `time_spine.timestamp <= '2024-01-31'` and `orders.ordered_at <= '2024-01-31'`
- **THEN** after load each reads `< '2024-02-01'` with its operand spelt as stored

#### Scenario: Spine operands in every stored document

- **WHEN** `time_spine.timestamp <= '2024-01-31'` appears in a stored model's saved-measure formula and Mode-B column filter, in a stored model's nested source query, in a stored memory's query naming its datasource and in one naming none, and in a query extension's column filter, including an extension whose source is `TIME_SPINE` filtering `timestamp <= '2024-01-31'`
- **THEN** after load each reads `< '2024-02-01'`

#### Scenario: Memory on the spine in a store with one datasource

- **WHEN** a store holds one datasource and a stored memory that names no datasource, rooted at `time_spine`, has the filter `orders.ordered_at <= '2024-01-31'`
- **THEN** after load it reads `orders.ordered_at < '2024-02-01'`; with a second datasource in the store also holding `orders`, it is typed in the datasource the datasource priority picks

#### Scenario: Spine reached through an extension's join

- **WHEN** a stored query on `orders` has an extension adding a join from `orders` to `customers` and the filter `customers.time_spine.timestamp <= '2024-01-31'`
- **THEN** after load it reads `customers.time_spine.timestamp < '2024-02-01'`

#### Scenario: Spine spelling that is untypable

- **WHEN** a datasource stores a model named `Time_Spine` (saved before the spine's name was reserved) and a stored query has the filter `TIME_SPINE.timestamp <= '2024-01-31'`; and a stored query has the filter `orders.time_spine.customers.created_at <= '2024-01-31'`
- **THEN** both load byte-identical, each with a warning naming the document and the comparison logged

#### Scenario: Stored names shadow the spine

- **WHEN** a stored query on `orders` filters `time_spine.code <= '2024-01-31'` where `orders` has a stored join named `time_spine` to a model whose `code` is TEXT; a stored model's source query on `orders` filters `time_spine.timestamp <= '2024-01-31'` while a sibling source query is named `time_spine`; and a datasource stores a model named exactly `time_spine` whose `timestamp` column is TEXT, read by a stored query filtering `time_spine.timestamp <= '2024-01-31'`
- **THEN** the third loads byte-identical without a warning, and the first and second load byte-identical with a warning naming the document and the comparison

#### Scenario: Legacy time literal on the spine

- **WHEN** a stored query written before legacy time literals were repaired, rooted at `time_spine`, compares `timestamp` with a legacy date literal for `2024-01-31` using `<=`
- **THEN** after load the literal is repaired and the comparison reads `timestamp < '2024-02-01'`

#### Scenario: A routed operand keeps its whole-day meaning

- **WHEN** a stored query on `orders` written before this upgrade filters `regions.opened_at = '2024-01-01'`, where `opened_at` is a TIMESTAMP column of `regions`, reached only by the route `orders → customers → regions`
- **THEN** after load the comparison reads `in '2024-01-01'` on the qualified route and the query returns the rows it returned before

#### Scenario: A quoted operand is typed by its exact spelling

- **WHEN** stored queries on `orders` filter `"Regions"."Opened_At" <= '2024-01-31'` and `"Regions"."OPENED_AT" <= '2024-01-31'`, where `Regions` has the TIMESTAMP column `Opened_At` and no column spelt `OPENED_AT`
- **THEN** the first reads `< '2024-02-01'` after load and the second keeps its comparison as written

### Requirement: Stored queries are upgraded to qualified references
A stored query written before this change — a saved query, a query-backed model's `source_queries` stage, or a memory's query, nested or not, with or without a version of its own — SHALL load in the qualified syntax of `queries/references`, with no deprecation warning, by a conversion that reads the stored datasets:
- each reference SHALL be rewritten to the qualified spelling that has the meaning it had before this change, reading the stage's own population;
- a reference the new routing would send along a different path SHALL be rewritten to the explicit route from the population that keeps its old path;
- a reference whose anchor was reached by a route longer than its written path SHALL be rewritten to its explicit route from the population, so its column name is kept;
- a rewritten path SHALL be kept only if it resolves back to the same oriented sequence of relationships;
- an unaliased entry whose references share no anchor SHALL get an alias equal to its column name before this change, so every later stage, query-backed model consumer and `DatasetField.sql` reading that column keeps working;
- `count(*)` SHALL become `count(<population>.*)`.
- a double-quoted segment SHALL match only its exact spelling, and a rewritten reference SHALL keep each double-quoted segment of its leaf double-quoted.

A stored query that cannot be converted this way (a dataset it reads is missing, or an old path has no unique explicit spelling) SHALL fail to load through the existing unloadable-document boundary, untouched on disk, with a warning naming the document and the reference. Saved queries and query-backed models SHALL be written back as other migrated documents are; memories SHALL convert on each load.

#### Scenario: A saved query loads qualified with its results
- **WHEN** a store holds a saved query with `population: orders`, the dimension `status`, the metric `sum(amount)` and the filter `customers.region = 'EAST'`
- **THEN** it loads as `orders.status`, `sum(orders.amount)` and `customers.region = 'EAST'`, with no `FutureWarning`, and returns the rows it returned before

#### Scenario: A stored cycle reference keeps its path
- **WHEN** a stored query with `population: x` over the cycle `x → y → z → x` measures `sum(z.amount)`
- **THEN** it loads as `sum(x.z.amount)` and returns the value it returned before

#### Scenario: A stored stage chain keeps its column names
- **WHEN** a stored query-backed model's first stage measures `sum(amount) / count(customers.*)` unaliased and its second stage reads that column by its old name
- **THEN** the first stage's entry gets that name as its alias and the model returns the rows it returned before

#### Scenario: An unspellable old path makes the document unloadable
- **WHEN** a stored query's reference reads a dataset that its population reaches only across two unnamed parallel relationships, so its old path has no explicit spelling
- **THEN** the document fails to load, stays untouched on disk, and the warning names the document and the reference

#### Scenario: A memory's query converts on every load
- **WHEN** a stored memory holds a query with `population: orders` measuring `count(*)`
- **THEN** each load reads it as `count(orders.*)`, with no `FutureWarning`, and the stored memory is not rewritten

#### Scenario: A quoted leaf keeps its exact spelling
- **WHEN** a stored query on `orders` filters `"Regions"."Opened_At" >= '2024-01-01'`, `Regions` reached by the route `orders → customers → Regions`
- **THEN** it loads with the leaf still spelt `"Opened_At"` on its explicit route
