# models/stored-upgrade Specification

## Purpose
Guarantees that a store written by any released SLayer version still loads, re-saves, re-ingests and queries with that release's results when opened by a later version. Stored documents are repaired on load; live client input keeps today's validation.

## Requirements

### Requirement: A released store opens with its release's results

A store written by SLayer 0.10.2, and a store written by SLayer 1.8.2, SHALL each load completely when opened by the current version, both with its datasource present and with the datasource entry absent. The 0.10.2 store SHALL hold every model and memory shape that release could write: ingested models, a Cube import whose foreign key is not a dimension, a multi-stage query-backed model ordering by `rank(...)`, an aggregation reading a quoted parameter, legacy time literals, and a memory with a query. The 1.8.2 store SHALL hold the list-column shapes that release could write: ingested native list columns, an `sql`-mode list column whose undeclared `element_type` 1.8.2 stored as `UNKNOWN`, a sole primary key allowing `collect`, stored measures and a query-backed model using `collect`, the list functions and list elements, and a memory with such a query. With the datasource present, every model SHALL re-save and the datasource SHALL re-ingest. Every query the release could run SHALL return the rows the release returned.

#### Scenario: The 0.10.2 corpus opens with its datasource present

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened with its DuckDB datasource present
- **THEN** every model and memory loads, every model re-saves, the datasource re-ingests, and every recorded query returns the rows recorded from 0.10.2

#### Scenario: The 0.10.2 corpus opens offline

- **WHEN** the same store is opened with its datasource entry removed
- **THEN** every model and memory loads, without any connection being attempted

#### Scenario: The 1.8.2 corpus opens with its datasource present

- **WHEN** the committed store written by `motley-slayer==1.8.2`, on both the YAML and the SQLite store, is opened with its DuckDB datasource present
- **THEN** every model and memory loads, every model re-saves, the datasource re-ingests, and every recorded query returns the rows recorded from 1.8.2

#### Scenario: The 1.8.2 corpus opens offline

- **WHEN** the same store is opened with its datasource entry removed
- **THEN** every model and memory loads, without any connection being attempted

### Requirement: Live type refinement applies only to models written before refining ingest

On load, a stored model below schema version 8 SHALL have its DOUBLE and SQLite INT base columns refined against the live datasource. At version 8 and above, ingest already wrote refined types. A stored model at version 8 or above SHALL load without consulting the datasource, keeping its stored column types. Refinement SHALL read only the model's own table, through a client of the query engine that is loading the model. A load that needs refinement while the model's datasource has a stored entry but no query engine is loading it SHALL fail with an error stating that the model must be loaded through a query engine. On SQLite, refinement SHALL keep its sampling rules: a column is narrowed to INT only when the sample positively certifies integer storage, a saturated or failed sample leaves the stored type unchanged, and stored INT is widened only on evidence of non-integer storage.

#### Scenario: A v10 model loads with no datasource entry
- **WHEN** a raw v10 model with a DOUBLE base column is loaded and its datasource has no stored entry
- **THEN** it loads with the column still DOUBLE and is written back at the current version

#### Scenario: A v10 model loads with an unreachable datasource
- **WHEN** a raw v10 model with a DOUBLE base column is loaded and its datasource is a Postgres server that refuses connections
- **THEN** it loads, and no connection to the datasource is attempted

#### Scenario: A pre-v8 model still requires its datasource
- **WHEN** a raw v5 model with a refineable DOUBLE base column is loaded and its datasource has no stored entry
- **THEN** loading fails with the error stating that the datasource is unavailable for type refinement

#### Scenario: A v7 model is still refined
- **WHEN** a raw v7 model with a DOUBLE base column backed by a live integer column is loaded through a query engine with its datasource present
- **THEN** the column loads as INT

#### Scenario: Refinement reads only the model's table
- **WHEN** a raw v7 model is loaded through a query engine whose datasource holds several tables
- **THEN** exactly one table's columns are read

#### Scenario: Refinement without a query engine is refused
- **WHEN** a raw v5 model with a refineable DOUBLE base column is loaded directly from storage, with its datasource entry present
- **THEN** loading fails with the error stating that the model must be loaded through a query engine

#### Scenario: Refinement through a gateway
- **WHEN** a raw v7 model is loaded through a query engine whose client factory returns a gateway client, with every direct connection path failing
- **THEN** the column is refined from the gateway's metadata

#### Scenario: SQLite refinement keeps its sampling rules
- **WHEN** raw v7 SQLite models are loaded through a query engine with base columns holding integers only, mixed REAL values, BLOBs, coercible text, non-coercible text, non-finite text, more rows than the sample cap, and in an attached schema with a quoted identifier
- **THEN** only the integers-only column is narrowed, stored INT columns with non-integer evidence are widened, and the saturated column keeps its stored type

#### Scenario: The 0.10.2 corpus opens through storage and through an engine
- **WHEN** the committed 0.10.2 store is opened offline, both directly from storage and through a query engine
- **THEN** every model and memory loads both ways, without any connection being attempted

### Requirement: Stored rank-family orders get an explicit direction

On load of a stored query, wherever it is stored (a model's `source_queries` and their stages, or a memory's query), every rank-family call without a direction SHALL be given `direction='desc'`. This covers order items persisted as a placeholder column with their expression text, and it SHALL also run on documents already stored at the version that first introduced the direction rewrite. A query sent by a client, without a stored version, SHALL NOT be rewritten.

#### Scenario: A 0.10.2 stage ordering by rank executes

- **WHEN** a raw 0.10.2 multi-stage query-backed model's second stage orders by an item persisted as `{column: {name: _funcstyle_pending}, direction: asc, raw_formula: "rank(rev:sum)"}`
- **THEN** the model loads and executes, ranking descending as 0.10.2 did, on both the YAML and the SQLite backends

#### Scenario: A 0.10.2 memory query ordering by rank executes

- **WHEN** a memory stored by 0.10.2, as YAML frontmatter or as a SQLite row, bundles a query whose order item is persisted with `raw_formula: "rank(rev:sum)"`
- **THEN** the memory loads and its query executes, ranking descending

#### Scenario: A document stored after the first direction rewrite is repaired

- **WHEN** a stored v13 model holds a v5 source query whose order item still carries `raw_formula: "rank(rev:sum)"` without a direction
- **THEN** it loads as the current model and query versions, with the order item ranking descending

#### Scenario: A memory stored after the first direction rewrite is repaired

- **WHEN** a stored v3 memory bundles a v5 query whose order item still carries `raw_formula: "rank(rev:sum)"` without a direction
- **THEN** it loads as the current memory and query versions, with the order item ranking descending

### Requirement: Stored legacy date ranges are repaired on load

On load of a stored query, wherever it is stored, each time dimension's `date_range` SHALL be repaired before validation:
- A range whose length is not 2 SHALL be dropped, as 0.10.x applied no filter for it.
- A bound carrying a zone offset (`Z` or `±HH:MM`) SHALL lose the offset and keep its wall-clock time, as 0.10.x databases read it.
- A slashed date `YYYY/MM/DD`, optionally followed by a time, SHALL become its dashed ISO form.
- Any other bound SHALL be left unchanged, for validation to judge.

A query sent by a client SHALL keep being rejected for these shapes.

#### Scenario: Offset bounds return the 0.10.2 rows

- **WHEN** a raw 0.10.2 query-backed model's source query has `date_range: ['2024-01-01T00:00:00Z', '2024-12-31T23:59:59Z']`
- **THEN** it loads and returns the rows 0.10.2 returned on DuckDB and on Postgres

#### Scenario: Slashed bounds return the 0.10.2 rows

- **WHEN** a raw 0.10.2 query-backed model's source query has `date_range: ['2024/01/01', '2024/12/31']`
- **THEN** it loads and returns the rows 0.10.2 returned

#### Scenario: Ranges of length other than two apply no filter

- **WHEN** a raw 0.10.2 query-backed model's source query has a `date_range` of `[]`, of one element, or of three elements
- **THEN** it loads and returns the unfiltered rows, as 0.10.2 did

#### Scenario: A memory query with an offset bound runs

- **WHEN** a memory stored by 0.10.2, as YAML frontmatter or as a SQLite row, bundles a query whose `date_range` bound carries `+02:00`
- **THEN** the memory loads and its query runs, filtering on the bound's wall-clock time

#### Scenario: Client queries keep the strict check

- **WHEN** a client sends a query without a version whose `date_range` is `[]` or has a bound with a zone offset
- **THEN** the query is rejected with today's construction error

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

### Requirement: Stored query-backed models drop their cached backing SQL

A model SHALL NOT store the rendered SQL of its backing query. On load, a stored model below schema version 15 SHALL drop its `backing_query_sql` field. Inspect and REST SHALL NOT return backing SQL; a query-backed model's SQL SHALL be obtained by a `dry_run` query against it.

#### Scenario: A v14 query-backed model loads without backing SQL

- **WHEN** a stored v14 query-backed model holding `backing_query_sql` is loaded
- **THEN** it loads without the field, and is written back at version 15 without it

#### Scenario: Inspect shows no backing SQL

- **WHEN** a query-backed model is inspected with `show_sql=True`
- **THEN** the output has no backing-SQL section, and a `dry_run` query on the model returns its SQL

### Requirement: Stored Mode-B text is upgraded to the SQL string and identifier rules

On load, every Mode-B surface of a stored model, saved query or memory written before this change — `DatasetMetric.expression`, and a stored query's measures, dimensions, computed-dimension expressions, time-dimension columns, order columns and expressions, `main_time_dimension` and filters, recursively through stored `source_queries`, saved queries and extension measures — SHALL be rewritten so that it means under the current grammar what it meant when stored: a double-quoted string literal becomes the equivalent single-quoted literal, a string literal using backslash escapes is re-encoded with its decoded value, a `|` concatenation becomes `||`, two minus signs written with nothing between them are separated by a space, so a double negation never becomes a comment, and a Python `#` comment becomes a `--` comment. The rewrite SHALL be token-exact: text inside string literals is never otherwise touched, and stored text that cannot be tokenised is left unchanged. Stored text that the grammar of the time it claims to be from could not have held SHALL be left unchanged, with a warning naming the document and the field. The grammar of the time SHALL be every spelling the release that wrote the text accepted on that surface: on a query filter also `=`, `<>`, `NULL`, the keywords `AND` / `OR` / `NOT` / `IS` / `IN` in any case, `[NOT] LIKE` with a single-quoted pattern and `||`; on every surface also `CASE … END` (its `WHEN` conditions with the filter spellings), `extract(<part> from x)`, `count(distinct x)`, colon aggregations, `*` arguments and `{var}` placeholders. A text is held when that release's reader parsed it; a text it refused only for what the parse means (an unknown function, a chained comparison) counts as held. Held text SHALL be upgraded with no warning. Mode-A surfaces SHALL NOT be rewritten, and a client query without a version SHALL NOT be rewritten.

#### Scenario: Double-quoted strings in a stored query

- **WHEN** a stored query written before this change filters `status == "paid"` and has measure `sum(iif(region == "EU", amount, 0))`
- **THEN** it loads as `status == 'paid'` and `sum(iif(region == 'EU', amount, 0))` and returns the rows it returned before

#### Scenario: Escapes and embedded quotes

- **WHEN** a stored filter is `name == "O'Brien"`, another `name == 'O\'Brien'`, and a stored measure formula compares with `"tab\there"`
- **THEN** they load as `name == 'O''Brien'` (twice) and a single-quoted literal containing a real tab character, and match the same rows as before

#### Scenario: Pipe concatenation

- **WHEN** a stored computed dimension is `status | region`
- **THEN** it loads as `status || region` and returns the same values

#### Scenario: Adjacent minus signs

- **WHEN** a stored measure formula is `sum(amount) --sum(refund)`, another `sum(a--b)`, and a third `sum(a- -b)`
- **THEN** they load as `sum(amount) - -sum(refund)`, `sum(a- -b)` and `sum(a- -b)` unchanged, and return the values they returned before

#### Scenario: Single equals sign in a stored filter

- **WHEN** a stored memory query and a stored saved query written before this change each filter `status = "ok"`, and another stored saved query filters `status = 'ok'`
- **THEN** the first two load as `status = 'ok'` (qualified by the later upgrades), the third loads unchanged, all three return the rows they returned before, and no warning about the grammar is logged

#### Scenario: SQL filter spellings in a stored filter

- **WHEN** stored filters written before this change are `status <> "ok"`, `status = "ok" AND NOT region = "EU"`, `region IS NULL OR status = "ok"`, `status IN ("ok", "paid")`, `name LIKE 'A%' AND status = "ok"` and `status || region = "okEU"`
- **THEN** each loads with its double-quoted literals single-quoted and the rest byte-identical, returns the rows it returned before, and no warning about the grammar is logged

#### Scenario: 1.x expression spellings in a stored measure

- **WHEN** stored measures written before this change are `sum(CASE WHEN status = "ok" THEN amount ELSE 0 END)`, `count(distinct iif(status == "ok", id, None))` and `sum(iif(extract(year from created_at) == 2024 and status == "ok", amount, 0))`
- **THEN** each loads with its double-quoted literals single-quoted and the rest byte-identical, returns the value it returned before, and no warning about the grammar is logged

#### Scenario: Mode-A text untouched

- **WHEN** a stored model has `DatasetField.sql` `"Order Total" * 2` and a model filter `status = "x"`
- **THEN** both load byte-for-byte unchanged

#### Scenario: Client queries are not rewritten

- **WHEN** a client sends a query without a version that filters `status = "paid"`
- **THEN** it is not rewritten and fails with the double-quoted-name hint unless a column named `paid` exists

#### Scenario: Released stores still open with their results

- **WHEN** the committed 0.10.2 store is opened
- **THEN** every model and memory loads and every recorded query returns the rows recorded from 0.10.2

#### Scenario: Text already in the current grammar is left alone

- **WHEN** a stored model still marked with a version before this rewrite has the measures `sum(x::int)`, `sum(a || b)`, `sum(CAST(x AS INT))` and `sum(x) FILTER (WHERE y > 1)`, and a stored query of the same era filters `x BETWEEN 1 AND 2`
- **THEN** each loads byte-for-byte unchanged and a warning names the document and each such field

#### Scenario: Spellings the old release refused stay unchanged

- **WHEN** a stored query written before this change filters `name LIKE "A%"` and another filters `status = "ok" {? AND region = {r} ?}`
- **THEN** each loads byte-for-byte unchanged and a warning names the document and the field

### Requirement: Stored time dimensions are upgraded to expression text

On load, every time dimension of a stored model's `source_queries`, a saved query or a memory's bundled query written before this change, whose `dimension` (or `column`) is stored as a column-reference object `{"name": …, "model": …}`, SHALL be rewritten to the dotted text `model.name` (`name` when `model` is absent), recursively through nested stored queries, and re-saved in that form; the query SHALL return the rows it returned before. A time dimension already stored as text, and a client query without a version, SHALL NOT be rewritten.

#### Scenario: Stored object form becomes text

- **WHEN** a stored query-backed model's source query holds the time dimension `{"dimension": {"name": "created_at", "model": "customers"}, "granularity": "month"}`
- **THEN** it loads with `field` = `customers.created_at`, dumps it as that string, and returns the same rows as before

#### Scenario: Memory-bundled query

- **WHEN** a memory stored as YAML frontmatter or as a SQLite row bundles a query whose time dimension is stored in the object form
- **THEN** it loads with the dotted text and the bundled query returns the same rows

#### Scenario: The released corpus still opens

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened
- **THEN** every stored time dimension loads as text and every stored query returns its release's results

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

### Requirement: Stored granularities colliding with a library name are renamed

On every load, whatever the document's version, a stored datasource custom granularity whose name case-insensitively matches any reserved library name SHALL be renamed to `<name>_granularity`, or `<name>_granularity_<n>` with the smallest `n` ≥ 2 that is free, and every reference to it in that datasource's stored models, saved queries, query-backed models and memories SHALL be rewritten to the new name in every position a granularity name is accepted. Each rename SHALL be logged as a warning naming the datasource and both names. The rename SHALL be a function of the stored datasource alone, and dependent documents SHALL be persisted before the datasource, so a load interrupted at any point completes on the next load. This SHALL hold on both the YAML and the SQLite store. A client saving a custom granularity with a reserved name SHALL still be refused.

#### Scenario: Granularity named left

- **WHEN** a stored datasource defines the granularity `left` and a stored model's query-backed source and a memory's query both bucket `created_at` by it
- **THEN** after load the granularity is `left_granularity`, both documents reference `left_granularity`, a warning names the rename, and both queries return the rows they returned before

#### Scenario: Granularity named collect

- **WHEN** a stored datasource defines the granularity `collect`
- **THEN** it loads as `collect_granularity`

#### Scenario: The new name is taken

- **WHEN** a stored datasource defines the granularities `left` and `left_granularity`
- **THEN** `left` is renamed to `left_granularity_2` and references to `left_granularity` are untouched

#### Scenario: An interrupted load completes

- **WHEN** a load is interrupted after some dependent documents were persisted with the new name and before the datasource was
- **THEN** the next load renames the datasource's granularity to the same name and rewrites the remaining documents, leaving one consistent store

#### Scenario: A rename converges

- **WHEN** a store whose granularity `contains` was renamed is loaded again, on the YAML and on the SQLite store
- **THEN** no document changes and no rename warning is logged

#### Scenario: A granularity colliding with a later library addition

- **WHEN** a datasource stored at the current version defines the granularity `left`, which a later library release reserves, and a stored query buckets by `left(created_at)`
- **THEN** on load the granularity is `left_granularity`, the query reads `left_granularity(created_at)`, and it returns the rows it returned before

#### Scenario: A new reserved-name granularity is refused

- **WHEN** a client saves a datasource defining the granularity `left`
- **THEN** the save fails naming the reserved name

### Requirement: Stored aggregations over a column named distinct are upgraded

On load, a stored model, saved query or memory written before this change whose Mode-B text aggregates a bare `distinct` (`agg(distinct)` or `agg(distinct, …)`) SHALL have that argument rewritten to the quoted identifier `"distinct"`, so it reads the column named `distinct` as it did when stored. Any other use of `DISTINCT` SHALL be untouched.

#### Scenario: Sum over a column named distinct

- **WHEN** a stored model has a column `distinct` and a measure `sum(distinct)` written by the previous release
- **THEN** it loads as `sum("distinct")` and returns the sum of that column

#### Scenario: The DISTINCT modifier is untouched

- **WHEN** a stored measure is `count(DISTINCT customer_id)`
- **THEN** it loads unchanged

### Requirement: A store written by this release omits unset fields

Every persisted model, saved query, datasource and memory, in every storage backend, SHALL omit a field whose value is unset (`None`). A backend's own document layout (a YAML memory's file name and body) SHALL be unchanged. A store written by this release is not readable by SLayer 1.x: its documents use the Ossie vocabulary.

#### Scenario: Memory with an ordered query

- **WHEN** a memory whose query orders by a field without `nulls` is saved to YAML and to SQLite
- **THEN** neither stored form contains a `nulls` key

#### Scenario: No unset field in any kind

- **WHEN** a model, a saved query, a datasource and a memory are saved to each backend
- **THEN** no stored value is `null`

### Requirement: Stored colon spellings are upgraded to the functional spelling

On load, every Mode-B surface of a stored model, saved query or memory written before this change — `DatasetMetric.expression`, and a stored query's measures, dimensions, computed-dimension expressions, time-dimension columns, order columns and expressions, `main_time_dimension` and filters, recursively through stored `source_queries`, saved queries and extension measures — SHALL have every colon span rewritten to its twin: `date_trunc('<name>', <source>)` when `<name>` is a built-in granularity, and `<name>(<source>[, <args>])` otherwise, with the source and arguments as written and colon spans inside the arguments rewritten too. Everything outside a colon span SHALL stay byte-identical, and a colon inside a string literal, a quoted identifier or a `::` cast SHALL be untouched. A stored order item whose expression is rewritten SHALL load exactly as the same order key written fresh in the functional spelling. The document SHALL return the rows and result keys it returned before; a persisted computed-dimension name SHALL be kept and keep its meaning. A datasource granularity rename SHALL apply before the rewrite. Stored text that cannot otherwise be parsed SHALL be left unchanged, with a warning naming the document and the field. Mode-A surfaces SHALL NOT be rewritten, and a client query without a version SHALL NOT be rewritten.

#### Scenario: Aggregations, stars and parameters

- **WHEN** a stored model has the measures `amount:sum`, `*:count`, `customers.*:count`, `customers.regions.population:sum`, `price:percentile(p=0.9)` and `balance:last(updated_at)`
- **THEN** they load as `sum(amount)`, `count(*)`, `count(customers.*)`, `sum(customers.regions.population)`, `percentile(price, p=0.9)` and `last(balance, updated_at)` and return the values they returned before

#### Scenario: Colon spans inside larger text

- **WHEN** a stored query has the measure `rank(rev:sum)`, the filter `amount:sum > 100 AND note = 'a:sum'` and the measure `sum(amount::INT) / *:count`
- **THEN** they load as `rank(sum(rev))`, `sum(amount) > 100 AND note = 'a:sum'` and `sum(amount::INT) / count(*)`, every other byte unchanged

#### Scenario: Order items

- **WHEN** a stored query orders by the item persisted as `{column: {name: amount_sum}, raw_formula: "amount:sum"}` descending, and by `*:count`
- **THEN** it loads exactly as a fresh query ordering by `sum(amount)` descending and by `count(*)`, and returns the rows in the order it returned them before

#### Scenario: Granularity spans

- **WHEN** a stored query orders by `created_at:month` and a stored computed dimension buckets `created_at:fiscal_quarter` on a datasource defining `fiscal_quarter`
- **THEN** they load as `date_trunc('month', created_at)` and `date_trunc('fiscal_quarter', created_at)` and return the rows they returned before

#### Scenario: A granularity renamed on load

- **WHEN** a stored datasource defines the granularity `left`, and a stored model's source query and a memory's query bucket by `created_at:left`
- **THEN** after load they read `date_trunc('left_granularity', created_at)` and return the rows they returned before

#### Scenario: Result keys and computed-dimension names are kept

- **WHEN** a stored query has the unnamed measures `amount:sum / *:count` and a measure whose derived key is long enough to carry a hash, and a computed dimension persisted with its auto-name
- **THEN** after load the result keys are the ones returned before, and the computed dimension keeps its name and is still treated as auto-named

#### Scenario: Memories

- **WHEN** a memory stored as YAML frontmatter or as a SQLite row bundles a query measuring `amount:sum`, and it is loaded twice
- **THEN** both loads give `sum(amount)` and the query's rows

#### Scenario: The released corpus returns its results

- **WHEN** the committed store written by `motley-slayer==0.10.2` is opened
- **THEN** every colon formula in it loads in the functional spelling and every recorded query returns the rows and result keys recorded from 0.10.2

#### Scenario: Client queries are refused, not rewritten

- **WHEN** a client sends a query without a version measuring `amount:sum`
- **THEN** it is not rewritten and fails with the colon-spelling error

### Requirement: Stored bucket calls are upgraded to date_trunc
On load, a stored model, saved query or memory written before this change SHALL have every Mode-B call `<g>(x)` whose callee names a built-in granularity, or a custom granularity of the datasource it belongs to, rewritten to `date_trunc('<g>', x)` — in query measures, filters, dimensions, time dimensions, order keys and `main_time_dimension`, in saved-measure formulas, and in every nested saved query, model extension and query-backed model — with the rest of the text byte-identical. Mode-A text (`DatasetField.sql`, `sql_table`, model `filters`, custom-aggregation formulas and parameter defaults), structured time dimensions, transform units and `DatasetField.granularity` SHALL be untouched. A call whose callee is a custom aggregation of the owning model and not a granularity SHALL be untouched. Every upgraded query SHALL return the rows and result keys it returned before. The custom-granularity rewrite SHALL use the datasource's settled granularity names (after any library-collision rename) and SHALL complete on the next load when a load is interrupted.

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

### Requirement: Stored root extensions move to query extensions

On load, every stored query written before this change whose `source_model` is an extension object SHALL become a query with `population` naming the extension's base model and that extension as its one `extensions` entry. This SHALL apply recursively through stored `source_queries`, saved queries and memories' queries, including a query nested in a document whose own version is current.
- The upgrade SHALL emit no deprecation warning and SHALL lose no measure or join; the extension's columns SHALL then be inlined as "Stored extension columns are inlined" specifies.
- Repeated loads SHALL change nothing further.
- The upgraded query SHALL return the rows it returned before.
- A save SHALL write the upgraded form.

#### Scenario: A stored query-backed model's stage
- **WHEN** a stored query-backed model has a stage with `source_model` = `{"source_name": "orders", "joins": [...]}`
- **THEN** it loads with `population` `orders` and one extension of `orders` carrying those joins, without a deprecation warning, and returns the rows recorded before the upgrade

#### Scenario: A memory's query in a current-version memory
- **WHEN** a memory at the current memory version holds a query marked with a version before this change whose `source_model` is an extension object
- **THEN** the query loads in the upgraded form, without a deprecation warning, with its extension intact

#### Scenario: An unversioned stored memory query
- **WHEN** a stored memory without a version holds a query whose `source_model` is an extension object
- **THEN** it loads in the upgraded form, without a deprecation warning

#### Scenario: Upgrade is idempotent
- **WHEN** an upgraded document is saved and loaded again
- **THEN** it is unchanged

### Requirement: Stored extension columns are inlined

On load, every stored query written before this change — in a stored query-backed model's `source_queries`, a saved query or a memory's query, recursively through nested stages — whose extensions carry `columns` SHALL be converted so that no extension carries `columns`, with the result keys it returned before, except that a column reached through a join keeps its name as a stage output column while its top-level result key takes the flattened spelling. The conversion SHALL bind the query against the models of its datasource, as the engine resolves them, with the extension columns present, and SHALL replace every reference that resolves to an extension column by that column's expression, qualified by the canonical path the reference resolved through:
- a dimension SHALL become a computed dimension named after the column, and a time dimension an expression time dimension named after it, the column's join path flattened with `__` when it is reached through a join (`customers__tier_label`);
- a measure reading the column SHALL carry an explicit name equal to the name it was generated before, its join path flattened with `__`;
- filters, order items, partition and ranking keys, `main_time_dimension`, extension measures and other extension columns SHALL take the expression in place, extension columns expanding in dependency order;
- an expression is the column's `sql` read as Mode B, wrapped as `iif(<filter>, <sql>, NULL)` when the column has a `filter`, and as `CAST(<expression> AS <type>)` when it declares a type other than `TEXT` or `UNKNOWN`;
- a column without `sql` SHALL become a reference to the extended model's column of its name;
- a column's `label` and `format` SHALL carry over where the converted position holds them.

Each converted column SHALL emit one structured warning naming the document and the column, stating that its text is now read as Mode B. The converted query SHALL return the rows it returned before, except where a construct's Mode-B meaning differs from the dialect's native one (`queries/string-matching`, `queries/regex`, `queries/mixed-type-coercion`).

A stored query SHALL be unconvertible, and the document holding it SHALL fail to load with a typed error naming the document and the construct, through the stored-document load boundary, and stay unchanged in storage, when it holds:
- an inline `source_model`;
- an extension column whose text is not valid Mode B;
- an extension column of type `ARRAY`;
- an extension column without `sql` whose name is not a visible column of the extended model;
- an extension column without `sql` that has a `filter`;
- an extension column with `sql` named like a column of the extended model;
- a dependency cycle among extension columns;
- an extension join whose `join_pairs` names an extension column on either side;
- a reference to an extension column in a position that holds no expression, or one the binder cannot attribute.

A converted query-backed model or saved query SHALL be written back at the current version; a memory SHALL be converted on every load and SHALL NOT be written back.

#### Scenario: A dimension keeps its result key
- **WHEN** a stored query-backed model's stage rooted at `orders` extends `customers` with the column `tier_label` (`sql` `upper(tier)`) and selects the dimension `customers.tier_label` and `sum(amount)`
- **THEN** it loads with the computed dimension `upper(customers.tier)` named `customers__tier_label`, no extension columns, one warning naming the model and `tier_label`, and returns the rows recorded before under the stage column `customers__tier_label`

#### Scenario: A measure keeps its generated name
- **WHEN** a stored stage extends `orders` with `double_amount` (`sql` `amount * 2`, type `DOUBLE`) and selects `sum(double_amount)`
- **THEN** it loads as `sum(CAST(amount * 2 AS DOUBLE))` named `double_amount_sum` and returns the recorded value

#### Scenario: A reference through a named edge is qualified by the edge
- **WHEN** a stored stage rooted at `orders` extends `addresses` with `city_label` and selects `billing_address.city_label`
- **THEN** the expression is qualified as `billing_address.<…>` and returns the recorded rows

#### Scenario: Filtered, chained and sql-less columns
- **WHEN** a stored stage's extension has `q_amount` (`sql` `amount`, `filter` `status = 'ok'`), `q_double` (`sql` `q_amount * 2`) and a sql-less `status`, each used in a filter, an order item and a measure
- **THEN** each use takes `iif(status = 'ok', amount, NULL)`, `iif(status = 'ok', amount, NULL) * 2` and `status` respectively, and returns the recorded rows

#### Scenario: Unconvertible documents fail to load and stay untouched
- **WHEN** stored documents hold, in turn, an inline `source_model`, a column `sql` `strftime('%Y', created_at)`, a sql-less column naming a column the model does not declare, a join keyed on an extension column, and two columns referencing each other
- **THEN** each fails to load with the typed error naming the document and the construct, every other document of the store loads, and the stored bytes are unchanged

#### Scenario: A memory converts on every load
- **WHEN** a stored memory's query carries an extension column and the memory is loaded twice
- **THEN** both loads return the converted query with the warning, and the stored memory is unchanged

#### Scenario: Released stores still open with their results
- **WHEN** the committed 0.10.2 and 1.8.2 stores are opened
- **THEN** every model and memory loads and every recorded query returns the rows recorded from its release

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

### Requirement: Stored queries are upgraded to qualified references
A stored query written before this change — a saved query, a query-backed model's `source_queries` stage, or a memory's query, nested or not, with or without a version of its own — SHALL load in the qualified syntax of `queries/references`, with no deprecation warning, by a conversion that reads the stored datasets:
- each reference SHALL be rewritten to the qualified spelling that has the meaning it had before this change, reading the stage's own population;
- a bare name SHALL read what it read before this change, in this order: inside a metric, dimension or time-dimension entry, or the main time dimension, the population's field, then the population's metric, then an alias of the stage other than the entry's own; in a filter or order, the population's field, then an alias of the stage, then the population's metric; a population that is a stage or a query-backed dataset SHALL offer its output columns as fields;
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

#### Scenario: A metric alias equal to a population column reads the column
- **WHEN** a stored query-backed model's stage over the query-backed model `rev_f`, which outputs the metric column `rev`, measures `sum(rev)` aliased `rev` and filters `rev >= 0`
- **THEN** it loads as `sum(rev_f.rev)` aliased `rev` with the filter `rev_f.rev >= 0`, and returns the sum of `rev_f.rev` over the `rev_f` rows with `rev >= 0`

#### Scenario: A metric reads the population metric a sibling alias shares
- **WHEN** a stored query on `orders`, whose metric `revenue` is `sum(amount)`, has the dimension `status`, measures `count(*)` aliased `revenue` and `revenue * 10` aliased `r10`, and filters `revenue > 1`
- **THEN** it loads as `count(orders.*)` aliased `revenue`, `orders.revenue * 10` aliased `r10` and the filter `revenue > 1`, and returns, for each `status` with more than one order, `r10` equal to ten times its sum of `amount`

#### Scenario: A stage over an earlier stage reads the earlier stage's column
- **WHEN** a stored query-backed model's first stage `s1` outputs `rev`, and its second stage over `s1` measures `sum(rev)` aliased `rev` and filters `rev > 25`
- **THEN** the second stage loads as `sum(s1.rev)` aliased `rev` with the filter `s1.rev > 25`, and the model returns the sum of the `s1` rows with `rev > 25`

#### Scenario: A partition key naming a computed dimension stays bare
- **WHEN** a stored query on `orders` has the dimension `status` and the computed dimension `upper(status)` named `su`, and measures `sum(amount, partition_by=su)`
- **THEN** it loads with `partition_by=su` unchanged and returns the rows it returned before

#### Scenario: A quoted alias matches only its exact spelling
- **WHEN** a stored query on `orders`, which has the field `amount`, has the dimension `status`, measures `count(*)` aliased `"Amount"` and filters `"Amount" > 1`
- **THEN** the filter stays `"Amount" > 1`, reading the metric, and returns each `status` with more than one order

### Requirement: A stored memory whose population names no stored dataset reads one datasource

When a stored memory's query is pinned to no datasource and either names no population or names a population spelling the time spine that no stored dataset spells exactly, its datasource SHALL be the one datasource its dataset-level entities (`<datasource>.<dataset>` or `<datasource>.<dataset>.<leaf>`, naming a stored dataset) name, else the only datasource holding a stored dataset. Every load-time upgrade of it — its qualified-reference upgrade, the typing of its literals, the rewrite of its custom-granularity buckets, and the rename of a library-colliding granularity it references — SHALL read that datasource; a root-less memory SHALL infer its population there as a pinned query does and SHALL fail to load when no single dataset there reaches what it reads; no other datasource is tried. With no such datasource, its custom-granularity buckets SHALL NOT be rewritten, no granularity it references SHALL be renamed, and, when its qualified-reference upgrade is pending, it SHALL fail to load with a warning naming it and saying that its datasource cannot be determined. A stored memory whose explicit population names no stored dataset in any other spelling SHALL NOT use this datasource and SHALL fail to load naming the population. A memory's conversion SHALL run on each load and never be written back; only the rename of a library-colliding granularity SHALL be persisted, as for every stored document.

#### Scenario: A root-less memory loads on a single-datasource store

- **WHEN** the one datasource holds `orders` and `customers`, and a stored v4 memory query with no entities has the dimension `status`, the measure `sum(amount)` named `rev` and the filter `status = 'ok'`, with no population
- **THEN** it loads as `orders.status`, `sum(orders.amount)` and `orders.status = 'ok'`, returns its earlier rows (`ok`: 80), and the stored memory is unchanged

#### Scenario: Its entities pin a root-less memory's datasource over the priority

- **WHEN** datasources `shop` and `shop2` both hold `orders` and `customers`, `status` and `amount` are fields of `orders` in `shop` and of `customers` in `shop2`, `shop` is first in the priority, and the root-less memory above is stored with the entities `shop2.customers`, `shop2.customers.status` and `shop2.customers.amount`
- **THEN** it loads as `customers.status` and `sum(customers.amount)` and returns its rows when run in `shop2`

#### Scenario: A root-less memory's legacy literal is repaired

- **WHEN** the root-less memory filters `ordered_at >= '2024/01/01'` on a single-datasource store
- **THEN** it loads as `orders.ordered_at >= '2024-01-01'` and returns its earlier total (200)

#### Scenario: A root-less memory's literal is typed in the datasource it is read in

- **WHEN** datasources `shop` and `shop2` both hold `orders`, `ordered_at` is a TIMESTAMP in `shop` and TEXT in `shop2`, and the root-less memory filters `ordered_at >= '2024/01/01'`
- **THEN** with the entity `shop.orders` the literal is repaired to `'2024-01-01'`; with the entity `shop2.orders` it is left unchanged

#### Scenario: An entity-pinned memory is read only in its datasource

- **WHEN** the root-less memory reads `status`, its entity `shop.customers` names `shop`, which holds no dataset reaching `status`, and `shop2` holds one
- **THEN** the memory fails to load with a warning naming it and `status`

#### Scenario: An entity-less memory on a multi-datasource store fails to load

- **WHEN** datasources `shop` and `shop2` both hold `orders` and `customers`, and the root-less memory is stored with no entities, or with entities naming no stored dataset or naming both datasources
- **THEN** the memory fails to load with a warning naming it and saying that its datasource cannot be determined

#### Scenario: A root-less memory's custom-granularity bucket is rewritten

- **WHEN** the datasource defines the granularity `mid_month`, and a stored pre-v9 root-less memory buckets `created_at` by `mid_month` and filters `created_at:mid_month >= '2025-02-01'`
- **THEN** it loads with the filter `date_trunc('mid_month', events.created_at) >= '2025-02-01'` and returns its earlier rows

#### Scenario: A root-less memory's library-colliding granularity is renamed

- **WHEN** the datasource defines the granularity `left` and a stored root-less memory buckets `created_at` by `left`
- **THEN** it loads bucketing by `left_granularity` and returns its earlier rows

#### Scenario: An entity-pinned memory's custom granularities follow its datasource

- **WHEN** datasources `g1` and `g2` both hold `events`, only `g2` defines the granularities `left` and `mid_month`, and a stored pre-v9 root-less memory with the entity `g2.events` buckets `created_at` by `left` and filters `created_at:mid_month >= '2025-02-01'`
- **THEN** it loads bucketing by `left_granularity` with the filter `date_trunc('mid_month', events.created_at) >= '2025-02-01'`, and returns its earlier rows when run in `g2`

#### Scenario: A time-spine memory's custom granularities are upgraded

- **WHEN** a stored memory with population `time_spine` buckets `time_spine.timestamp` by the datasource's granularity `left`, or filters `events.created_at:mid_month >= '2025-02-01'` with `mid_month` defined
- **THEN** it loads bucketing by `left_granularity`, or with the filter `date_trunc('mid_month', events.created_at) >= '2025-02-01'`, and returns its earlier rows

#### Scenario: A missing explicit population gets no fallback

- **WHEN** a stored memory names the population `ordrs`, which no datasource stores, and buckets by the datasource's granularity `left`
- **THEN** no datasource is read for it, its granularity is not renamed, and it fails to load naming `ordrs`

### Requirement: Relationships stored before trust upgrade as trusted
Loading a stored model, saved query or memory written before relationships carried
`trusted` SHALL set `trusted: true` on every relationship it stores, its query-extension
relationships and those of its backing queries included, so its results and SQL are
unchanged. Stored semantic-model documents are not upgraded.

#### Scenario: A stored model keeps its inline join
- **WHEN** a model stored at the previous version declares `orders → customers` as
  `many_to_one`, and the data holds customer C1 twice
- **THEN** after loading, the relationship is trusted and `orders.total_revenue` by
  `customers.region` returns EAST 650 as before

#### Scenario: A stored query's extension relationship is trusted
- **WHEN** a saved query stored at the previous version declares a join in its query extension
- **THEN** after loading, that relationship is trusted

#### Scenario: A stored memory's query is upgraded
- **WHEN** a memory stored at the previous version carries a query declaring a join in its
  query extension
- **THEN** after loading, that relationship is trusted
