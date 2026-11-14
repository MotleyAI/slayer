## MODIFIED Requirements

### Requirement: Stored date-only comparisons keep their whole-day meaning

On load, a stored model, saved query or memory written before this change — judged by each nested document's own stored version, so a current document holding an older nested query, extension or memory query is upgraded where it nests — SHALL have every Mode-B comparison of a temporal operand with a date-only literal `D` rewritten to the comparison it meant before: `x <= 'D'` → `x < '<D+1>'`; `x > 'D'` → `x >= '<D+1>'`; `x = 'D'` → `x in 'D'`; `x != 'D'` → `x not in 'D'`; `x BETWEEN 'A' AND 'B'` → `(x >= 'A' and x < '<B+1>')` and `x NOT BETWEEN 'A' AND 'B'` → `(x < 'A' or x >= '<B+1>')`, each with only date-only bounds rewritten; a literal on the left mirrored. On the last representable day `9999-12-31`, `< '<D+1>'` SHALL be `<= 'D 23:59:59.999999'` and `>= '<D+1>'` SHALL be `> 'D 23:59:59.999999'`. `x >= 'D'` and `x < 'D'` SHALL be untouched. This covers query measures, filters, dimensions, time dimensions, order keys and `main_time_dimension`, saved-measure formulas, Mode-B column filters, and every nested saved query, model extension, query-backed model and memory query, with the rest of the text byte-identical and no warning on the response. Whether an operand is temporal SHALL be decided with the models of the document's datasource, the built-in time spine included: an unquoted model name spelt `time_spine` in any case SHALL name the spine unless a stored model has exactly that spelling, and SHALL be untypable when it also reaches a stored model in another case; a path SHALL reach the spine from any model, a query or extension on the spine SHALL reach the datasource's models, and a path that continues past the spine SHALL be untypable. A comparison with a non-temporal operand, an operand whose type cannot be decided (an untyped column, a missing or unloadable model), a `BETWEEN` whose tested expression is the literal, and a comparison with a variable SHALL be left unchanged, each untypable one with a logged warning naming the document and the text. Mode-A text and `date_range` SHALL be untouched. The upgrade SHALL be idempotent and SHALL complete on the next load when a load is interrupted. Every upgraded query SHALL return the rows it returned before.

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

- **WHEN** a stored query compares a column with no recorded type, or a column of a model that no longer exists, with `<= '2024-12-31'`
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

#### Scenario: Spine reached through an extension's join

- **WHEN** a stored query on `orders` has an extension adding a join from `orders` to `customers` and the filter `customers.time_spine.timestamp <= '2024-01-31'`
- **THEN** after load it reads `customers.time_spine.timestamp < '2024-02-01'`

#### Scenario: Spine spelling that is untypable

- **WHEN** a datasource stores a model named `Time_Spine` (saved before the spine's name was reserved) and a stored query has the filter `TIME_SPINE.timestamp <= '2024-01-31'`; and a stored query has the filter `orders.time_spine.customers.created_at <= '2024-01-31'`
- **THEN** both load byte-identical, each with a warning naming the document and the comparison logged

#### Scenario: Stored names shadow the spine

- **WHEN** a stored query on `orders` filters `time_spine.code <= '2024-01-31'` where `orders` has a stored join named `time_spine` to a model whose `code` is TEXT; a stored model's source query on `orders` filters `time_spine.timestamp <= '2024-01-31'` while a sibling source query is named `time_spine`; and a datasource stores a model named exactly `time_spine` whose `timestamp` column is TEXT, read by a stored query filtering `time_spine.timestamp <= '2024-01-31'`
- **THEN** the first and third load byte-identical without a warning, and the second loads byte-identical with a warning naming the document and the comparison

#### Scenario: Legacy time literal on the spine

- **WHEN** a stored query written before legacy time literals were repaired, rooted at `time_spine`, compares `timestamp` with a legacy date literal for `2024-01-31` using `<=`
- **THEN** after load the literal is repaired and the comparison reads `timestamp < '2024-02-01'`
