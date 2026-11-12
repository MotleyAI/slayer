## ADDED Requirements

### Requirement: Stored date-only comparisons keep their whole-day meaning

On load, a stored model, saved query or memory written before this change — judged by each nested document's own stored version, so a current document holding an older nested query, inline model, extension or memory query is upgraded where it nests — SHALL have every Mode-B comparison of a temporal operand with a date-only literal `D` rewritten to the comparison it meant before: `x <= 'D'` → `x < '<D+1>'`; `x > 'D'` → `x >= '<D+1>'`; `x = 'D'` → `x in 'D'`; `x != 'D'` → `x not in 'D'`; `x BETWEEN 'A' AND 'B'` → `(x >= 'A' and x < '<B+1>')` and `x NOT BETWEEN 'A' AND 'B'` → `(x < 'A' or x >= '<B+1>')`, each with only date-only bounds rewritten; a literal on the left mirrored. `x >= 'D'` and `x < 'D'` SHALL be untouched. This covers query measures, filters, dimensions, time dimensions, order keys and `main_time_dimension`, saved-measure formulas, Mode-B column filters, and every nested saved query, inline source model, model extension, query-backed model and memory query, with the rest of the text byte-identical and no warning on the response. Whether an operand is temporal SHALL be decided with the models of the document's datasource. A comparison with a non-temporal operand, an operand whose type cannot be decided (an untyped column, a missing or unloadable model), a `BETWEEN` whose tested expression is the literal, and a comparison with a variable SHALL be left unchanged, each untypable one with a logged warning naming the document and the text. Mode-A text and `date_range` SHALL be untouched. The upgrade SHALL be idempotent and SHALL complete on the next load when a load is interrupted. Every upgraded query SHALL return the rows it returned before.

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
