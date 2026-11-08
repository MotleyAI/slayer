## ADDED Requirements

### Requirement: Stored Mode-B text is upgraded to the SQL string and identifier rules

On load, every Mode-B surface of a stored model, saved query or memory written before this change — `ModelMeasure.formula`, and a stored query's measures, dimensions, computed-dimension expressions, time-dimension columns, order columns and expressions, `main_time_dimension` and filters, recursively through stored `source_queries`, saved queries and inline `ModelExtension.measures` — SHALL be rewritten so that it means under the current grammar what it meant when stored: a double-quoted string literal becomes the equivalent single-quoted literal, a string literal using backslash escapes is re-encoded with its decoded value, and a `|` concatenation becomes `||`. The rewrite SHALL be token-exact: text inside string literals is never otherwise touched, and stored text that cannot be tokenised is left unchanged. Mode-A surfaces SHALL NOT be rewritten, and a client query without a version SHALL NOT be rewritten.

#### Scenario: Double-quoted strings in a stored query

- **WHEN** a stored query written before this change filters `status == "paid"` and has measure `sum(iif(region == "EU", amount, 0))`
- **THEN** it loads as `status == 'paid'` and `sum(iif(region == 'EU', amount, 0))` and returns the rows it returned before

#### Scenario: Escapes and embedded quotes

- **WHEN** a stored filter is `name == "O'Brien"`, another `name == 'O\'Brien'`, and a stored measure formula compares with `"tab\there"`
- **THEN** they load as `name == 'O''Brien'` (twice) and a single-quoted literal containing a real tab character, and match the same rows as before

#### Scenario: Pipe concatenation

- **WHEN** a stored computed dimension is `status | region`
- **THEN** it loads as `status || region` and returns the same values

#### Scenario: Mode-A text untouched

- **WHEN** a stored model has `Column.sql` `"Order Total" * 2` and a model filter `status = "x"`
- **THEN** both load byte-for-byte unchanged

#### Scenario: Client queries are not rewritten

- **WHEN** a client sends a query without a version that filters `status = "paid"`
- **THEN** it is not rewritten and fails with the double-quoted-name hint unless a column named `paid` exists

#### Scenario: Released stores still open with their results

- **WHEN** the committed 0.10.2 store is opened
- **THEN** every model and memory loads and every recorded query returns the rows recorded from 0.10.2
