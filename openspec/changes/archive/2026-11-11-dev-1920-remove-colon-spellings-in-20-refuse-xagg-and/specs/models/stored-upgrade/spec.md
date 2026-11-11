## ADDED Requirements

### Requirement: Stored colon spellings are upgraded to the functional spelling

On load, every Mode-B surface of a stored model, saved query or memory written before this change — `ModelMeasure.formula`, and a stored query's measures, dimensions, computed-dimension expressions, time-dimension columns, order columns and expressions, `main_time_dimension` and filters, recursively through stored `source_queries`, saved queries and inline `ModelExtension.measures` — SHALL have every colon span rewritten to its twin: `date_trunc('<name>', <source>)` when `<name>` is a built-in granularity, and `<name>(<source>[, <args>])` otherwise, with the source and arguments as written and colon spans inside the arguments rewritten too. Everything outside a colon span SHALL stay byte-identical, and a colon inside a string literal, a quoted identifier or a `::` cast SHALL be untouched. A stored order item whose expression is rewritten SHALL load exactly as the same order key written fresh in the functional spelling. The document SHALL return the rows and result keys it returned before; a persisted computed-dimension name SHALL be kept and keep its meaning. A datasource granularity rename SHALL apply before the rewrite. Stored text that cannot otherwise be parsed SHALL be left unchanged, with a warning naming the document and the field. Mode-A surfaces SHALL NOT be rewritten, and a client query without a version SHALL NOT be rewritten.

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
- **THEN** they load as `date_trunc('month', created_at)` and `fiscal_quarter(created_at)` and return the rows they returned before

#### Scenario: A granularity renamed on load

- **WHEN** a stored datasource defines the granularity `left`, and a stored model's source query and a memory's query bucket by `created_at:left`
- **THEN** after load they read `left_granularity(created_at)` and return the rows they returned before

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
