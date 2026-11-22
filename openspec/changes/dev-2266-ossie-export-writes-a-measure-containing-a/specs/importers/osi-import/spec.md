## MODIFIED Requirements

### Requirement: Aggregate operands

A non-bare aggregate operand (arithmetic, `CASE`, scalar functions, or, in a model-scoped metric, an operand combining fields of several datasets) SHALL be carried in the measure's formula when the formula language can express it. Ossie's `DATE_TRUNC` with one of Ossie's truncation parts, `CAST` to any of Ossie's types, `DATEADD` with a part the formula's `date_add` supports, and `DATE_PART`, `EXTRACT` and the one-argument extraction functions (`YEAR`, `QUARTER`, `MONTH`, `DAY`, `DAYOFYEAR`, `HOUR`, `MINUTE`, `SECOND`) with a part whose numbering Ossie fixes SHALL count as expressible, each carried as the formula function of the same meaning. Otherwise the importer SHALL create a hidden helper column holding the operand on its single owning dataset, typed from the database. An operand that is neither expressible nor confined to one dataset SHALL make the metric reported as unconverted. A string literal in a metric SHALL keep its value exactly, including any `'` (written `''`) and backslash characters.

#### Scenario: Arithmetic operand
- **WHEN** a dataset-scoped metric is `SUM(quantity * amount)`
- **THEN** the measure returns the sum of `quantity * amount` and the model gains no hidden column for it

#### Scenario: Cross-dataset operand
- **WHEN** a model-scoped metric is `SUM(orders.amount - customers.discount)` and `orders` joins `customers` many-to-one
- **THEN** the measure imports and returns the sum over orders of `amount - discount` of each order's customer

#### Scenario: Operand needing a database-specific function
- **WHEN** a dataset-scoped metric aggregates a function the formula language does not offer
- **THEN** the model gains a hidden column holding the operand and the measure aggregates it

#### Scenario: Quote and backslash in a literal
- **WHEN** a dataset-scoped metric is `COUNT(*) FILTER (WHERE note = 'O''Brien\x')` and one row's `note` is `O'Brien\x`
- **THEN** the measure imports and returns 1

#### Scenario: Date truncation operand
- **WHEN** a dataset-scoped metric on a SQLite datasource is `COUNT(DISTINCT DATE_TRUNC('month', CAST(ordered_at AS TIMESTAMP)))`
- **THEN** the measure's formula is `count_distinct(date_trunc('month', CAST(ordered_at AS TIMESTAMP)))`, the model gains no hidden column for it, and the measure returns the number of distinct months

#### Scenario: Date addition operand
- **WHEN** a dataset-scoped metric on a SQLite datasource is `MAX(DATEADD(day, 1, ordered_at))`
- **THEN** the measure's formula carries `date_add(ordered_at, 1, 'day')`, the model gains no hidden column for it, and the measure returns the latest order time plus one day

#### Scenario: Extraction operand
- **WHEN** dataset-scoped metrics on a SQLite datasource are `COUNT(DISTINCT DATE_PART(month, ordered_at))`, `COUNT(DISTINCT EXTRACT(MONTH FROM ordered_at))` and `COUNT(DISTINCT MONTH(ordered_at))`
- **THEN** each measure's formula carries `date_part('month', ordered_at)`, the model gains no hidden column for them, and each returns the number of distinct months of year

#### Scenario: Extraction part without a fixed numbering
- **WHEN** a dataset-scoped metric is `COUNT(DISTINCT DATE_PART(dayofweek, ordered_at))`
- **THEN** the model gains a hidden column holding the operand and the measure aggregates it
