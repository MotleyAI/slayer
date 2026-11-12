## Purpose

Defines how Mode-B expressions read scalar values out of JSON: `json_value` with an optional typed result, the Postgres and MySQL arrow chains, the accepted aliases, and the JSON inputs each dialect reads.

## ADDED Requirements

### Requirement: json_value reads one scalar

`json_value(x, path [RETURNING type])` SHALL return the scalar at `path` in the JSON document `x`. It SHALL yield NULL when `x` is NULL or not valid JSON, the path does not exist, or the value there is JSON `null`, an object or an array. A JSON string SHALL be returned unquoted; a JSON boolean as `'true'` / `'false'`; a JSON number in the database's own number-to-text rendering. On Postgres older than 16, or of unknown version, invalid JSON input SHALL follow the `TRY_CAST` contract of `queries/casts` and fail the query.

#### Scenario: Scalars, missing and non-scalars

- **WHEN** a dimension is `json_value(payload, '$.a.b')` over rows holding `{"a": {"b": "x"}}`, `{"a": {}}`, `{"a": {"b": null}}`, `{"a": {"b": [1]}}` and `'not json'`
- **THEN** they yield `'x'`, NULL, NULL, NULL and NULL on every Tier-1 dialect (Postgres 16+)

#### Scenario: Boolean as text

- **WHEN** a dimension is `json_value(payload, '$.ok')` over a row holding `{"ok": true}`
- **THEN** it yields `'true'`

### Requirement: The path subset

A path SHALL be a string literal or `{var}` placeholder made of `$` followed by any sequence of `.key`, `."quoted key"` and `[n]` with `n` a non-negative integer. Wildcards, filters, recursive descent, negative indices and anything else SHALL be rejected at plan time with a typed error naming the construct.

#### Scenario: Array index and quoted key

- **WHEN** a dimension is `json_value(payload, '$.items[1]."unit price"')` over a row holding `{"items": [{}, {"unit price": "9.5"}]}`
- **THEN** it yields `'9.5'`

#### Scenario: Wildcard

- **WHEN** a dimension is `json_value(payload, '$.items[*].sku')`
- **THEN** the query fails at plan time naming the wildcard

### Requirement: RETURNING converts or yields NULL

`RETURNING INT | DOUBLE | BOOLEAN | DATE | TIMESTAMP | TEXT` SHALL type the result (default TEXT). A value that cannot convert SHALL yield NULL, with the `TRY_CAST` meaning of `queries/casts`; a JSON boolean SHALL convert to BOOLEAN directly, never through its text; a JSON number SHALL convert to INT / DOUBLE directly.

#### Scenario: Typed result

- **WHEN** a measure is `sum(json_value(payload, '$.qty' RETURNING INT))` over rows holding `{"qty": 2}`, `{"qty": "3"}`, `{"qty": "x"}` and `{}`
- **THEN** it yields 5 on every Tier-1 dialect (Postgres 16+)

#### Scenario: Genuine zero and false are kept

- **WHEN** dimensions are `json_value(payload, '$.n' RETURNING INT)` and `json_value(payload, '$.f' RETURNING BOOLEAN)` over a row holding `{"n": 0, "f": false}`, and over a row holding `{}`
- **THEN** the first row yields `0` and `false`, the second NULL and NULL, on every Tier-1 dialect

### Requirement: JSON inputs

`x` SHALL be a TEXT expression or a column of the database's native JSON type (Postgres `json` / `jsonb`, MySQL / DuckDB / BigQuery `JSON`, Snowflake `VARIANT` / `OBJECT` / `ARRAY`, ClickHouse `JSON`); ingestion SHALL record such a column's database type so its native form is read. A column whose database type is not recorded SHALL be read as TEXT.

#### Scenario: Native JSON column

- **WHEN** a Postgres table has a `jsonb` column `payload`, its model is ingested, and a dimension is `json_value(payload, '$.a')`
- **THEN** the query runs and yields the value at `$.a`

#### Scenario: Snowflake VARIANT

- **WHEN** a Snowflake table has a `VARIANT` column `payload`, its model is ingested, and a dimension is `json_value(payload, '$.a' RETURNING INT)`
- **THEN** the query runs and an unconvertible value yields NULL

### Requirement: Arrow chains

`x -> k1 -> … ->> kn` (Postgres form) and `x ->> '$.path'` (MySQL form) SHALL mean `json_value(x, path)` with TEXT result: a string operand starting with `$` is a path, any other string a key, an integer an array index. Every operand SHALL be a literal. The arrows SHALL bind as Postgres binds them: at the precedence of `||`, left-associative. A chain ending in `->` (a JSON-valued result) SHALL be rejected with a hint to end it with `->>`.

#### Scenario: Postgres chain

- **WHEN** a dimension is `payload -> 'a' -> 0 ->> 'b'` over a row holding `{"a": [{"b": "x"}]}`
- **THEN** it yields `'x'`, the same as `json_value(payload, '$.a[0].b')`

#### Scenario: MySQL path form

- **WHEN** a dimension is `payload ->> '$.a.b'`
- **THEN** it means `json_value(payload, '$.a.b')`

#### Scenario: Bare arrow

- **WHEN** a dimension is `payload -> 'a'`
- **THEN** the query fails with a hint to use `->>`

#### Scenario: Postgres precedence

- **WHEN** a dimension is `payload ->> 'n' || 'x'`
- **THEN** it means `(payload ->> 'n') || 'x'`

### Requirement: JSON aliases

`json_extract(x, path)` and `json_extract_scalar(x, path)` SHALL mean `json_value(x, path)` (exactly two arguments, no `RETURNING`); `json_extract_path_text(x, k1, …)` and `jsonb_extract_path_text(x, k1, …)` SHALL mean `json_value` over the path of those keys.

#### Scenario: Key-list alias

- **WHEN** a dimension is `json_extract_path_text(payload, 'a', 'b')`
- **THEN** it means `json_value(payload, '$.a.b')`

#### Scenario: json_extract of an object

- **WHEN** a dimension is `json_extract(payload, '$.a')` over a row holding `{"a": {"b": 1}}`
- **THEN** it yields NULL
