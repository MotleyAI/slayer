## MODIFIED Requirements

### Requirement: json_value reads one scalar

`json_value(x, path [RETURNING type])` SHALL return the scalar at `path` in the JSON document `x`. It SHALL yield NULL when `x` is NULL or not valid JSON, the path does not exist, or the value there is JSON `null`, an object or an array. A JSON string SHALL be returned unquoted; a JSON boolean as `'true'` / `'false'`; a JSON number in the database's own number-to-text rendering. On Postgres older than 16, or of unknown version, invalid JSON input SHALL follow the `TRY_CAST` contract of `queries/casts` and fail the query. On SQL Server, a scalar longer than 4,000 characters SHALL fail the query rather than read as NULL on 2022 or later (and when the version is unknown); before 2022 it reads as NULL, and this is documented.

#### Scenario: Scalars, missing and non-scalars

- **WHEN** a dimension is `json_value(payload, '$.a.b')` over rows holding `{"a": {"b": "x"}}`, `{"a": {}}`, `{"a": {"b": null}}`, `{"a": {"b": [1]}}` and `'not json'`
- **THEN** they yield `'x'`, NULL, NULL, NULL and NULL on every Tier-1 dialect (Postgres 16+)

#### Scenario: Boolean as text

- **WHEN** a dimension is `json_value(payload, '$.ok')` over a row holding `{"ok": true}`
- **THEN** it yields `'true'`

#### Scenario: Scalar document on SQL Server

- **WHEN** a query on SQL Server 2022 or later has the dimension `json_value(payload, '$')` over a row holding `"x"`
- **THEN** it yields `'x'`

#### Scenario: Long scalar on SQL Server

- **WHEN** a query on SQL Server 2022 or later groups by `json_value(payload, '$.a')` over rows holding a 5,000-character string at `a`, a short string, an object, JSON `null` and no `a`
- **THEN** the query fails with SQL Server's truncation error instead of returning NULL for the long string, and without the long row it returns the short string and NULL for the others
