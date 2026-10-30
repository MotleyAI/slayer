# results/value-encoding Specification

## Purpose
Defines how every output path — MCP json / csv / markdown, REST `/query`, the CLI, the Postgres wire and Arrow
Flight — writes each result value, so one value reads the same wherever it is returned.

## Requirements

### Requirement: One encoding of each result value, in a JSON form and a text form

Every result value SHALL have one JSON form and one text form, used by every output path. The JSON form SHALL be
used by MCP `format="json"`, REST `/query` and the CLI's json output, and inside every list and JSON text. The text
form SHALL be used by MCP `format="csv"` / `"markdown"`, the CLI's table output, Postgres-wire text-typed columns
and Arrow Flight UTF-8 columns. The forms SHALL be:

| Value | JSON form | Text form |
|---|---|---|
| list | JSON array of its elements' JSON forms | that JSON array's text |
| JSON object (a dict) | JSON object, keys as strings | that JSON object's text |
| UUID | its canonical string | the same string |
| bytes | the string `\x` followed by lowercase hex digits | the same `\x…` text |
| interval | an ISO-8601 duration string (`P1DT2H3M4.5S`; a negative interval prefixed `-`) | the same duration |
| decimal | a JSON number with exactly the decimal's digits, never rounded through a float | the decimal's exact digits |
| non-finite number (NaN, ±Infinity, float or decimal) | `null` | a scalar cell: `NaN` / `Infinity` / `-Infinity`; inside a list or JSON text: `null` |
| date / timestamp | ISO-8601 string (a timestamp with a time zone keeps its offset) | ISO-8601 text |
| text / integer / boolean | the JSON string / number / `true` / `false` | as before |

A JSON value the database driver has already decoded to a scalar SHALL be written as that scalar. Every JSON form
SHALL be valid JSON.

#### Scenario: UUID list
- **WHEN** a query returns `collect(id)` over a UUID column on DuckDB
- **THEN** `format="json"` returns a JSON array of the UUID strings, and csv / markdown, the Postgres wire and Arrow
  Flight return that array's JSON text

#### Scenario: Exact decimals
- **WHEN** a query returns the decimal `12345678901234567890.12` in a cell and inside a list
- **THEN** every JSON output carries the number `12345678901234567890.12` with those exact digits, and every text
  output carries `12345678901234567890.12`

#### Scenario: Non-finite numbers in JSON
- **WHEN** a result holds a NaN cell and a list `[1.5, NaN, Infinity]`
- **THEN** `format="json"` and REST return valid JSON with `null` for each non-finite number, while the Postgres
  wire returns the scalar cell as `NaN`

#### Scenario: Bytes and intervals
- **WHEN** a result holds the bytes `DE AD BE EF` and an interval of 1 day 2 hours
- **THEN** every output path carries `\xdeadbeef` and `P1DT2H`

#### Scenario: JSON objects
- **WHEN** a Postgres `jsonb` column returns `{"a": [1, "x"]}`
- **THEN** every JSON output carries that object and every text output (including the Postgres wire and Arrow
  Flight) carries its JSON text, never a programming-language representation

#### Scenario: Same value on every path
- **WHEN** the same result is returned through MCP json, csv and markdown, REST, the CLI, the Postgres wire and Arrow
  Flight
- **THEN** each value's text is identical across the text-form paths and each value's JSON is identical across the
  JSON-form paths
