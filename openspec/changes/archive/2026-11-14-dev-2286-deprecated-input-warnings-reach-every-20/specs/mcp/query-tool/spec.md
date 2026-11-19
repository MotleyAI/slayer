## MODIFIED Requirements

### Requirement: Execution wrappers and output shaping

The `query` tool SHALL support, uniformly across all three input forms: `variables` (merged with precedence runtime > named-stage > outer-query > model query variables), `dry_run` (return generated SQL without executing, together with the run's warnings), `explain` (return SQL plus the query plan), `show_sql` (prefix results with the SQL), and `format` in {`markdown`, `json`, `csv`} case-insensitively — any other value SHALL be rejected with an error naming the valid options. When a result carries dimension/measure attribute metadata, the attributes SHALL be rendered machine-safely for the chosen format, regardless of input form: embedded inside the json payload, as leading `#` comment lines in csv, and as a prose footer (before any warnings) in markdown — never as trailing prose that would break `json.loads` or the csv column count. A dry run's warnings SHALL be rendered for the chosen format too: under json the reply SHALL be one object holding the SQL and, when any, the structured warnings; under csv each warning SHALL be a leading `#` comment line before the SQL; under markdown they SHALL follow the SQL as the warnings block.

#### Scenario: Dry run returns SQL only

- **WHEN** `query` is called with any valid `query` value and `dry_run=true`
- **THEN** the response contains the generated SQL and no result rows

#### Scenario: A dry run carries its warnings in every format

- **WHEN** `query` is called with `dry_run=true` for a query whose run carries a warning, once per format
- **THEN** the json reply parses as an object whose `sql` holds the SQL and whose `warnings` holds the structured warning; the csv reply carries the warning as a leading `#` comment line before the SQL; and the markdown reply carries the SQL followed by the warning in its warnings block

#### Scenario: Invalid format is rejected

- **WHEN** `query` is called with `format="xml"`
- **THEN** an error lists the valid formats json, csv, and markdown

#### Scenario: Attributes stay machine-safe on run-by-name results

- **WHEN** `query` is called with a query-backed model name whose result carries attribute metadata, in json or csv format
- **THEN** the response stays parseable — the attributes ride inside the json payload and appear as leading `#` comment lines in csv, never as trailing prose
