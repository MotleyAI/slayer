## ADDED Requirements

### Requirement: The CLI prints each deprecation once
Within one `slayer` command, each deprecation SHALL reach the operator as exactly one line. A deprecation shown as its `Deprecated: …` line SHALL NOT be printed again from the command's response warnings, and identical deprecation lines from the response warnings SHALL print once. A deprecation carried only on the response — because it emits no Python warning, or because the Python warning was filtered out or already shown earlier in the process — SHALL still print once. A deprecation's Python warning and its structured response warning SHALL carry the same text. This SHALL hold for the lines `slayer query`, `slayer memory save` and `slayer search` (text output) print; warnings that are not deprecations SHALL print as before, and structured JSON output and `slayer inspect`'s rendered document SHALL keep every warning they carry.

#### Scenario: A query with several deprecated forms prints each once
- **WHEN** `slayer query` runs a query using `source_model`, `measures`, `filters`, `to_many_handling: "broadcast"` and the `==` comparison
- **THEN** stderr carries exactly one line per deprecation

#### Scenario: A deprecation carried only on the response prints once
- **WHEN** `slayer query` runs by name a saved query stored with `to_many_handling: "broadcast"` on two stages
- **THEN** stderr carries the broadcast deprecation exactly once

#### Scenario: memory save prints each deprecation once
- **WHEN** `slayer memory save --query` is given a query using `source_model` and `measures`
- **THEN** the output carries each deprecation exactly once

#### Scenario: search prints a renamed label once
- **WHEN** `slayer search` runs with a `--cypher-filter` using the pre-2.0 label `:Model`
- **THEN** the text output carries the label's deprecation exactly once

#### Scenario: search JSON keeps its warnings
- **WHEN** `slayer search --format json` runs with a `--cypher-filter` using `:Model`
- **THEN** the JSON response's `warnings` carries the label's deprecation

#### Scenario: Filtered Python warnings still leave one line
- **WHEN** a deprecated query runs with Python's warnings filtered out, or runs a second time in the same process
- **THEN** each deprecation still prints exactly once

#### Scenario: A warning that is not a deprecation still prints
- **WHEN** a query's response carries a warning that is not a deprecation
- **THEN** the CLI prints it

#### Scenario: A spelling deprecation has one wording
- **WHEN** a query uses a deprecated Mode-B spelling
- **THEN** its Python warning's text equals its structured response warning's text
