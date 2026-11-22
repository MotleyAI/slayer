## ADDED Requirements

### Requirement: An untyped document field is a type finding that cleanup writes in
Validation SHALL report each semantic-model document field that declares no `datatype` and whose live type it can determine as a finding `{finding: "untyped", dataset_name, data_source, semantic_model, field, live_type, summary}`, on every validation surface (engine, MCP, REST, CLI). The CLI's `--force-clean` SHALL apply these findings with the same prompt and `--yes` as its deletes, writing into the document: an Ossie scalar type or `Opaque` as the field's `datatype`; a list as `datatype: Opaque` plus a `SLAYER` entry on the field recording the list type and its element type. Applying SHALL change only those fields of the document (`models/semantic-model-documents` › Saves keep the document's text outside what was edited). MCP and REST SHALL NOT apply findings. A field whose live type cannot be determined SHALL NOT be reported as untyped.

#### Scenario: An untyped decimal is reported and written
- **WHEN** document field `orders.amount` declares no `datatype`, its live column is a decimal, and `slayer validate-datasets --force-clean --yes` runs
- **THEN** the report lists `orders.amount` as untyped with live type `Decimal`, and the stored document's field declares `datatype: Decimal` with every other line unchanged

#### Scenario: A list is written as Opaque with a SLAYER entry
- **WHEN** document field `orders.tags` declares no `datatype`, its live column is a list of strings, and `--force-clean --yes` runs
- **THEN** the field declares `datatype: Opaque` and carries a `SLAYER` entry recording a list of `String`
