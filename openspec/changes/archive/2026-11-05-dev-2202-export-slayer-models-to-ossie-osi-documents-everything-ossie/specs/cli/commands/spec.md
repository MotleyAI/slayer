## ADDED Requirements

### Requirement: export-osi writes an Ossie document
`slayer export-osi --datasource X --out <file>` SHALL write the datasource's models as an Ossie document to `<file>`, as JSON when the file name ends in `.json` and as YAML otherwise, and SHALL print the export report. `--models` SHALL limit the export to the named models, and `--no-extensions` SHALL omit the `SLAYER` extensions. An unknown datasource or an unknown model name SHALL fail as a CLI error and write no file.

#### Scenario: Export to YAML
- **WHEN** `slayer export-osi --datasource jaffle_shop --out shop.osi.yaml` runs
- **THEN** `shop.osi.yaml` holds a YAML Ossie document of the datasource's models, the report is printed, and the exit status is 0

#### Scenario: Unknown model
- **WHEN** `slayer export-osi --datasource jaffle_shop --models nope --out shop.osi.yaml` runs
- **THEN** stderr holds one `Error: ` line naming `nope`, no file is written, and the exit status is 1
