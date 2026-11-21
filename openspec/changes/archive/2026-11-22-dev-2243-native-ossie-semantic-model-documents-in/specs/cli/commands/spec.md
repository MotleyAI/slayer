## MODIFIED Requirements

### Requirement: export-osi writes an Ossie document
`slayer export-osi --datasource X --out <file>` SHALL write the datasource's semantic model as one Ossie document to `<file>` (`importers/osi-export`), as JSON when the file name ends in `.json` and as YAML otherwise, and SHALL print the export report. `--datasets` SHALL limit the export to the named datasets, and `--no-extensions` SHALL omit the `SLAYER` extensions. An unknown datasource or an unknown dataset name SHALL fail as a CLI error and write no file.

#### Scenario: Export to YAML
- **WHEN** `slayer export-osi --datasource jaffle_shop --out shop.osi.yaml` runs
- **THEN** `shop.osi.yaml` holds a YAML Ossie document of the datasource's semantic model, the report is printed, and the exit status is 0

#### Scenario: Unknown model
- **WHEN** `slayer export-osi --datasource jaffle_shop --datasets nope --out shop.osi.yaml` runs
- **THEN** stderr holds one `Error: ` line naming `nope`, no file is written, and the exit status is 1

## ADDED Requirements

### Requirement: semantic-models lists, shows and deletes documents
`slayer semantic-models list [--datasource X]` SHALL print each stored document's datasource, name and datasets; `slayer semantic-models show <name> --datasource X` SHALL print the document's stored text; `slayer semantic-models delete <name> --datasource X` SHALL delete the document and its datasets after a confirmation prompt that `--yes` skips. An unknown document SHALL fail as a CLI error.

#### Scenario: Show prints the stored text
- **WHEN** `slayer semantic-models show sales --datasource shop` runs
- **THEN** stdout holds the stored text of document `sales` and the exit status is 0

#### Scenario: Unknown document
- **WHEN** `slayer semantic-models delete nope --datasource shop --yes` runs
- **THEN** stderr holds one `Error: ` line naming `nope` and the exit status is 1

### Requirement: ossie-roundtrip imports and exports one document
`slayer ossie-roundtrip <in> <out>` SHALL import the Ossie document `<in>` natively into scratch storage under a scratch datasource, export that datasource to `<out>` (JSON when `<out>` ends in `.json`, YAML otherwise), leave the user's storage untouched, and exit non-zero with the import's reasons when `<in>` is refused.

#### Scenario: A valid document round-trips unchanged
- **WHEN** `slayer ossie-roundtrip m4_cross_dataset.ossie.yaml out.yaml` runs
- **THEN** `out.yaml` equals the input text, the configured storage is unchanged, and the exit status is 0
