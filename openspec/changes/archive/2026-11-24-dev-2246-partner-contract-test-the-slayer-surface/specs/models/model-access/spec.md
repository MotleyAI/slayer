## MODIFIED Requirements

### Requirement: Models carry access tags

A model SHALL carry `access_tags`, a list of strings that defaults to empty. An empty list means the model is public. The tags SHALL round-trip unchanged through model JSON, storage, git sync, import and export, REST, the Python client, and MCP `create_dataset` / `edit_dataset`. SLayer SHALL accept any string as a tag. A stored model without the field SHALL load as untagged, and models SHALL be written at the current schema version; schema version 15 introduced `access_tags`. Access tags are independent of the `hidden` flag: `hidden` curates the catalog for everyone and keeps the model queryable.

#### Scenario: Tags round-trip through MCP and storage

- **WHEN** `create_dataset` is called with `access_tags=["hr"]` and the model is read back through storage, REST and inspect
- **THEN** each returns `access_tags == ["hr"]`

#### Scenario: A pre-v15 model loads untagged

- **WHEN** a stored v14 model is loaded
- **THEN** it has `access_tags == []`, the stored document is unchanged, and a save or `slayer storage upgrade` persists it at the current version

#### Scenario: A non-conforming tag is kept

- **WHEN** a model with the tag `"HR Team"` is saved and loaded
- **THEN** it keeps the tag `"HR Team"`
