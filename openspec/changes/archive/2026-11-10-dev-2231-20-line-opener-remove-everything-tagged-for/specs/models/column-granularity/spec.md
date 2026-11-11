## REMOVED Requirements

### Requirement: Model-editing surfaces round-trip the granularity
**Reason**: Restated in "Model-editing and inspection surfaces round-trip the granularity": the MCP `inspect_model` tool its last scenario names is removed in SLayer 2.0.
**Migration**: None.

## ADDED Requirements

### Requirement: Model-editing and inspection surfaces round-trip the granularity

The MCP `create_model` and `edit_model` tools SHALL accept `granularity` on a column definition and document it among the column fields; `edit_model` SHALL preserve an existing granularity when a partial update omits it, clear it when the update passes `null`, and report the construction error when the update would put a granularity on a non-temporal column. The column summary of `inspect` of a model SHALL show the granularity when one is set.

#### Scenario: edit_model sets, preserves and clears a granularity

- **WHEN** `edit_model` upserts `created_at` with `granularity: "month"`, then upserts the same column with only a new `description`, then upserts it with `granularity: null`
- **THEN** the persisted column's granularity is `month`, still `month`, then unset, in that order

#### Scenario: edit_model rejects a granularity on a non-temporal column

- **WHEN** `edit_model` upserts a `string` column with `granularity: "month"`
- **THEN** the tool reports an invalid-column error naming the column, `month` and the type, and the model is not changed

#### Scenario: inspect shows a set granularity

- **WHEN** a column with `granularity: month` is inspected through `inspect` of its model
- **THEN** its summary entry carries `granularity: month`, and a column without one carries no such entry
