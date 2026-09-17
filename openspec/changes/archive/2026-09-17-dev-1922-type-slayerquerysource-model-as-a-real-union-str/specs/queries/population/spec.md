## ADDED Requirements

### Requirement: Population specs are typed at construction

A query's `source_model` SHALL accept exactly three forms — a saved model name (a string),
an inline model extension (an object carrying `source_name` plus optional `columns`,
`measures`, `joins`), and an inline model (an object without `source_name`) — and SHALL
validate each into its typed form when the query is constructed, before any engine
resolution. An object carrying `source_name` SHALL always be validated as an extension,
never as an inline model. An extension object carrying any other key SHALL be rejected at
construction. A value that is none of the three forms SHALL be rejected at construction.
Validation errors SHALL name the offending keys of the one form the object was classified
as. The query's JSON schema SHALL list the three forms for `source_model`.

#### Scenario: Extension object validates as a typed extension

- **WHEN** a query is constructed with `source_model` =
  `{"source_name": "orders", "columns": [{"name": "double_amount", "sql": "amount * 2"}]}`
- **THEN** the query's `source_model` is a typed extension whose `columns` (and `joins`)
  are typed objects, and the query executes exactly as before

#### Scenario: Inline model object validates as a typed model

- **WHEN** a query is constructed with `source_model` =
  `{"name": "ad_hoc", "sql_table": "things", "data_source": "ds", "columns": [...]}`
- **THEN** the query's `source_model` is a typed model and the query executes exactly as
  before

#### Scenario: Typed instances pass through

- **WHEN** a query (or a REST query body model) is constructed with a `ModelExtension` or
  `SlayerModel` instance as `source_model`
- **THEN** the same instance is held, and copying the query (without re-validation)
  with a replacement `source_model` that is a string or a typed instance keeps it one
  of the three typed forms, with `source_model_name` following the replacement

#### Scenario: Unknown extension key is rejected

- **WHEN** a query is constructed with `source_model` =
  `{"source_name": "orders", "filters": ["subtotal > tax_paid * 5"]}`
- **THEN** construction fails with a validation error naming `filters` as an unexpected
  key, and nothing is silently dropped

#### Scenario: Object with source_name plus model keys is an extension, never a model

- **WHEN** a query is constructed with `source_model` =
  `{"source_name": "orders", "name": "x", "sql_table": "t", "data_source": "ds"}`
- **THEN** construction fails with a validation error naming `name`, `sql_table`, and
  `data_source` as unexpected extension keys; the object is never accepted as an inline
  model

#### Scenario: Non-spec value is rejected

- **WHEN** a query is constructed with `source_model` = `42` or `[1, 2]`
- **THEN** construction fails with a validation error

#### Scenario: Legacy inline shapes still migrate before validation

- **WHEN** a stored version-1 query carries an extension object spelled with `dimensions`
  (the pre-v2 key), directly or inside a stored query-backed model's stages
- **THEN** the migration rewrites it to `columns` and the query validates into a typed
  extension

#### Scenario: Typed spec round-trips through serialization

- **WHEN** a query holding a typed extension or inline model is dumped (python or JSON
  mode) and validated again
- **THEN** the result equals the original; dumping with none-valued fields excluded adds
  no null-valued keys, and an extension carrying only non-defaulted values dumps back to
  exactly the object the author supplied (an inline model dumps with its own version and
  list defaults, as when persisted)

#### Scenario: Schema advertises the three forms

- **WHEN** the query's JSON schema is generated
- **THEN** `source_model` is described as one of a string, a `ModelExtension` reference, or
  a `SlayerModel` reference, or null — and the MCP query tool's input schema carries the
  same definitions

#### Scenario: REST body rejects a malformed spec at validation

- **WHEN** `POST /query` carries a `source_model` that is not one of the three forms (for
  example an extension with an unknown key)
- **THEN** the request is rejected with HTTP 422 by body validation, before the query
  reaches the engine; well-formed extension and inline-model bodies still execute
