## MODIFIED Requirements

### Requirement: Population may be omitted

`SlayerQuery.source_model` SHALL be optional. A query naming it — as a model name or an
inline model, with or without `extensions` — SHALL behave exactly as before, and the named
population MAY be a bridge model owning none of the queried items. Population
inference SHALL run only when the field is absent.

#### Scenario: Explicit population unchanged

- **WHEN** a query names `source_model` explicitly
- **THEN** it executes byte-identically to today, and no inference runs

#### Scenario: Omitted population infers

- **WHEN** a query omits `source_model` and a unique minimal determining model exists
- **THEN** the query executes with that model as its population

### Requirement: Population specs are typed at construction

A query's `source_model` SHALL accept exactly two forms — a saved model name (a string) and
an inline model (an object without `source_name`) — and SHALL validate each into its typed
form when the query is constructed, before any engine resolution. An object carrying
`source_name` is a legacy root extension: it SHALL be converted into `extensions` as the
`queries/extensions` capability specifies, and never accepted as an inline model. A value
that is neither form SHALL be rejected at construction. Validation errors SHALL name the
offending keys of the form the object was classified as. The query's JSON schema SHALL list
the two forms for `source_model`.

#### Scenario: Extension object validates as a typed extension

- **WHEN** a query is constructed without a version with `source_model` =
  `{"source_name": "orders", "columns": [{"name": "double_amount", "sql": "amount * 2"}]}`
- **THEN** the query's `source_model` is `orders`, its `extensions` hold one typed extension
  of `orders` whose `columns` are typed objects, and the query executes exactly as before

#### Scenario: Inline model object validates as a typed model

- **WHEN** a query is constructed with `source_model` =
  `{"name": "ad_hoc", "sql_table": "things", "data_source": "ds", "columns": [...]}`
- **THEN** the query's `source_model` is a typed model and the query executes exactly as
  before

#### Scenario: Typed instances pass through

- **WHEN** a query (or a REST query body model) is constructed with a `SlayerModel` instance
  as `source_model`
- **THEN** the same instance is held, and copying the query (without re-validation)
  with a replacement `source_model` that is a string or a typed model keeps it one
  of the two typed forms, with `source_model_name` following the replacement

#### Scenario: Unknown extension key is rejected

- **WHEN** a query is constructed with an extension
  `{"source_name": "orders", "filters": ["subtotal > tax_paid * 5"]}`, in `extensions` or
  in the legacy `source_model` form
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
- **THEN** the migrations rewrite it to `columns` and move it into `extensions`, and the
  query validates with a typed extension

#### Scenario: Typed spec round-trips through serialization

- **WHEN** a query holding an inline model or extensions is dumped (python or JSON
  mode) and validated again
- **THEN** the result equals the original; dumping with none-valued fields excluded adds
  no null-valued keys, and an extension carrying only non-defaulted values dumps back to
  exactly the object the author supplied (an inline model dumps with its own version and
  list defaults, as when persisted)

#### Scenario: Schema advertises the three forms

- **WHEN** the query's JSON schema is generated
- **THEN** `source_model` is described as one of a string or a `SlayerModel` reference, or
  null, `extensions` as a list of `ModelExtension` references — and the MCP query tool's
  input schema carries the same definitions

#### Scenario: REST body rejects a malformed spec at validation

- **WHEN** `POST /query` carries a `source_model` that is neither form nor a legacy
  extension, or an extension with an unknown key
- **THEN** the request is rejected with HTTP 422 by body validation, before the query
  reaches the engine; well-formed extension and inline-model bodies still execute
