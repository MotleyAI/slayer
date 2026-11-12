## ADDED Requirements

### Requirement: A population is named, and typed at construction

A query's `source_model` SHALL accept exactly one form — a saved model or stage name (a
string) — or be omitted, and SHALL be validated when the query is constructed, before any
engine resolution. An object without `source_name` SHALL be rejected at construction with an
error stating that inline models are not accepted and that `source_model` names a stored
model or stage, with `extensions` for query-local measures and joins. An object carrying
`source_name` is a legacy root extension: it SHALL be converted into `extensions` as the
`queries/extensions` capability specifies. Any other value SHALL be rejected at construction. Validation errors SHALL name the
offending keys of the form the object was classified as. The query's JSON schema SHALL describe `source_model` as a
string or null.

#### Scenario: Extension object validates as a typed extension

- **WHEN** a query is constructed without a version with `source_model` =
  `{"source_name": "orders", "measures": [{"name": "double_amount", "formula": "sum(amount * 2)"}]}`
- **THEN** the query's `source_model` is `orders`, its `extensions` hold one typed extension
  of `orders` whose `measures` are typed objects, and the query executes exactly as before

#### Scenario: Inline model object is rejected

- **WHEN** a query is constructed with `source_model` =
  `{"name": "ad_hoc", "sql_table": "things", "data_source": "ds", "columns": [...]}`, as a
  dict or as a `SlayerModel` instance, directly, via REST or via the MCP query tool
- **THEN** construction fails with the inline-model error, and nothing executes

#### Scenario: Copies keep the name form

- **WHEN** a query is copied (without re-validation) with a replacement `source_model` string
- **THEN** `source_model_name` follows the replacement

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
  `data_source` as unexpected extension keys

#### Scenario: Non-spec value is rejected

- **WHEN** a query is constructed with `source_model` = `42` or `[1, 2]`
- **THEN** construction fails with a validation error

#### Scenario: Legacy inline shapes still migrate before validation

- **WHEN** a stored version-1 query carries an extension object spelled with `dimensions`
  (the pre-v2 key), directly or inside a stored query-backed model's stages
- **THEN** the migrations rewrite it to `columns`, move it into `extensions`, and inline its
  columns as `models/stored-upgrade` › Stored extension columns are inlined specifies, and
  the query validates with a typed extension

#### Scenario: Typed spec round-trips through serialization

- **WHEN** a query holding extensions is dumped (python or JSON mode) and validated again
- **THEN** the result equals the original; dumping with none-valued fields excluded adds
  no null-valued keys, and an extension carrying only non-defaulted values dumps back to
  exactly the object the author supplied

#### Scenario: Schema advertises a name or null

- **WHEN** the query's JSON schema is generated
- **THEN** `source_model` is described as a string or null, with no `SlayerModel`
  reference, `extensions` as a list of `ModelExtension` references — and the MCP query tool's
  input schema carries the same definitions

#### Scenario: REST body rejects a malformed spec at validation

- **WHEN** `POST /query` carries a `source_model` that is neither a name nor a legacy
  extension, or an extension with an unknown key
- **THEN** the request is rejected with HTTP 422 by body validation, before the query
  reaches the engine; an inline-model body is rejected the same way, and well-formed
  extension bodies still execute

## MODIFIED Requirements

### Requirement: Population may be omitted

`SlayerQuery.source_model` SHALL be optional. A query naming it — as a stored model or stage name,
with or without `extensions` — SHALL behave exactly as before, and the named
population MAY be a bridge model owning none of the queried items. Population
inference SHALL run only when the field is absent.

#### Scenario: Explicit population unchanged

- **WHEN** a query names `source_model` explicitly
- **THEN** it executes byte-identically to today, and no inference runs

#### Scenario: Omitted population infers

- **WHEN** a query omits `source_model` and a unique minimal determining model exists
- **THEN** the query executes with that model as its population

## REMOVED Requirements

### Requirement: Population specs are typed at construction
**Reason**: `source_model` no longer accepts an inline model, so the two-form rule and its inline-model scenarios no longer hold; replaced by "A population is named, and typed at construction".
**Migration**: Name a stored model or stage in `source_model`; put query-local measures and joins in `extensions`.
