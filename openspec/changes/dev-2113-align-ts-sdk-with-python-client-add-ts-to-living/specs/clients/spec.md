# Spec Delta

## Purpose

Defines how SLayer's two SDK clients — the Python `SlayerClient` and the TypeScript `@motley-ai/slayer` — talk to the SLayer REST API, so that one operation behaves the same from either language apart from language idiom.

## ADDED Requirements

### Requirement: Operation parity
Every remote operation offered by either client SHALL be offered by both, under the same name converted to each language's method casing (Python snake_case, TypeScript camelCase). Python-only conveniences (synchronous variants, DataFrame output, local-engine mode) are the only declared exceptions.

#### Scenario: Both clients expose the same operations
- **WHEN** the public methods of the Python client and the TypeScript client are compared against the shared operation list
- **THEN** each client exposes exactly the listed operations plus only its declared language-specific exceptions

#### Scenario: Former TypeScript-only operations exist in Python
- **WHEN** a Python caller uses `health()`, `delete_model(name)` or `with_token(token)`
- **THEN** the client performs `GET /health`, `DELETE /models/{name}`, or returns a client copy that sends the new token, respectively

#### Scenario: Former Python-only operations exist in TypeScript
- **WHEN** a TypeScript caller uses `sql`, `explain`, `createModel`, `createDatasource`, `getDatasourcePriority`, `setDatasourcePriority`, `saveMemory`, `forgetMemory` or `recommendRootModel`
- **THEN** the client sends the same request the Python client sends for the corresponding operation

### Requirement: Identical requests
For the same operation and arguments, both clients SHALL send the same HTTP method, path, query string, JSON body (up to the server's own request normalisation) and auth headers. Path segments taken from caller-supplied names SHALL be percent-encoded.

#### Scenario: Shared wire cases pass in both languages
- **WHEN** every case in the shared parity case file is replayed against each client with a mocked transport
- **THEN** each client produces the case's expected method, path, query string, headers and body

#### Scenario: Model name with reserved characters
- **WHEN** either client gets or deletes a model named `a/b c`
- **THEN** the request path is `/models/a%2Fb%20c`

### Requirement: Naming rule for parameters and data
In TypeScript, required arguments SHALL stay positional and optional or keyword-only arguments SHALL go in one options object with camelCase keys. SLayer data passed in (queries, models, datasources) and returned (responses) SHALL keep SLayer's snake_case wire field names in both clients.

#### Scenario: Options are camelCase, data is wire-shaped
- **WHEN** a TypeScript caller runs `query({ source_model: "orders", measures: ["revenue"] }, { dryRun: true })`
- **THEN** the request body contains `source_model`, `measures` and `dry_run: true`

#### Scenario: Response fields keep wire names
- **WHEN** a query response is returned by the TypeScript client
- **THEN** it exposes `data`, `row_count`, `columns`, `sql`, `attributes`, `population`, `population_inferred` and `warnings` under those names

### Requirement: One query entry point
Each client's `query` SHALL accept a single query object, a list of stages, or a saved query name, with optional `refine`, `variables`, `dry_run`/`dryRun` and `explain`. `sql` SHALL return the generated SQL of a dry run; `explain` SHALL run the database's EXPLAIN.

#### Scenario: Saved query by name with refinement
- **WHEN** a client calls `query("monthly_revenue", refine={"limit": 5})` (TypeScript: `query("monthly_revenue", { refine: { limit: 5 } })`)
- **THEN** the body is `{"name": "monthly_revenue", "refine": {"limit": 5}}`

#### Scenario: Stage list
- **WHEN** a client calls `query([stage1, stage2])`
- **THEN** the body is `{"queries": [stage1, stage2]}`

#### Scenario: Runtime variables override the query's own
- **WHEN** a single query carrying `variables: {"a": 1, "b": 2}` is run with runtime variables `{"b": 3}`
- **THEN** the body's `variables` is `{"a": 1, "b": 3}`

#### Scenario: Flags sent only when set
- **WHEN** a query is run without `dry_run`/`explain`
- **THEN** the body contains neither `dry_run` nor `explain`

#### Scenario: Refinement on a non-name query is rejected
- **WHEN** a client calls `query` with `refine` and a query object or a stage list
- **THEN** the client raises an error without sending a request

#### Scenario: SQL only
- **WHEN** a client calls `sql(query)`
- **THEN** it sends the query with `dry_run: true` and returns the response's `sql` string

### Requirement: Discovery call defaults
`search` SHALL always send `max_results` (default 10) and `compact` (default true). `inspect` SHALL require `entity_type`, always send `reference`, `compact` (true), `format` ("markdown"), `num_rows` (3) and `show_sql` (false), and SHALL return the response's `result` text.

#### Scenario: Search with defaults
- **WHEN** a client calls `search` with only `question: "revenue"`
- **THEN** the body is `{"question": "revenue", "max_results": 10, "compact": true}`

#### Scenario: Inspect returns text
- **WHEN** a client calls `inspect` with `entity_type: "model"` and reference `orders`, and the server answers `{"result": "# orders ..."}`
- **THEN** the call returns the string `# orders ...`

### Requirement: Bearer token and extra headers
Both clients SHALL accept a token that is a string or a function returning one (synchronously or asynchronously), resolved for every request and sent as `Authorization: Bearer <token>`, plus extra headers sent on every request. `with_token`/`withToken` SHALL return a client with the same settings and a different token.

#### Scenario: Static token
- **WHEN** a client created with token `t1` sends any request
- **THEN** the request carries `Authorization: Bearer t1`

#### Scenario: Token function is resolved per request
- **WHEN** a client's token is an async function returning `t1` then `t2`
- **THEN** its first request carries `Bearer t1` and its second `Bearer t2`

#### Scenario: Per-user copy
- **WHEN** `client.with_token("user-jwt")` (TypeScript `withToken`) sends a request
- **THEN** it carries `Bearer user-jwt` and the original client's headers and URL, and the original client still sends its own token

### Requirement: Same defaults
Both clients SHALL default to the URL `http://localhost:5143` and to a 60-second request timeout, which the caller can change or turn off (Python `timeout=None`, TypeScript `timeoutMs: null`).

#### Scenario: Default timeout
- **WHEN** a client is created without a timeout and a request takes longer than 60 seconds
- **THEN** the request fails with the language's timeout error

#### Scenario: Timeout turned off
- **WHEN** a client is created with the timeout turned off
- **THEN** its requests carry no client-side time limit

### Requirement: Local mode refuses remote-only settings
The Python client in local-engine mode (`storage=`) SHALL raise `ValueError` at construction when given `token` or `headers`, and from `with_token`.

#### Scenario: Token with storage
- **WHEN** `SlayerClient(storage=s, token="t")` is constructed
- **THEN** it raises `ValueError`

#### Scenario: Copy with token in local mode
- **WHEN** `with_token("t")` is called on a local-mode client
- **THEN** it raises `ValueError`

### Requirement: Local-mode model deletion
The Python client's `delete_model` SHALL delete through the configured storage in local mode, as other model operations do.

#### Scenario: Delete in local mode
- **WHEN** a local-mode client calls `delete_model("orders")` for a stored model
- **THEN** the model is removed from the storage

### Requirement: Response warnings are preserved
The Python client in remote mode SHALL return the response's `warnings`, as local mode does; the TypeScript client SHALL return them as sent.

#### Scenario: Warnings over HTTP
- **WHEN** the server's query response contains a warning
- **THEN** the Python remote-mode response's `warnings` contains it

### Requirement: Typed responses in the API schema
Every REST route either client calls SHALL declare its response schema in the published OpenAPI document, without changing any response body.

#### Scenario: Schemas published
- **WHEN** the OpenAPI document is generated
- **THEN** the responses of `/health`, `/models`, `/models/{name}`, `/datasources`, `/datasources/priority`, `/memories`, `/memories/{memory_id}`, `/search`, `/inspect` and `/recommend-root-model` each reference a named schema

#### Scenario: Bodies unchanged
- **WHEN** each of those routes is called after the change
- **THEN** its JSON body is identical to the body it returned before the change

### Requirement: Gateway token signing
Both clients SHALL provide a function that signs an HS256 token for the gateway from claims, a secret and a lifetime (default 3600 s), rejecting secrets shorter than 32 bytes; for the same input and clock both SHALL produce the same token.

#### Scenario: Same token in both languages
- **WHEN** both languages sign the same claims with the same secret at the same fixed time
- **THEN** the tokens are byte-identical

#### Scenario: Short secret
- **WHEN** a secret shorter than 32 bytes is given
- **THEN** signing fails with an error

### Requirement: Portable TypeScript entry
The TypeScript package's main entry SHALL have no runtime dependencies and SHALL not use Node-only modules; Node-only helpers SHALL be available only from the `node` entry.

#### Scenario: No Node modules in the main entry
- **WHEN** the TypeScript sources outside the Node-only entry are scanned
- **THEN** none imports a `node:` module

#### Scenario: No runtime dependencies
- **WHEN** the package manifest is read
- **THEN** it declares no runtime `dependencies`
