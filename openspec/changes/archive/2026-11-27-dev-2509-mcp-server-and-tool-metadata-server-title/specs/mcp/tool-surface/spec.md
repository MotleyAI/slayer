## ADDED Requirements

### Requirement: Every tool carries a title and behaviour annotations
Every tool in `tools/list` SHALL carry a non-empty human-readable `title` and annotations with all four hints
(`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) set explicitly, none left to the protocol
default. The hints SHALL follow these criteria, where *stored definitions* are the datasources, datasets (their
cached sampled values excluded), semantic models, ontologies, ontology mappings, memories and the datasource
priority list, and derived state (cached field samples, embeddings, search indexes) is not part of them:

- `readOnlyHint` is true iff the tool changes no stored definition;
- `destructiveHint` is true iff the tool can remove, replace or re-type existing stored content, a same-name
  replace included; a read-only tool sets it false;
- `idempotentHint` is true iff a repeat call with the same arguments leaves the stored definitions as the first
  call left them, a refusal or "not found" on the repeat included; a read-only tool sets it true;
- `openWorldHint` is true iff the tool can connect to a datasource's database; a call to an embedding provider
  does not count.

The tools, their titles and hints (R = read-only, D = destructive, I = idempotent, O = open-world; `-` = false)
SHALL be exactly:

| Tool | Title | R | D | I | O |
|------|-------|---|---|---|---|
| `query` | Query data | R | - | I | O |
| `inspect` | Inspect entity | R | - | I | O |
| `search` | Search memories and entities | R | - | I | O |
| `describe_datasource` | Describe datasource | R | - | I | O |
| `validate_datasets` | Validate datasets | R | - | I | O |
| `datasets_summary` | Summarize datasets | R | - | I | - |
| `list_datasources` | List datasources | R | - | I | - |
| `get_datasource_priority` | Get datasource priority | R | - | I | - |
| `recommend_population` | Recommend population | R | - | I | - |
| `create_dataset` | Create dataset | - | D | I | O |
| `edit_dataset` | Edit dataset | - | D | I | O |
| `create_datasource` | Create datasource | - | D | I | O |
| `ingest_datasource` | Ingest datasource | - | D | I | O |
| `edit_datasource` | Edit datasource | - | D | I | - |
| `set_datasource_priority` | Set datasource priority | - | D | I | - |
| `delete_dataset` | Delete dataset | - | D | I | - |
| `delete_datasource` | Delete datasource | - | D | I | - |
| `create_semantic_model` | Create semantic model | - | D | I | - |
| `edit_semantic_model` | Edit semantic model | - | D | I | - |
| `delete_semantic_model` | Delete semantic model | - | D | I | - |
| `create_ontology` | Create ontology | - | D | I | - |
| `edit_ontology` | Edit ontology | - | D | I | - |
| `delete_ontology` | Delete ontology | - | D | I | - |
| `create_ontology_mapping` | Create ontology mapping | - | D | I | - |
| `edit_ontology_mapping` | Edit ontology mapping | - | D | I | - |
| `delete_ontology_mapping` | Delete ontology mapping | - | D | I | - |
| `forget_memory` | Forget memory | - | D | I | - |
| `save_memory` | Save memory | - | D | - | - |

A tool registered without a title or without its four hints SHALL fail the server build.

#### Scenario: Titles and hints match the table
- **WHEN** a client lists the tools
- **THEN** the set of tool names equals the table's, and each tool's title and four hints equal its row, none of them null

#### Scenario: A tool without metadata cannot be registered
- **WHEN** the server registers a tool without a title or without its hints
- **THEN** building the server fails

### Requirement: Read-only tools leave stored definitions unchanged
A tool whose `readOnlyHint` is true SHALL leave every stored definition (as defined by the annotation criteria)
unchanged; it MAY refresh derived state such as cached field samples, embeddings and search indexes.

#### Scenario: Read-only calls on seeded storage
- **WHEN** each read-only tool is called with valid arguments against storage seeded with a datasource, table-backed and query-backed datasets, a memory and a datasource priority list
- **THEN** a snapshot of the stored definitions, taken without cached sampled values, is identical before and after the call

### Requirement: Closed-world tools open no database connection
A tool whose `openWorldHint` is false SHALL complete without connecting to any datasource's database.

#### Scenario: Closed-world calls with an unreachable database
- **WHEN** each closed-world tool is called with valid arguments against storage seeded with table-backed and query-backed datasets, and every attempt to obtain a database client fails
- **THEN** no database client is requested and the call does not fail for want of one

### Requirement: Tool output schemas stay the wrapped result string
Every tool SHALL advertise the output schema of a single required string property `result`, and no other output
schema.

#### Scenario: Output schemas
- **WHEN** a client lists the tools
- **THEN** every tool's output schema is an object whose only property is the required string `result`
