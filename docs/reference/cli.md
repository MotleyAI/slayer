# CLI

SLayer provides a command-line interface for server management, querying, and dataset operations.

## Storage

All commands accept a `--storage` flag to specify where datasets and datasources are stored. When omitted, SLayer uses a platform-appropriate default (`~/.local/share/slayer` on Linux, `~/Library/Application Support/slayer` on macOS, `%LOCALAPPDATA%\slayer` on Windows). See [Storage](../configuration/storage.md) for full details on backends, resolution, and overrides.

The `SLAYER_NOW` environment variable pins the "now" that relative time points resolve against (see [Time](../concepts/time.md#relative-tokens)); every command validates it first and exits non-zero on an invalid value.

## Errors, `--debug` and `--version`

A failing command prints one `Error: …` line on stderr and exits 1; add `--debug` (before or after the command) to see the full traceback, and run `slayer --version` to print the installed version.

The server commands (`serve`, `mcp`, `pg-serve`, `flight-serve`) stop gracefully and exit 143 on SIGTERM and 130 on SIGINT; a second signal forces the stop.

## Commands

### `slayer serve`

Start the HTTP server (REST API + MCP SSE endpoint at `/mcp/sse`).

```bash
slayer serve
slayer serve --host 0.0.0.0 --port 8080
slayer serve --storage slayer.db
slayer serve --demo                  # auto-ingest the bundled Jaffle Shop demo first
slayer serve --ingest-on-startup     # run idempotent ingest over every configured datasource first
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `5143` | Port number |
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
| `--demo` | off | Generate and ingest the bundled Jaffle Shop demo before starting (idempotent). |
| `--ingest-on-startup` | off | Walk every configured datasource and run idempotent auto-ingestion before the port opens. Per-datasource errors are logged to stderr and never abort startup. Also enabled by `SLAYER_INGEST_ON_STARTUP=1`. |
| `--always-load-query` | off | Mark the MCP `query` tool with Claude Code's `anthropic/alwaysLoad` flag so it is never deferred behind tool search. Also enabled by `SLAYER_MCP_ALWAYS_LOAD_QUERY=1`. |

### `slayer mcp`

Run SLayer as an MCP server using stdio transport. This command is **not meant to be run manually** — it is spawned by an AI agent (Claude Code, Cursor, etc.) as a subprocess. To set it up, register the command with your agent:

```bash
# Register with Claude Code (the agent will spawn the process)
claude mcp add slayer -- slayer mcp --ingest-on-startup --storage ./slayer_data

# If slayer is in a virtualenv, use the full executable path:
#   claude mcp add slayer -- $(poetry env info -p)/bin/slayer mcp --ingest-on-startup --storage /abs/path/to/slayer_data
```

For MCP over HTTP (SSE), use `slayer serve` instead — it exposes MCP at `/mcp/sse` alongside the REST API.

| Flag | Default | Description |
|------|---------|-------------|
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
| `--demo` | off | Generate and ingest the bundled Jaffle Shop demo before starting (idempotent). |
| `--ingest-on-startup` | off | Walk every configured datasource and run idempotent auto-ingestion before stdio JSON-RPC starts. Per-datasource errors are logged to stderr and never abort startup. Also enabled by `SLAYER_INGEST_ON_STARTUP=1`. |
| `--always-load-query` | off | Mark the MCP `query` tool with Claude Code's `anthropic/alwaysLoad` flag so it is never deferred behind tool search. Also enabled by `SLAYER_MCP_ALWAYS_LOAD_QUERY=1`. |

### `slayer pg-serve`

Start the Postgres wire-protocol server for BI tools (see [SQL API](../interfaces/pg-facade.md)); the connection's database name selects the SLayer datasource.

```bash
slayer pg-serve --demo                                   # loopback, no token
slayer pg-serve --host 0.0.0.0 --token "$SLAYER_PG_TOKEN"
psql "host=127.0.0.1 port=5145 dbname=jaffle_shop"
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` (`127.0.0.1` with `--demo` and no token) | Bind address |
| `--port` | `5145` | Port number |
| `--token` | `$SLAYER_PG_TOKEN` | Password token; required when binding a non-loopback address |
| `--tls-cert` / `--tls-key` | | TLS certificate and private key (PEM) |
| `--demo` | off | Generate and ingest the bundled Jaffle Shop demo before starting (idempotent). |
| `--storage` | [platform default](../configuration/storage.md) | Storage path |

### `slayer flight-serve`

Start the Arrow Flight SQL server, wire-compatible with the dbt Semantic Layer JDBC driver (see [Flight SQL](../interfaces/flight-sql.md)); needs the `flight` extra.

```bash
slayer flight-serve --demo                               # loopback, no token
slayer flight-serve --host 0.0.0.0 --token "$SLAYER_FLIGHT_TOKEN"
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` (`127.0.0.1` with `--demo` and no token) | Bind address |
| `--port` | `5144` | Port number |
| `--token` | `$SLAYER_FLIGHT_TOKEN` | Bearer token; required when binding a non-loopback address |
| `--tls-cert` / `--tls-key` | | TLS certificate and private key (PEM) |
| `--demo` | off | Generate and ingest the bundled Jaffle Shop demo before starting (idempotent). |
| `--storage` | [platform default](../configuration/storage.md) | Storage path |

### `slayer query`

Execute a query from the terminal.

```bash
# Inline JSON
slayer query '{"population": "orders", "metrics": ["count(orders.*)"], "dimensions": ["orders.status"]}'

# From a file
slayer query @query.json

# JSON output
slayer query '{"population": "orders", "metrics": ["count(orders.*)"]}' --format json

# Preview SQL without executing
slayer query '{"population": "orders", "metrics": ["count(orders.*)"]}' --dry-run

# Show execution plan
slayer query @query.json --explain

# Run a saved query by name, refined
slayer query monthly_revenue --refine '{"dimensions": ["regions.name"]}'

# Pass runtime variables (always overrides query.variables / dataset.query_variables)
slayer query monthly_revenue --variables region=US --variables threshold=100
slayer query @query.json --variables-json '{"region": "US"}'
```

The positional argument is a JSON query if it starts with `{` or `[`, a file path if it starts with `@`, and otherwise the name of a query-backed dataset whose stored backing query runs.

| Flag | Default | Description |
|------|---------|-------------|
| `--storage` | [platform default](../configuration/storage.md) | Storage path (directory for YAML, `.db` file for SQLite) |
| `--format` | `table` | Output format: `table` (numbers rendered through each field's or metric's `format`, as in MCP output) or `json` (raw values) |
| `--dry-run` | | Generate SQL without executing |
| `--explain` | | Run the database's EXPLAIN on the query |
| `--conformance` | `slayer` | `ossie-l3` runs the strict [Ossie Layer 3 binding](../interfaces/ossie-layer-3.md); a refusal prints `OssieQueryError: <code>: …` |
| `--refine` | | Dataset-name form only: JSON (or `@file`) clauses merged into the saved query's final stage |
| `--variables KEY=VALUE` | | Runtime variable, repeatable. Overrides `query.variables` and `dataset.query_variables`. |
| `--variables-json '{...}'` | | Runtime variables from a JSON object. Mutually exclusive with `--variables`. |

### `slayer ingest`

Auto-generate datasets from a datasource.

```bash
slayer ingest --datasource my_postgres
slayer ingest --datasource my_postgres --schema public
slayer ingest --datasource my_postgres --include orders,customers
slayer ingest --datasource my_postgres --exclude migrations,django_session
slayer ingest --datasource my_postgres --no-views
slayer ingest --datasource my_postgres --surface-internals
```

| Flag | Required | Description |
|------|----------|-------------|
| `--datasource` | Yes | Datasource name |
| `--schema` | No | Database schema to inspect |
| `--include` | No | Comma-separated tables to include |
| `--exclude` | No | Comma-separated tables to exclude |
| `--no-views` | No | Skip views and materialized views (ingested by default) |
| `--surface-internals` | No | Ingest recognised ELT/migration internals visible instead of hidden |
| `--storage` | No | Storage path |

#### Views

Views and materialized views are ingested alongside tables by default — dbt
materializes staging models as views, so skipping them would hide most of a
typical dbt project. Pass `--no-views` to restrict ingestion to base tables.

A view-backed dataset records `source_kind: view` (or `materialized_view`) and is
labelled `[view]` in the ingest output. Views expose no primary key and no
foreign keys, so such datasets carry no primary-key field and get no
auto-generated relationships; add relationships by hand if you need them.

`--no-views` affects ingestion only. `slayer validate-datasets` always resolves
datasets against tables *and* views, so a dataset pointing at a view is never
mistaken for one whose table was dropped.

#### Unmodellable objects

`__` is legal in a dataset name, so an object whose name contains it (dlt writes
nested-JSON child tables like `reports__patient__drug`) is modelled under its
**faithful** name — `reports__patient__drug`, with `sql_table` matching. A
re-ingest still matches a pre-existing stored dataset that was sanitized under the
old rules (`reports_patient_drug`) to the same live object, so it is updated in
place rather than duplicated (stored name wins).

Objects that cannot be modelled at all — a name containing `.`, `:`, `/` or `\`,
one using the reserved `__slayer_` prefix, a cross-schema collision on the same
raw name, or one that fails introspection — are listed under `Skipped` and the
rest of the ingest proceeds. Use `--exclude <name>` to accept a skip permanently.

#### Recognised internals

ELT and migration tooling writes bookkeeping tables into the same schema as
your data. These are modelled with `hidden: true` and listed under `Hidden`:

```text
Hidden (3) — recognised ELT/migration internals (excluded from datasets_summary; still queryable by name):
  - _dlt_loads: dlt
  - _dlt_pipeline_state: dlt
  - _dlt_version: dlt
  --surface-internals ingests NEW internals visible; use edit_dataset("<dataset>", data_source="my_postgres", hidden=False) to unhide an existing one.
```

The hint names the datasource because these datasets collide across datasources
by construction — every dlt-loaded database has its own `_dlt_loads`, so a bare
`edit_dataset("_dlt_loads", hidden=false)` resolves by [datasource
priority](../concepts/datasets.md) and can un-hide the wrong one.

When a dataset's name differs from the live object it points at, the listing shows
the dataset name too, since that is what `edit_dataset` takes: `- analytics._dlt_loads
(dataset: _dlt_loads): dlt`.

Hidden, not skipped. The dataset still exists, is still queryable by name, and is
still a valid relationship target — it is only absent from `datasets list`, MCP
`datasets_summary`, the REST dataset listing, semantic search, and the BI catalogs.
That keeps a deliberate use working (`_dlt_loads` answers "when did this last
load?") while keeping the agent-facing dataset list free of junk that costs tokens
in every session and invites an agent to aggregate or join it by mistake.

Matching is case-insensitive and applies to the **live object name**
(`sql_table`), not the dataset name, so an internal is still recognised when its
dataset name differs from the table it points at (e.g. an old-world sanitized name
kept for re-ingest matching, or a schema-qualified `sql_table`).

| Rule | Matches | Tool |
|------|---------|------|
| prefix `_dlt_` | `_dlt_loads`, `_dlt_pipeline_state`, `_dlt_version`, … | dlt |
| prefix `_airbyte_` | `_airbyte_destination_state`, `_airbyte_raw_*` | Airbyte |
| exact | `_fivetran_audit`, `_fivetran_audit_warning` | Fivetran |
| exact | `flyway_schema_history`, `schema_version` | Flyway |
| exact | `databasechangelog`, `databasechangeloglock` | Liquibase |
| exact | `alembic_version` | Alembic |
| exact | `django_migrations` | Django |
| exact | `schema_migrations`, `ar_internal_metadata` | Rails |
| exact | `sequelizemeta` | Sequelize |
| exact | `pgmigrations` | node-pg-migrate |
| exact | `__efmigrationshistory`, `__migrationhistory` | Entity Framework |
| exact | `knex_migrations`, `knex_migrations_lock` | Knex |

Prefix rules are limited to namespaces the vendor reserves by contract, in every
warehouse it loads into. Fivetran and Singer get no prefix rule because their
real surface is *columns* on your tables (`_fivetran_synced`, `_sdc_batched_at`,
…), not tables. There is no `sqlite_` rule either: SQLite's own internals are
filtered out by the inspector before SLayer sees them, so such a rule could only
ever fire on a *non*-SQLite database, where `sqlite_backup` is an ordinary
table. PostGIS (`spatial_ref_sys`, `geometry_columns`, …) and
`pg_stat_statements` are deliberately not matched.

Hiding happens **when the dataset is created**. A re-ingest never re-hides a dataset
you un-hid, and never retro-hides one that already existed — so `edit_dataset(name,
hidden=false)` sticks. For the same reason `--surface-internals` governs datasets
the run creates; it cannot surface one an earlier run already hid.

`--include` and `--exclude` choose *which* objects are scanned, not whether they
are visible: `--include _dlt_loads` still ingests it hidden. Use `--exclude` to
leave an internal out of the store entirely.

#### Exit codes

| Code | Meaning |
|------|---------|
| `0` | Ingest completed; nothing skipped and no errors |
| `1` | One or more objects were skipped, a dataset failed to persist, or the schema contained no tables or views at all |

An empty result prints the available schemas so a mistyped `--schema` is
obvious rather than silent.


### `slayer validate-datasets`

Diff persisted datasets against the live database schemas (read-only) and, optionally, profile each relationship's arity from the data. See [Schema Drift](../concepts/schema-drift.md) and [Relationship cardinality](../concepts/datasets.md#relationship-cardinality).

```bash
slayer validate-datasets                                   # every datasource
slayer validate-datasets --datasource jaffle_shop
slayer validate-datasets --dataset orders --format json
slayer validate-datasets --cardinality                     # + profile relationship arity
slayer validate-datasets --cardinality --persist-cardinality
slayer validate-datasets --datasource jaffle_shop --force-clean --yes
```

| Flag | Default | Description |
|------|---------|-------------|
| `--datasource X` | all | Limit to one datasource. Unknown names fail fast. |
| `--dataset D` | all | Limit the whole report — and `--force-clean` — to one dataset. Resolves across every datasource that has a dataset of that name. |
| `--cardinality` | off | Also profile each relationship's arity from the data. Full-scans both sides of every relationship, so it is opt-in. |
| `--persist-cardinality` | off | Write the detected `cardinality` back onto each matching relationship (identified by target dataset + key pairs). Implies `--cardinality`. |
| `--format` | `text` | `text`, or `json` for one `{"drift": [...], "join_safety": [...], "cardinality": {...}}` document. Cannot be combined with `--force-clean`. |
| `--force-clean` | off | After printing the diff, prompt to apply each delete and write each [untyped](../concepts/schema-drift.md#untyped-findings) semantic-model field's live type into its document. Destructive; opt-in only. |
| `-y` / `--yes` | off | With `--force-clean`, skip the confirmation prompt. |

Without a cardinality flag the output is the drift diff, plus a `Join safety` section whenever unproven-relationship findings exist. With one, the report gains labelled `Schema drift` and `Join cardinality` sections (with `Join safety` between them), and any `--force-clean` apply happens *before* profiling, so it reads the repaired datasets.

Exit code is `0` for any report about the data, including `contradicts_hard` — the command did its job and the answer is unwelcome. It is `1` when the command could not do its job: an unknown datasource or dataset, a datasource that failed validation (an unscoped run validates each one explicitly rather than silently skipping failures), a profiling failure, any `scan_failed` finding, or residual drift after `--force-clean`. Diagnostics go to stderr, so stdout stays parseable under `--format json`.

#### Cardinality verdicts

| Verdict | Meaning |
|---------|---------|
| `fills_none` | No cardinality was stored; the detected value fills the gap. |
| `confirms` | Detected value matches what was stored. |
| `refines` | Differs from the stored value, but the data does not disprove it — "no duplicates observed" only *suggests* uniqueness. |
| `contradicts_hard` | The data **disproves** the stored value: a side it claimed unique has duplicates. |
| `skipped_unsupported` | Not profilable — a non-`sql_table` dataset (sql-mode / query-backed) or an expression-valued join key. |
| `no_evidence` | Profiled fine, but one side had no non-null key rows. An empty scan says nothing about arity, so nothing is detected or written. Worth re-running once data lands — unlike `skipped_unsupported`, which never becomes profilable. |
| `scan_failed` | The scan itself raised; the message is in `note`. Contained per relationship, so one unreadable table costs one finding rather than the whole report — but the command still exits 1, since that relationship was not profiled. |

Fields declared `unique` (or `primary_key`) that the data shows have duplicates are reported under `unique_contradictions`; detection never mutates `DatasetField.unique`.

Detection is a strong guess, not a guarantee: a duplicate disproves uniqueness with certainty, but its absence only suggests uniqueness.


### `slayer recommend-population`

Recommend the query root (population) for a set of `dataset.field` / `dataset.metric` items, with each item's reference path from it. See [Choosing a root dataset](../concepts/queries.md#choosing-a-root-dataset).

```bash
slayer recommend-population customers.name products.category
slayer recommend-population "sum(orders.revenue)" regions.name --format json
slayer recommend-population customers.name products.category --root-hint orders
```

| Flag | Default | Description |
|------|---------|-------------|
| `items` | | One or more `dataset.field` / `dataset.metric` items (aggregation suffixes allowed) |
| `--data-source X` | priority list | Datasource that scopes name resolution |
| `--root-hint M` | | Intended root (bare name or `<data_source>.<dataset>`); honoured when it reaches every item, else a warning explains the auto-pick |
| `--format` | `text` | `text` or `json` |
| `--storage` | [platform default](../configuration/storage.md) | Storage path |

### `slayer import-dbt`

Import dbt Semantic Layer definitions into SLayer.

```bash
slayer import-dbt ./my_dbt_project --datasource my_postgres
slayer import-dbt ./my_dbt_project --datasource my_postgres --include-hidden-datasets
```

| Flag | Required | Description |
|------|----------|-------------|
| `dbt_project_path` | Yes | Path to the dbt project root (or a models directory) |
| `--datasource` | Yes | SLayer datasource name for the imported datasets |
| `--include-hidden-datasets` | No | Also import regular dbt models (those not wrapped by a `semantic_model`) as hidden SLayer datasets via SQL introspection. Requires the `dbt` extra (`pip install 'motley-slayer[dbt]'`). See [dbt Import](../dbt/dbt_import.md#regular-dbt-models-hidden-import). |

### `slayer import-cube`

Import Cube (Cube.js / Cube.dev) YAML and JavaScript data models into SLayer. See [Importing Cube definitions](../cube/cube_import.md).

```bash
slayer import-cube ./cube_project --datasource my_postgres
slayer import-cube ./cube_project/model --datasource my_postgres --report ./report.json
```

| Flag | Required | Description |
|------|----------|-------------|
| `cube_project_path` | Yes | Path to the Cube project (or its model directory) |
| `--datasource` | Yes | SLayer datasource name to file the imported datasets under |
| `--report` | No | Path for the JSON conversion report (default: `<storage>/cube_import_report.json`) |
| `--include-hidden` | No | Also print hidden (`public: false`) datasets in the summary |
| `--ignore-required-meta` | No | Emit every `FILTER_PARAMS` pushdown as an optional filter, ignoring a member's `meta.required` marker |
| `--storage` | No | Storage path |

### `slayer import-osi`

Store Ossie (OSI) semantic-model documents natively, as written. See [Importing Ossie documents](../osi/osi_import.md).

```bash
slayer import-osi ./osi_configs --datasource my_postgres
slayer import-osi ./model.yaml --datasource my_postgres
```

| Flag | Required | Description |
|------|----------|-------------|
| `osi_path` | Yes | Path to an Ossie document or a directory of them (`.yaml`/`.yml`/`.json`) |
| `--datasource` | Yes | Configured SLayer datasource the documents belong to; it must be reachable only to upgrade a legacy `semantic_model` wrapper document |
| `--storage` | No | Storage path |

Imports semantic-model, [ontology and mapping](../concepts/ontologies.md) documents. Prints each stored document and the import report. Exits 1 when any file is refused, after storing the others.

### `slayer ossie-roundtrip`

Import one Ossie document into scratch storage and export it again; your storage is never opened. Exits non-zero with the import's reasons when the document is refused.

```bash
slayer ossie-roundtrip model.ossie.yaml returned.ossie.yaml
```

| Argument | Required | Description |
|------|----------|-------------|
| `input` | Yes | The Ossie document to import |
| `output` | Yes | Where to write the export; JSON when it ends in `.json`, YAML otherwise |

### `slayer export-osi`

Export a datasource as one Ossie (OSI) document: its semantic models merged with its standalone datasets, or a single semantic model's stored text unchanged. See [Exporting to OSI](../osi/osi_export.md).

```bash
slayer export-osi --datasource my_postgres --out shop.osi.yaml
slayer export-osi --datasource my_postgres --datasets orders customers --out shop.osi.json --no-extensions
```

| Flag | Required | Description |
|------|----------|-------------|
| `--datasource` | Yes, unless `--ontology` | SLayer datasource whose datasets (or, with `--mapping`, whose mapping) to export |
| `--ontology` | No | Export this stored ontology's text instead; takes no `--datasource` or `--datasets` |
| `--mapping` | No | Export this stored mapping of `--datasource` instead |
| `--out` | Yes | Output file; JSON when it ends in `.json`, YAML otherwise |
| `--datasets` | No | Export only these datasets (an unknown name fails with no file written) |
| `--no-extensions` | No | Omit the `SLAYER` non-portable extensions; SLayer-only content is reported instead of exported |
| `--storage` | No | Storage path |

### `slayer datasets`

Manage datasets.

```bash
slayer datasets list
slayer datasets show orders
slayer datasets create dataset.yaml
slayer datasets delete orders
```

A dataset of a semantic model is edited and deleted inside its document.

### `slayer semantic-models`

List, show and delete [semantic models](../concepts/semantic-models.md).

```bash
slayer semantic-models list --datasource shop
slayer semantic-models show sales --datasource shop
slayer semantic-models delete sales --datasource shop --yes
```

| Subcommand | Description |
|------|-------------|
| `list [--datasource X]` | One line per document: `<datasource>.<name>: <datasets>` |
| `show <name> [--datasource X]` | Print the document's stored text |
| `delete <name> [--datasource X] [--yes]` | Delete the document and its datasets, after a confirmation prompt that `--yes` skips |

`--datasource` is needed for `show` and `delete` when the name exists in several datasources. An unknown document fails with an `Error:` line and exit code 1.

### `slayer ontologies` / `slayer ontology-mappings`

Manage [ontologies and mappings](../concepts/ontologies.md). Ontologies are stored once per store; mappings per datasource, so every `ontology-mappings` subcommand but `list` needs `--datasource`.

```bash
slayer ontologies create flights.ontology.yaml
slayer ontologies show Flights
slayer ontology-mappings create flights.mapping.yaml --datasource flights
slayer ontology-mappings list --datasource flights
```

| Subcommand | Description |
|------|-------------|
| `list` | One name per line; a mapping that no longer resolves is listed with its problems |
| `show <name>` | Print the stored text |
| `create <file>` | Store the document under its own `name` |
| `replace <name> <file>` | Replace the stored document |
| `delete <name> [--yes]` | Delete it, after a confirmation prompt that `--yes` skips |

A refused document fails with one `Error:` line listing every problem and exit code 1.

### `slayer datasources`

Manage datasources.

```bash
slayer datasources list
slayer datasources show my_postgres   # credentials masked
slayer datasources test my_postgres
slayer datasources delete my_postgres
```

#### `slayer datasources refresh-samples`

Re-profile and persist `DatasetField.sampled` / `sampled_values` / `distinct_count` for table-backed datasets. Per-field failures are reported but do not abort; a `--dataset` that does not resolve exits 1.

```bash
slayer datasources refresh-samples
slayer datasources refresh-samples --data-source jaffle_shop
slayer datasources refresh-samples --data-source jaffle_shop --dataset orders --dataset customers
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-source X` | all | Limit the refresh to one datasource. |
| `--dataset D` | all | Repeatable; limit to specific datasets. |
| `--format` | `text` | `text`, or `json` for the `{"errors": [...], "unresolved_datasets": [...]}` envelope. |

#### `slayer datasources create`

Create a datasource from a connection URL. The name is derived from the database portion of the URL (or the filename stem for SQLite/DuckDB) unless `--name` is passed. Pass `--ingest` to create and ingest in a single step.

```bash
slayer datasources create postgresql://user:${DB_PW}@localhost/analytics
slayer datasources create postgresql://localhost/analytics --ingest
slayer datasources create sqlite:///path/to/app.db --name analytics --ingest
slayer datasources create demo --ingest        # bundled Jaffle Shop demo
```

| Flag | Required | Description |
|------|----------|-------------|
| `connection_string` | Yes | Database URL (e.g. `postgresql://…`, `mysql+pymysql://…`, `sqlite:///path/to/file.db`, `duckdb:///…`, `clickhouse+http://…`). `${ENV_VAR}` references are resolved at use time. Pass the literal `demo` to spin up the bundled Jaffle Shop demo DuckDB. |
| `--name` | No | Override the auto-derived name (default for the demo: `jaffle_shop`) |
| `--description` | No | Human-readable description |
| `--ingest` | No | Run `slayer ingest` immediately after creating the datasource (the demo runs its own enriched ingest instead) |
| `--schema` | No | (with `--ingest`) Schema to ingest from |
| `--include` | No | (with `--ingest`) Comma-separated tables to include |
| `--exclude` | No | (with `--ingest`) Comma-separated tables to exclude |
| `--no-views` | No | (with `--ingest`) Skip views and materialized views (ingested by default) |
| `--surface-internals` | No | (with `--ingest`) Ingest recognised ELT/migration internals visible instead of hidden |
| `--years` | No | (demo only) Years of synthetic data to generate (default: 2) |
| `-y`, `--yes` | No | Overwrite an existing datasource without prompting |
| `--storage` | No | Storage path |

The demo path generates a DuckDB at `<storage>/demo/jaffle_shop.duckdb` and is idempotent — re-running reuses the existing file. Ingested demo datasets are enriched with curated field labels/descriptions, currency and percent formats, saved metrics (e.g. `orders.total_revenue`, `orders.avg_order_value`, `orders.effective_tax_rate`), and a `weighted_avg` custom-aggregation example on `orders`. The enrichment is additive-only: labels/descriptions are filled only where unset and existing metrics are never overwritten, so user edits survive re-runs. `duckdb` and `jafgen` are core dependencies of `motley-slayer`, so the demo works after a single `pip install motley-slayer` with no extras needed.

If a datasource with the same name already exists, SLayer prompts for confirmation. Use `--yes` for non-interactive use.

For a database URL (not `demo`), `--ingest` runs the same idempotent, additive ingest as `slayer ingest` and prints the same output. Re-creating a datasource replaces its config but merges the fresh scan into its stored datasets, never overwriting them. To reset a dataset to a fresh scan, run `slayer datasets delete` first. Once the datasource is created, the command exits 0 whatever the ingest reports. An ingest that raises exits 1.

With `--ingest`, the `Hidden` and `Skipped` sections described under [`slayer ingest`](#slayer-ingest) are printed after the datasets are written. Neither affects the exit code here — creating the datasource is this command's job, and it succeeded.

### `slayer inspect`

Point-lookup of an entity by reference and kind — no ranking, no bundled
memories (use `slayer search` for an entity *in context*). Pass **two or more**
references to inspect several entities of the **same kind** in one call:
the output is one `## <canonical>` block per reference, in input
order (a JSON array under `--format json`), with per-reference error isolation.
**Omit the reference entirely** to list the whole **collection** at
`--type` — supported for `dataset` and `datasource` only. `--type dataset` lists all
datasets grouped by datasource (a terse one line per dataset by default; `--no-compact`
gives the full per-dataset tables); `--type datasource` lists all datasources.

```bash
slayer inspect jaffle_shop.orders --type dataset
slayer inspect jaffle_shop.orders.order_total --type field --no-compact
slayer inspect jaffle_shop.orders.stores.regions.name --type field --no-compact  # relationship path → owning dataset
slayer inspect memory:42 --type memory --no-compact
slayer inspect jaffle_shop.orders --type dataset --format json
slayer inspect jaffle_shop.orders.order_total jaffle_shop.orders.id --type field --no-compact  # batch
slayer inspect --type dataset       # collection: all datasets, grouped by datasource
slayer inspect --type datasource    # collection: all datasources
```

| Flag | Required | Description |
|------|----------|-------------|
| `reference` | No | Zero or more entity references: canonical id, bare name, relationship path (resolved to the owning dataset), or `memory:<id>`. Two or more → a same-kind batch. **Omit entirely** to list the whole collection at `--type` (`dataset` or `datasource` only). |
| `--type` | Yes | Entity kind: `datasource`, `dataset`, `field`, `metric`, `aggregation`, or `memory`. Disambiguates same-named entities and asserts the kind. |
| `--no-compact` | No | Return the full render. The compact default is description-only for field/metric/aggregation/datasource/memory (`compact` never shortens a `help.*` memory; `descriptions_max_chars` still does), and a cheap **schema skeleton** (field/metric/aggregation names + relationship targets, zero DB calls) for `--type dataset`; `--no-compact` on a datasource renders a per-dataset skeleton for each visible dataset. |
| `--format` | No | `markdown` (default) or `json`. |
| `--num-rows` | No | (dataset only) Sample-data rows. Ignored with a warning for other kinds. |
| `--show-sql` | No | (dataset only) Include generated SQL. No-op for field/metric/aggregation; warned for datasource/memory. |
| `--section` | No | (dataset only, repeatable) Restrict to a section subset. Ignored with a warning for other kinds. |
| `--descriptions-max-chars` | No | Truncate description fields. Applies to every kind. |
| `--storage` | No | Storage path. |

### `slayer search`

Run semantic search over memories and canonical entities (datasources, datasets, fields, named metrics, custom aggregations). Three retrieval channels run in parallel — BM25 over memory entity tags, Tantivy full-text over memories ∪ entities, and (with the `advanced_search` extra plus a provider API key) dense embeddings — and are RRF-fused into a single ranked list. See [Search](../concepts/search.md).

```bash
# Entity-driven
slayer search --entity jaffle_shop.orders.order_total

# Question-driven (the question may also be given as --question)
slayer search "What stores are in jaffle_shop?"

# Query-driven (auto-extracts the entities the query references)
slayer search --query @draft_query.json

# Inline query JSON
slayer search --query '{"population": "orders", "metrics": ["sum(orders.order_total)"]}'

# Narrow to one datasource
slayer search --question "lifetime spend" --datasource jaffle_shop

# Graph-narrow with cypher_filter (naive form, always available)
slayer search --question "Brooklyn POS" --cypher-filter 'MATCH (n:Memory) RETURN n.id AS id'

# Graph-narrow with cypher_filter (full openCypher; requires the advanced_search extra)
slayer search --question "store rev" --cypher-filter \
  "MATCH (d:Datasource {name: 'jaffle_shop'})-[:CONTAINS]->(m:Dataset)-[:CONTAINS]->(c:DatasetField) RETURN c.id AS id"

# JSON output for piping
slayer search --question "lifetime spend" --format json
```

| Flag | Default | Description |
|------|---------|-------------|
| `--entity ENT` (repeatable) | | Canonical entity string (`<ds>`, `<ds>.<dataset>`, `<ds>.<dataset>.<leaf>`, `memory:<id>`). Pass multiple times to combine. Drives the BM25 channel. |
| `--query JSON_OR_@FILE` | | Inline SLayer query (or `@path.json`). Entities are auto-extracted from `population`, dimensions, metrics, time dims, and `filter`. |
| `QUESTION` / `--question TEXT` | | Free-text question, positional or as `--question` (not both). Drives Tantivy + embeddings. |
| `--datasource DS` | | Pre-narrow every channel to ids rooted at the named datasource. Unknown name raises. |
| `--cypher-filter CYPHER` | | Pre-narrow all three channels via a graph query. Full openCypher with `advanced_search` (LadybugDB property graph with `Memory` / `Datasource` / `Dataset` / `DatasetField` / `Metric` / `Aggregation` nodes and `MENTIONS` / `CONTAINS` / `RELATIONSHIPS` edges). Without the extra, only the naive `MATCH (n:Label1:Label2…) RETURN n.id AS id` form is accepted; anything richer raises with an install hint. |
| `--max-results N` | `10` | Cap applied after RRF fusion and the `cypher_filter` allowlist. |
| `--format` | `text` | `text` (newline-grouped human output) or `json` (full `SearchResponse`). |

In `text` mode each result row prints only `kind`, `id`, `score`, and a one-line preview of the hit's `text`. The full field snapshot — the top-50 `sampled_values`, JSON-encoded inside the hit's `text` — is returned in full only under `--format json`; fields with more than 50 distinct values surface only that top 50, with no exact total. (The `50+ distinct` overflow marker is a property of `DatasetField.sampled` and shows in `slayer inspect` / MCP `inspect` output, not in search results.) Memory hits with `query is not None` are saved example queries. Unresolved input entities surface as warnings rather than errors.

### `slayer memory`

Manage the agent-memory layer. See [Memories](../concepts/memories.md).

```bash
# Save a learning (--entities is a single comma-separated string)
slayer memory save \
  --learning "Refunded and cancelled orders keep their order_total; count only completed orders for net revenue" \
  --entities jaffle_shop.orders.status,jaffle_shop.orders.order_total

# Save with a pinned id and multiple entities
slayer memory save \
  --learning "Brooklyn POS changed late 2024" \
  --entities jaffle_shop.orders.order_total,jaffle_shop.stores.name \
  --id kb.brooklyn-pos

# Save with an inline query (mutually exclusive with --entities)
slayer memory save \
  --learning "Top customers by lifetime spend" \
  --query @top_customers.json \
  --id kb.top-customers

# Forget by id
slayer memory forget kb.brooklyn-pos
```

| Subcommand | Flag | Description |
|------------|------|-------------|
| `save` | `--learning TEXT` (required) | The free-form note. |
| `save` | `--entities ENT,ENT,…` | Comma-separated canonical entity strings. `memory:<id>` is valid for cross-memory refs. Mutually exclusive with `--query`; one of the two is required. |
| `save` | `--query JSON_OR_@FILE` | Inline SLayer query (or `@path.json`). Entities are auto-extracted and the query is persisted on the memory. Mutually exclusive with `--entities`. |
| `save` | `--id ID` | User-pinned canonical memory id. Forbidden charset: `:`, `/`, `?`, `#`, whitespace, ASCII control. Omit to auto-allocate (`max(int-shaped id) + 1`). Duplicate id → unconditional upsert; `created_at` preserved. |
| `forget` | `<id>` (positional) | Memory id. Cascade-strips every `memory:<id>` reference to it from every other memory's `entities` list. |

### `slayer storage`

Storage maintenance. `upgrade` stores every dataset, saved query, memory and datasource that loads in an older format in the current one; loads upgrade them in memory and never write. It prints one line per document (`upgraded`, `current`, `would upgrade`, `not upgraded: <reason>` or `conflict`) and exits 1 when a document is left behind. A document changed in the store while the command runs is a conflict and stays as it is; datasource priority and embeddings are never touched.

```bash
slayer storage upgrade --dry-run
slayer storage upgrade --data-source warehouse --datasource-type warehouse=duckdb
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-source X` | all | Limit to one datasource |
| `--dry-run` | off | Print each document's difference without writing anything |
| `--datasource-type NAME=TYPE` | none | The dialect of a datasource the store holds no config for (repeatable) |
| `--storage` | [platform default](../configuration/storage.md) | Storage path |

`migrate-types` introspects each dataset's datasource and refines `Float` to `Integer` on base fields whose live SQL type is integer (on SQLite, whose stored values probe as integers, and there it also widens an `Integer` field to `Float` or `String` whose values are not); it fails if a datasource is unreachable.

```bash
slayer storage migrate-types --dry-run
slayer storage migrate-types --data-source jaffle_shop
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-source X` | all | Limit to one datasource |
| `--dry-run` | off | Report the planned refinements without writing them |
| `--storage` | [platform default](../configuration/storage.md) | Storage path |

### `slayer usage-reporting`

Show or change [usage reporting](usage-reporting.md).

```bash
slayer usage-reporting status    # on/off, the rule that decided it, the install ID
slayer usage-reporting show      # the report the next send would contain (sends nothing)
slayer usage-reporting disable   # persist off; delete the install ID and unsent data
slayer usage-reporting enable    # persist on
```

### `slayer otel`

Inspect and check the [OpenTelemetry](opentelemetry.md) setup.

```bash
slayer otel status   # on/off, the rule that decided it, where each signal goes (no network call)
slayer otel test     # send one empty OTLP request per signal and one test span; exit 1 if any check fails
```

## Conceptual help

SLayer's conceptual help ships as a predefined set of **help memories** (`memory:help.intro` … `memory:help.queries`): read one with `inspect` (compact never shortens a `help.*` memory), or find the relevant one with `search`.

```bash
slayer inspect memory:help.intro --type memory        # overview + judgment calls
slayer inspect memory:help.datasets --type memory     # authoring datasets, relationships, result keys
slayer search --question "how do relationships resolve"       # surface the relevant topic
```

`memory:help.intro` lists the deep-dive topics (`memory:help.datasets`, `memory:help.workflow`, `memory:help.aggregations`, `memory:help.transforms`, `memory:help.time`, `memory:help.relationships`, `memory:help.queries`); the query syntax itself is on the MCP `query` tool's schema fields.
