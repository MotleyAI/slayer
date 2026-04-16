# SLayer API Gaps

Features available in at least one API but missing in at least one other.

| Functionality | REST | CLI | MCP | Notes |
|---|:---:|:---:|:---:|---|
| Query from file | — | Y | — | CLI supports `@file.json` syntax |
| Show SQL on normal (non-dry-run) queries | — | — | Y | MCP has `show_sql` param; REST/CLI only on dry_run/explain |
| Output format: table (ASCII) | — | Y | — | |
| Output format: markdown | — | — | Y | |
| Output format: csv | — | — | Y | |
| List models in a datasource | Y | Y | Y | MCP uses `models_summary(datasource_name=...)` |
| Create model from query | — | — | Y | Saves a query's generated SQL as a reusable model |
| Update/replace model (full) | Y | — | — | `PUT /models/{name}` |
| Edit model (partial) | — | — | Y | Add/remove fields, update metadata |
| Sample data from model | — | — | Y | `inspect_model` with `num_rows` |
| Create datasource | Y | — | Y | |
| Edit datasource | — | — | Y | Update description only |
| Delete datasource | Y | — | Y | |
| List database tables | — | — | Y | Included in `describe_datasource` (default: `list_tables=true`) |
| Test datasource connection | — | — | Y | Included in `describe_datasource` |
| Auto-ingest on datasource creation | — | — | Y | `auto_ingest` param on `create_datasource` |
| Ingest: include specific tables | Y | — | Y | |
| Ingest: exclude specific tables | Y | — | — | REST-only `exclude_tables` param |
| Ingest: filter by schema | Y | Y | Y | All three support this (included for completeness of ingest row) |
| Health check endpoint | Y | — | — | `GET /health` |
| Start REST server | — | Y | — | `slayer serve` |
| Start MCP server | — | Y | — | `slayer mcp` |
| Conceptual help | — | Y | Y | `help()` MCP tool / `slayer help [TOPIC]` subcommand; identical content from `slayer/help/topics/*.md` |
