# SLayer API Gaps

Features available in at least one API but missing in at least one other.

| Functionality | REST | CLI | MCP | Notes |
|---|:---:|:---:|:---:|---|
| Query from file | — | Y | — | CLI supports `@file.json` syntax |
| Show SQL on normal (non-dry-run) queries | — | — | Y | MCP `query` and `inspect` have `show_sql` param; REST/CLI only on dry_run/explain |
| Output format: table (ASCII) | — | Y | — | |
| Output format: markdown | — | — | Y | `query`, `datasets_summary`, `inspect` all default to markdown |
| Output format: json | — | — | Y | `query`, `datasets_summary`, `inspect` support `format="json"` |
| Output format: csv | — | — | Y | `query` supports `format="csv"` |
| List datasets in a datasource | Y | Y | Y | MCP uses `datasets_summary(datasource_name=...)` |
| Create dataset from query | — | — | Y | Saves a query's generated SQL as a reusable dataset |
| Update/replace dataset (full) | Y | — | — | `PUT /datasets/{name}` |
| Edit dataset (partial) | — | — | Y | Add/remove fields, update metadata |
| Sample data from dataset | — | — | Y | `inspect` (`entity_type="dataset"`) with `num_rows` |
| Create datasource | Y | Y | Y | |
| Edit datasource | — | — | Y | Update description only |
| Delete datasource | Y | Y | Y | |
| List database tables | — | — | Y | Included in `describe_datasource` (default: `list_tables=true`) |
| Test datasource connection | — | Y | Y | Included in `describe_datasource` |
| Auto-ingest on datasource creation | — | — | Y | `auto_ingest` param on `create_datasource` |
| Ingest: include specific tables | Y | — | Y | |
| Ingest: exclude specific tables | Y | Y | — | |
| Ingest: filter by schema | Y | Y | Y | All three support this (included for completeness of ingest row) |
| Health check endpoint | Y | — | — | `GET /health` |
| Start REST server | — | Y | — | `slayer serve` |
| Start MCP server | — | Y | — | `slayer mcp` |
| Conceptual help | Y | Y | Y | Seeded `memory:help.*` memories (from `slayer/memories/help_content/*.md`); read via `inspect(entity_type="memory")` / `slayer inspect memory:help.intro --type memory`, or `search` |