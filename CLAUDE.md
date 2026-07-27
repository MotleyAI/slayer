# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What is SLayer?

SLayer (Semantic Layer) is a lightweight, open-source (MIT) semantic layer for AI agents,
built by MotleyAI. Instead of writing raw SQL, agents describe what data they want —
measures, dimensions, filters — and SLayer generates and executes the query.

Server ports: REST API 5143, Flight SQL 5144, Postgres facade 5145.

When writing SLayer query examples or answering questions about syntax and capabilities,
read `docs/` first — especially `docs/concepts/queries.md`, `docs/concepts/formulas.md`,
`docs/concepts/models.md`, and `docs/examples/`.

## Layout

`slayer/core` domain models & errors; `slayer/engine` query engine; `slayer/sql` SQL
generation + dialects; `slayer/storage` YAML/SQLite backends + migrations; `slayer/api`
REST; `slayer/mcp` MCP server; `slayer/flight` / `slayer/pg_facade` / `slayer/facade`
BI wire protocols; `slayer/memories` + `slayer/search` agent memory & semantic search;
`slayer/cli.py` CLI.

## Common Commands

```bash
poetry install -E all                                # install with all extras
poetry run pytest                                    # unit tests (excludes integration)
poetry run pytest tests/integration/ -m integration  # all integration tests
poetry run pytest tests/test_sql_generator.py -v     # one file
poetry run slayer serve                              # REST API server
poetry run slayer mcp                                # MCP server
poetry run ruff check slayer/ tests/                 # lint
```

All CLI commands accept `--storage` (YAML dir or `.db` file); defaults to the platform
data dir, override with `$SLAYER_STORAGE`.

## Key Conventions

- Python 3.11+, Pydantic v2 for all models
- NEVER use dataclasses — use Pydantic classes instead
- Use `poetry run` for all Python commands
- Use keyword arguments for functions with more than 1 parameter
- Imports at the top of files
- SQL generation uses sqlglot AST building, not string concatenation
- Async-first: engine and storage methods are async; `execute_sync()` / `run_sync()` bridge for CLI/scripts
- Core principle: adding a measure/field must never change result cardinality or other fields' values
- Two expression layers — Mode A: free SQL in `Column.sql` / model `filters` (`__`-delimited join paths); Mode B: Python-AST DSL in formulas and query fields (dotted paths, colon aggregations, scalar-allowlist functions only). Rules: `docs/concepts/references.md`
- Aggregations are query-time, colon syntax: `revenue:sum`, `*:count` for COUNT(*), `price:percentile(p=0.9)`
- Result column keys are `model.column`: `revenue:sum` → `orders.revenue_sum`, `*:count` → `orders._count`; joined dimensions keep the full path (`orders.customers.regions.name`)
- Dots denote join paths in queries (`customers.regions.name`); `__` denotes path aliases in model SQL (`customers__regions.name`)
- Models are keyed by `(data_source, name)`; joins resolve within the parent model's datasource
- Models/queries/datasource configs carry a `version` field; storage migrations run automatically on load (`slayer/storage/migrations.py`)
- Filters support `{variable}` placeholders from `query.variables`; datasource configs support `${ENV_VAR}`

## Database Support

- Tier 1 (integration-tested, must not regress): SQLite, Postgres, DuckDB, MySQL, ClickHouse, SQL Server, BigQuery, Snowflake
- Tier 2 (unit-tested SQL generation only): Redshift, Trino/Presto, Databricks/Spark, Oracle
- Per-dialect emission lives in `slayer/sql/dialects/`; tiers and caveats in `docs/database-support.md`

## Testing

Always use `poetry run` (correct Poetry-managed virtualenv). Integration tests are marked
`@pytest.mark.integration` and skip when their DB is unavailable; shared fixtures in
`tests/conftest.py`.

```bash
poetry run pytest                                             # unit only
poetry run pytest tests/integration/ -m integration           # integration
poetry run pytest tests/ -m "integration or not integration"  # everything
poetry run pytest -m metabase_e2e tests/integration/test_metabase_e2e.py  # live Metabase e2e (needs Docker)
```

## Linting

ALWAYS run the linter at the end of every task and fix any issues before finishing:

```bash
poetry run ruff check slayer/ tests/          # check
poetry run ruff check --fix slayer/ tests/    # auto-fix
```

## Documentation Requirements

ALWAYS update documentation when making API or user-facing changes:

- `docs/` — concept docs, getting-started, reference, configuration
- `.claude/skills/` — slayer-query.md, slayer-models.md, slayer-overview.md
- When renaming a field or changing a response shape, grep all docs and skills for the old name

Every page under `docs/` must be linked from the `nav` block in `zensical.toml` (repo
root) — add or update the entry in the same commit as the page, or it ships as an orphan
the published site can't reach. Intentional exceptions: `docs/CLAUDE.md` and
`docs/api_gaps.md`.

## Design Decisions

`DECISIONS.md` (repo root) is the append-only dated log of design decisions and their
rationale, with issue refs. Consult it before changing established behavior. When you
make an important design decision in a session, append one entry at the bottom in the
existing format.
