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

Cross-cutting structure and principles live in `architecture/` (LikeC4 model + arc42 +
`index.yaml`); enforcement bundle: `poetry run lint-imports`, `poetry run python
tools/arch_check.py`, `npx -y likec4@1.47.0 validate architecture`, `poetry run basedpyright`.

## Common Commands

```bash
poetry install -E all                                # install with all extras
poetry run pytest -m "not integration"               # unit tests (excludes integration)
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
- Two expression layers — Mode A: free SQL in `Column.sql` / model `filters` (dotted join paths); Mode B: Python-AST DSL in formulas and query fields (dotted paths, colon aggregations, scalar-allowlist functions only). Rules: `docs/concepts/references.md`
- Aggregations are query-time, colon syntax: `revenue:sum`, `*:count` for COUNT(*), `price:percentile(p=0.9)`
- Result column keys are `model.column`: `revenue:sum` → `orders.revenue_sum`, `*:count` → `orders._count`; joined dimensions keep the full path (`orders.customers.regions.name`)
- Dots denote join paths in BOTH queries and model SQL (`customers.regions.name`, dotted-canonical); the legacy `__` split-alias input form is a hard error. `__` stays only as an internal generated-SQL join alias, and is a legal (exact-match) character in model/query/column names (`__slayer_` prefix reserved)
- Models are keyed by `(data_source, name)`; joins resolve within the parent model's datasource
- Models/queries/datasource configs carry a `version` field; storage migrations run automatically on load (`slayer/storage/migrations.py`)
- Filters support `{variable}` placeholders from `query.variables` (scalars, plus lists → auto-quoted `IN`-list body: `region IN ({regions})`). Values are trusted input; string escaping IS dialect-aware (DEV-1727) but only applies to *quoted* literals. Datasource configs support `${ENV_VAR}`
- Mode-A raw-SQL surfaces also support optional blocks `{? pred ?}` — render parenthesised when every inner `{var}` is supplied, else collapse to `(1=1)` (Cube `FILTER_PARAMS` optional-pushdown form, DEV-1730). `extract_model_variables(model)` classifies placeholders required vs optional; surfaced in the inspect skeleton
- `slayer import-cube` reads Cube `.yml`/`.yaml` **and `.js`** (`cube()`/`view()` via esprima); FILTER_PARAMS → `{var}`/`{? ?}`, requiredness from member `meta.required` (`--ignore-required-meta` to force optional). See `docs/cube/cube_import.md`
- A model may declare a variable `list_valued` in `meta.cube_variables` (importers do this for generated `col IN ({var})` templates); the engine then wraps a bare scalar into a one-element list. Hand-written model SQL keeps the author-writes-the-quotes convention — a scalar string substitutes unquoted

## Database Support

- Tier 1 (integration-tested, must not regress): SQLite, Postgres, DuckDB, MySQL, ClickHouse, SQL Server, BigQuery, Snowflake
- Tier 2 (unit-tested SQL generation only): Redshift, Trino/Presto, Databricks/Spark, Oracle
- Per-dialect emission lives in `slayer/sql/dialects/`; tiers and caveats in `docs/database-support.md`

## Testing

Always use `poetry run` (correct Poetry-managed virtualenv). Integration tests are marked
`@pytest.mark.integration` and skip when their DB is unavailable; shared fixtures in
`tests/conftest.py`.

```bash
poetry run pytest -m "not integration"                        # unit only
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
root) — add or update the entry in the same commit as the page. Otherwise the page is
still published, but as an orphan users cannot reach through site navigation.
Intentional exceptions: `docs/CLAUDE.md` and `docs/api_gaps.md`.
