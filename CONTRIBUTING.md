# Contributing to SLayer

Thanks for your interest in contributing! This guide will get you set up and oriented.

## Quick Start

```bash
git clone https://github.com/MotleyAI/slayer.git
cd slayer
poetry install -E all --with dev
poetry run pytest
```

## Development Setup

**Requirements:** Python 3.11+, [Poetry](https://python-poetry.org/) 2.x

```bash
# Install with all extras + dev dependencies
poetry install -E all --with dev

# Install pre-commit hooks (ruff lint with --fix, ruff format --check on every commit)
poetry run python -m pre_commit install

# Verify everything works
poetry run pytest
poetry run ruff check slayer/ tests/
poetry run ruff format --check slayer/ tests/
```

## Running Tests

```bash
# Unit tests (fast, no database needed)
poetry run pytest

# SQLite integration tests
poetry run pytest tests/integration/test_integration.py -m integration

# Postgres integration tests (auto-spawns temp Postgres via pytest-postgresql)
poetry run pytest tests/integration/test_integration_postgres.py -m integration

# DuckDB integration tests (in-process, no Docker)
poetry run pytest tests/integration/test_integration_duckdb.py -m integration

# Docker examples (sequential — they share port 5143)
cd examples/postgres && docker compose up --build -d
sleep 5 && poetry run python verify.py
docker compose down -v
# Repeat for mysql/, clickhouse/

# Performance benchmarks
poetry run pytest tests/perf/ --benchmark-only
```

## Code Style

- **Linting & formatting:** ruff (enforced via pre-commit hooks)
- **Line length:** 120 characters
- **Python:** 3.11+, type hints encouraged, Pydantic v2 for models
- **Imports:** at the top of files, except lazy imports in CLI/server entry points
- **Keyword arguments:** required for functions with more than 1 parameter
- **SQL generation:** sqlglot AST building, never string concatenation

**Pre-commit gotcha:** hooks run on *staged* files, not working tree. If you edit a file after `git add`, you must `git add` again before committing.

```bash
# Fix formatting issues
poetry run ruff format slayer/ tests/
poetry run ruff check --fix slayer/ tests/
git add -u && git commit
```

## Project Structure

```text
slayer/
  core/             # Domain models, enums, query/formula parsers
  engine/           # Query orchestration
    query_engine.py   # Central orchestrator (execute, model resolution)
    enrichment.py     # SlayerQuery → EnrichedQuery transformation
    enriched.py       # EnrichedQuery dataclasses
    ingestion.py      # Auto-ingestion from database schemas
  sql/              # SQL generation (sqlglot) and execution (SQLAlchemy)
  storage/          # Storage backends (YAML, SQLite, pluggable registry)
  api/server.py     # FastAPI REST API
  mcp/server.py     # MCP server (FastMCP)
  client/           # Python SDK (remote + local mode)
  cli.py            # CLI entry point (argparse)

tests/
  integration/      # Integration tests (real databases)
  perf/             # Performance benchmarks
  test_*.py         # Unit tests

docs/
  getting-started/  # Task-oriented setup guides (MCP, CLI, REST API, Python)
  concepts/         # Conceptual docs (models, queries, formulas, ingestion)
  reference/        # Complete reference (all endpoints, flags, tools)
  examples/         # Tutorial notebooks with companion .md posts
  configuration/    # Datasources, storage backends
```

## Making Changes

### Adding a new database dialect

1. Add the dialect mapping in `query_engine.py` → `_dialect_for_type()`
2. If it needs custom date arithmetic, add a branch in `generator.py` → `_build_time_offset_expr()`
3. If EXPLAIN syntax differs, add to `_EXPLAIN_PREFIX` / `_EXPLAIN_SUFFIX` in `query_engine.py`
4. Add parametrized tests in `TestMultiDialectGeneration` in `test_sql_generator.py`
5. Update `docs/configuration/datasources.md` with the driver install info

### Adding a new transform function

1. Add the function name to `ALL_TRANSFORMS` and/or `TIME_TRANSFORMS` in `core/formula.py`
2. Handle it in the formula parser (`parse_formula`)
3. Add the SQL generation in `generator.py`
4. Add enrichment support in `enrichment.py` if it needs special handling
5. Add unit tests in `test_sql_generator.py` and integration tests
6. Document in `docs/concepts/formulas.md`

### Adding a new storage backend

1. Implement the `StorageBackend` protocol from `storage/base.py`
2. Optionally register it via `register_storage()` for URI-based resolution
3. Add tests in `test_storage.py` or a new test file
4. Document in `docs/configuration/storage.md`

### Adding a new MCP tool

1. Add the tool function in `mcp/server.py` inside `create_mcp_server()`
2. Add tests in `test_mcp_server.py`
3. Document in `docs/reference/mcp.md`

## Documentation

Preview locally:

```bash
pip install mkdocs-material mkdocs-jupyter mkdocs-section-index
python3 -m mkdocs serve -a localhost:8000
```

**Style rule:** Use JSON/dict syntax in all docs and examples — not Python class constructors. Write `{"name": "status"}` not `ColumnRef(name="status")`. This keeps examples portable across Python, REST API, and MCP. (See `docs/CLAUDE.md` for details.)

**Plugins we use:**

- `mkdocs-jupyter` — renders `.ipynb` notebooks as pages
- `mkdocs-section-index` — makes section headers clickable (links to `index.md` or first untitled entry)

## Pull Requests

- Branch from `main`
- Include tests for new functionality
- Run `poetry run pytest` and `poetry run ruff check` before pushing
- Keep PRs focused — one feature or fix per PR
- Update docs if your change affects user-facing behavior (check CLAUDE.md, docs/, .claude/skills/)
