# Contributing to SLayer

Thanks for your interest in contributing! This guide gets you set up and walks you through how changes are made here.

## Quick Start

```bash
git clone https://github.com/MotleyAI/slayer.git
cd slayer
poetry install -E all --with dev
poetry run pytest -m "not integration"
```

## Development Setup

**Requirements:** Python 3.11+, [Poetry](https://python-poetry.org/) 2.x

```bash
# Install with all extras + dev dependencies
poetry install -E all --with dev

# Install the pre-commit hook (ruff check --fix on staged files)
poetry run python -m pre_commit install
```

Verify your setup with the same gates CI runs. CI also adds `-n logical`, timeouts and `--durations` to pytest.

```bash
poetry run pytest -m "not integration"
poetry run ruff check slayer/ tests/
poetry run basedpyright   # no new errors vs .basedpyright/baseline.json
poetry run la-arch-check
```

## Running Tests

```bash
# Unit tests (no external database; add -n logical to parallelise)
poetry run pytest -m "not integration"

# Integration tests, exactly as CI runs them
poetry run pytest tests/ -m "integration and not metabase_e2e" --timeout=180 --durations=25 \
  -n logical --dist loadscope \
  --ignore=tests/integration/test_integration_mysql.py \
  --ignore=tests/integration/test_integration_clickhouse.py \
  --ignore=tests/integration/test_integration_sqlserver.py \
  --ignore=tests/integration/test_integration_trino.py

# Performance benchmarks
poetry run pytest tests/perf/ --benchmark-only
```

Backends:

- **SQLite and DuckDB** run in-process. Tests that use them are unit tests.
- **Postgres** is spawned from your local Postgres binaries by `pytest-postgresql`; no Docker needed.
- **MySQL, ClickHouse, SQL Server and Trino** run through testcontainers (Docker). SQL Server also needs the ODBC Driver 18. In CI, each runs in its own path-gated `.github/workflows/integration-*.yml` workflow.
- **BigQuery and Snowflake** need credentials, so locally only their SQL-emission tests run.

The Docker examples (`examples/postgres`, `examples/mysql`, `examples/clickhouse`, `examples/trino`, `examples/sqlserver`) all expose SLayer on port 5143, so run them one at a time:

```bash
docker compose -f examples/postgres/docker-compose.yml up --build -d
poetry run python examples/postgres/verify.py
docker compose -f examples/postgres/docker-compose.yml down -v
```

## Working with living-architecture

SLayer uses [living-architecture](https://github.com/MotleyAI/living-architecture) to keep its code, architecture and specs in step. Please drive every change through it.

`architecture/` is normative:

- `architecture/model.c4` maps packages to nodes and declares which imports between them are allowed. CI enforces it with `la-arch-check`.
- `architecture/system.arc42.md` §3 holds the cross-cutting principles that all code obeys.
- Each node's `architecture/*.arc42.md` holds that node's own principles.

Creating, modifying or deleting any `architecture/**/*.c4` or `architecture/**/*.arc42.md` file needs a maintainer's sign-off. Raise it in the issue or PR before you make the edit.

Make changes with `/la:pr` in [Claude Code](https://docs.claude.com/en/docs/claude-code). It runs four stages: plan, failing tests, implement, then review. The plan is recorded as an OpenSpec change under `openspec/changes/`.

**Prerequisites:** Claude Code, git, bash, an authenticated `gh`, `jq`, and Node.js with `npx` (for the OpenSpec and LikeC4 CLIs).

**Install the `la` plugin** at the version pinned in `pyproject.toml`, from inside Claude Code:

```text
/plugin marketplace add MotleyAI/living-architecture#v0.3.1
/plugin install la@living-architecture
```

The plugin's `la-*` commands come from the Poetry dev group, so start Claude Code inside the Poetry environment:

```bash
eval $(poetry env activate)
claude
```

Then run `/la:pr` and describe your change. After the pin in `pyproject.toml` changes, re-add the marketplace at the new tag.

**Outside contributors:** `living-architecture.yaml` sets this repo up for MotleyAI's Linear tracker and the Codex reviewer. To override that, create a `living-architecture.local.yaml` at the repo root. It's git-ignored and is merged over the committed config:

```yaml
tracker: none     # no tracker issue: your /la:pr prompt is the brief
reviewers:
  codex: false    # only if you have no Codex MCP server
```

**When the model changes**, regenerate the arc42 diagrams and validate the model:

```bash
poetry run la-arch-diagrams
npx -y likec4@1.47.0 validate architecture
```

## Code Style

- **Line length:** 120 characters
- **Models:** Pydantic v2; never dataclasses
- **Imports:** at the top of files
- **Keyword arguments:** required for functions with more than one parameter
- **SQL generation:** sqlglot AST building, never string concatenation
- **Principles:** all code obeys `architecture/system.arc42.md` §3

**Pre-commit gotcha:** hooks run on *staged* files, not on the working tree. If you edit a file after `git add`, you must `git add` it again before committing.

## Documentation

Preview locally:

```bash
pip install zensical
zensical serve   # http://localhost:8000, live reload
```

- **Style rule:** use JSON/dict syntax in all docs and examples, not Python class constructors. Write `{"name": "status"}`, not `ColumnRef(name="status")`. This keeps examples portable across Python, the REST API and MCP (see `docs/CLAUDE.md`).
- **Navigation:** every page under `docs/` must be linked from the `nav` block in `zensical.toml`. The exceptions are `docs/CLAUDE.md` and `docs/api_gaps.md`.
- **Notebooks:** `.ipynb` files are listed by their `docs/`-relative path. Zensical can't render them inline, so the docs sync rewrites them to GitHub links, and local `zensical serve` shows them as plain links.
- **User-facing changes** update `docs/`. New database dialects follow "Adding a new dialect" in `docs/database-support.md`.

## Scope-closure validation

The test suite validates every generated SQL statement for **scope closure**: no SELECT scope may reference a table alias it doesn't bind, and no cross-scope reference may name a column that an inner scope doesn't project. This runs automatically under pytest, because `SLAYER_VALIDATE_SCOPES=1` is set for the whole suite, so a scope leak fails at generation time.

To run the same check at runtime while debugging (for example, when a generated query fails against a live database):

```bash
SLAYER_VALIDATE_SCOPES=1 poetry run slayer serve
```

A provable out-of-scope reference raises `ScopeLeakError` from `slayer/sql/scope_check.py`. The validator is deliberately conservative, so it never reports false positives. It treats unqualified references and physical-table column names as unverifiable and never flags them.

## Publishing

Publishing a GitHub release publishes to PyPI (`.github/workflows/publish.yml`), and the release tag must match the package version. To publish to TestPyPI, run the "Publish to TestPyPI" workflow manually (`.github/workflows/publish-test.yml`). Both use OIDC trusted publishing, so no API tokens are needed.

## Pull Requests

- Branch from `main`, and keep each PR focused on one feature or fix.
- Include tests for new behaviour.
- Update `docs/` when your change affects user-facing behaviour.

Before pushing, run:

```bash
poetry run pytest -m "not integration"
poetry run ruff check slayer/ tests/
poetry run basedpyright
poetry run la-arch-check
poetry run la-arch-diagrams   # when the model changed
```
