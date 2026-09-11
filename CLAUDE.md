# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What is SLayer?

SLayer (Semantic Layer) is a lightweight, open-source (MIT) semantic layer for AI agents,
built by MotleyAI. Instead of writing raw SQL, agents describe what data they want —
measures, dimensions, filters — and SLayer generates and executes the query.

When writing SLayer query examples or answering questions about syntax, capabilities, or
feature behaviour, read `docs/` first — `docs/concepts/` (queries, models, formulas,
references), `docs/examples/`, `docs/cube/cube_import.md`, `docs/database-support.md`,
`docs/configuration/`, `docs/reference/`.

## Structure

The package map and per-node docs live in `architecture/`: package→node claims in
`architecture/index.yaml` (enforced by `tools/arch_check.py`), node descriptions in
`architecture/*.arc42.md`, cross-cutting principles in `architecture/system.arc42.md` §3,
and the enforcement-bundle commands in §4.

`architecture/**/*.arc42.md` and `architecture/**/*.c4` are normative harnesses, like
tests: NEVER create, modify, or delete one without explicit per-change user approval —
present the exact edit and wait for the OK, even when a broader plan already mentioned it.

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

## Key Conventions

- Use `poetry run` for all Python commands
- Pydantic v2 for all models; NEVER use dataclasses
- Use keyword arguments for functions with more than 1 parameter
- Imports at the top of files
- Cross-cutting code principles (AST-built SQL, async-first, the cardinality invariant,
  dotted-canonical references, the two expression layers, versioned persistence, …) are
  `architecture/system.arc42.md` §3 — all code MUST obey them

## Testing

Integration tests are marked `@pytest.mark.integration` and skip when their DB is
unavailable; shared fixtures in `tests/conftest.py`.

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

- `docs/` — concept docs, getting-started, reference, configuration (user-facing only)
- `.claude/skills/` — slayer-query.md, slayer-models.md, slayer-overview.md
- When renaming a field or changing a response shape, grep all docs and skills for the old name
- Behaviour is specified in `openspec/specs/` (via an OpenSpec change); cross-cutting
  principles and the query algebra live in `architecture/` (arc42 + status tags)

Every page under `docs/` must be linked from the `nav` block in `zensical.toml` (repo
root) — add or update the entry in the same commit as the page. Otherwise the page is
still published, but as an orphan users cannot reach through site navigation.
Intentional exceptions: `docs/CLAUDE.md` and `docs/api_gaps.md`.
