## Context

See proposal.md (Why). Constraints:

- living-architecture reads only the committed `living-architecture.yaml`. When unset, `tracker` defaults to `linear` and `reviewers.codex` to `true`, so an outside contributor's `/la:pr` stops in `pr-plan`: there is no Linear access to resolve the branch's issue, and no Codex MCP server. DEV-2078 adds a git-ignored `living-architecture.local.yaml` that is deep-merged over the committed file.
- `la-doctor` stops every `/la:pr` stage when the `la-*` commands' version differs from the plugin's `plugin.json` version.
- `docs.motley.ai` pulls the `zensical.toml` nav and re-roots local paths (see the header comment of `zensical.toml`).
- CI (`.github/workflows/ci.yml`) gates on `ruff check`, `la-arch-check`, the unit suite and `basedpyright`, not on `ruff format`.

## Goals / Non-Goals

**Goals:** CONTRIBUTING.md is the single, CI-checked contributor guide, and it asks everyone (outside contributors included) to use `/la:pr`.

**Non-Goals:**

- The repo-wide `ruff format` (DEV-2079).
- The local-override feature itself (DEV-2078).
- First-issue and other "contributing bits" (DEV-1224).
- Any `architecture/**` edit.

## Decisions

**D1. Outside contributors: a git-ignored local override.**
- Contributors without MotleyAI Linear access set `tracker: none` in `living-architecture.local.yaml` (DEV-2078). Without a Codex MCP server they also set `reviewers.codex: false`.
- Rejected:
  - An uncommitted edit to the tracked `living-architecture.yaml`: too easy to commit by accident.
  - A manual OpenSpec fallback with no Claude Code: two paths to keep in sync, and it weakens the ask.

**D2. The pyproject pin is the single version source.**
- The plugin is installed as `/plugin marketplace add MotleyAI/living-architecture#v<pin>` followed by `/plugin install la@living-architecture`.
- The `la-*` commands come from the Poetry dev group, so Claude Code is launched inside the Poetry env (`eval $(poetry env activate)`, then `claude`).
- Rejected:
  - `uv tool install`: a second, global copy whose version has to be bumped by hand.
  - A committed `.claude/settings.json` with `extraKnownMarketplaces` + `enabledPlugins`: it duplicates the `la:*` skills for maintainers who load `la` from a local checkout, and still leaves the commands off PATH.

**D3. CONTRIBUTING.md is the single source; `docs/development.md` becomes a pointer.**
- The docs page keeps its title, URL and nav entry, and its body links to `https://github.com/MotleyAI/slayer/blob/main/CONTRIBUTING.md`.
- Its unique sections (scope-closure validation, publishing) move into CONTRIBUTING.md.
- Rejected:
  - The docs page as the source: repo links would have to be absolute, and contributor material would be published to end users.
  - Deleting the page: it breaks the public URL.

**D4. No code-layout descriptions in CONTRIBUTING.md.**
- The structure tree and the four "Making Changes" recipes go. The guide points at `architecture/` instead.
- The one checklist outside contributors need (new dialects) lives only in `docs/database-support.md` → "Adding a new dialect", corrected to the registry:
  1. Subclass `SqlDialect`, setting `sqlglot_name` and `ds_type_aliases`.
  2. Import the subclass in `slayer/sql/dialects/__init__.py`.
  3. Register it in `_ALL_DIALECTS`, or in `_DS_TYPE_ONLY_DIALECTS` when it shares another dialect's `sqlglot_name` (MariaDB). Otherwise it silently overwrites that dialect's entry in `_BY_SQLGLOT_NAME`.
  4. Add tests to `tests/dialects/test_multi_dialect_generation.py`.

**D5. `ruff format` comes out until DEV-2079.**
- The `ruff-format` hook and every `ruff format` instruction are removed, because the check fails repo-wide and CI does not run it.
- Reconciliation: if DEV-2079 has landed on `main` before implementation, keep the hook and document `ruff format --check` in the verify block and the PR checklist instead.

**D6. CONTRIBUTING.md content contract (sections, in order).**
1. **Quick Start:** `poetry install -E all --with dev`; `poetry run pytest -m "not integration"`.
2. **Development Setup:**
   - Python 3.11+ and Poetry 2.x.
   - `pre-commit install` (ruff check `--fix` only).
   - A verify block described as "the same gates CI runs", in local form (CI adds `-n logical`, timeouts and `--durations`): `pytest -m "not integration"`, `ruff check slayer/ tests/`, `basedpyright` (no new errors vs the baseline), `la-arch-check`.
3. **Running Tests:**
   - Unit tests, and the integration suite with CI's exact invocation (`-n logical --dist loadscope` plus its `--ignore`s).
   - Backends: SQLite/DuckDB in-process; Postgres via pytest-postgresql; MySQL/ClickHouse/SQL Server/Trino via testcontainers, in their path-gated workflows; BigQuery/Snowflake need credentials.
   - Docker examples (postgres, mysql, clickhouse, trino, sqlserver) share port 5143, so run one at a time.
   - Perf benchmarks.
4. **Working with living-architecture:**
   - `architecture/` is normative: `architecture/model.c4` (package→node map and allowed imports), `architecture/system.arc42.md` §3 (cross-cutting principles all code obeys) and the per-node `architecture/*.arc42.md`.
   - Creating, modifying or deleting any `.c4`/`.arc42.md` needs maintainer sign-off.
   - `/la:pr` runs plan → failing tests → implement → review, with the plan as an OpenSpec change under `openspec/changes/`.
   - Prerequisites: Claude Code, git, bash, an authenticated `gh`, jq, and Node/`npx` (the OpenSpec and LikeC4 CLIs).
   - Install per D2, plus "after a pin bump, re-add the marketplace at the new tag".
   - Outside contributors per D1.
   - When the model changes: `poetry run la-arch-diagrams` and `npx -y likec4@1.47.0 validate architecture`.
5. **Code Style:**
   - Line length 120, Pydantic v2 and never dataclasses, imports at the top, keyword arguments for more than one parameter, sqlglot AST.
   - A pointer to `architecture/system.arc42.md` §3.
   - The pre-commit staged-files gotcha.
6. **Documentation:**
   - Zensical preview, the JSON/dict style rule and the notebook note.
   - Every `docs/` page is linked from the `zensical.toml` nav, except `docs/CLAUDE.md` and `docs/api_gaps.md`.
   - User-facing changes update `docs/`; new dialects follow `docs/database-support.md`.
7. **Scope-closure validation** and **Publishing**, moved from `docs/development.md`.
8. **Pull Requests:**
   - Branch from `main`, keep PRs focused, add tests for new behaviour, update docs.
   - The checklist is a fenced block: `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright`, `poetry run la-arch-check`, and `poetry run la-arch-diagrams` (when the model changed).

**D7. Drift guard: `tests/test_contributing_guide.py`.** A unit test in the default suite. It does not parse prose, only code spans, fenced blocks and named files.

1. **Paths exist.**
   - Every inline code span and fenced-block line in CONTRIBUTING.md is split on whitespace.
   - A token counts as a repo path when it contains `/` or ends in `.md`, `.yaml`, `.toml`, `.c4`, `.py` or `.json`.
   - Tokens are skipped when they contain `://`, `<`, `#`, `@` or `$`, or start with `/` or `-`. A `--opt=value` token is checked on its value.
   - Each path must exist relative to the repo root. The one exception is `living-architecture.local.yaml`, which must instead be listed in `.gitignore`.
2. **CI gates are in the checklist.**
   - Collect every `poetry run <tool>` in `ci.yml`'s `lint-and-test` and `type-check` jobs, minus the setup tool `pip`.
   - Each tool must appear on a command line inside the "Pull Requests" section's fenced block.
   - `ruff check slayer/ tests/`, `basedpyright` and `la-arch-check` must match CI's command exactly.
   - The pytest line must contain `-m "not integration"`.
   - `poetry run la-arch-diagrams` must also be present.
3. **Pointer page.** `docs/development.md` links to `https://github.com/MotleyAI/slayer/blob/main/CONTRIBUTING.md`.
4. **Plugin pin.**
   - `pyproject.toml`'s `living-architecture` dev pin is an exact version (no `^`, `~`, `>` or `<`).
   - CONTRIBUTING.md contains `MotleyAI/living-architecture#v<that version>`.

Rejected: also asserting `poetry.lock`'s version. Poetry 2 already refuses `poetry install` when the lock's content-hash doesn't match `pyproject.toml`.

## Risks / Trade-offs

- [DEV-2078 changes the override's file name or keys during its own planning] → Amend D1, D6 §4 and D7.1 before implementing (plan-change approval required).
- [Renaming a CI job breaks D7.2] → The test fails loudly, naming the missing job. Update the test with the rename.
- [Contributors launch `claude` outside the Poetry env] → `la-doctor` reports the missing commands. The guide states the launch step next to the install step.
- [The pointer page sends docs readers to GitHub `main`, not the released tag] → Accepted. The guide describes the development workflow, which tracks `main`.
