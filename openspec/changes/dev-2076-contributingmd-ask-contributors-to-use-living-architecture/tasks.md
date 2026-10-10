## 1. Tests (pr-tests stage)

- [ ] 1.1 Gotcha: this worktree's `.venv/` is empty (Poetry created it without installing). Before running anything, run `poetry install -E all --with dev`, then verify that `poetry run la-arch-check` runs.
- [ ] 1.2 Write `tests/test_contributing_guide.py` per design D7.1–D7.4 (one test per check, imports at the top, no dataclasses). Verify that `poetry run pytest tests/test_contributing_guide.py` fails against the current CONTRIBUTING.md for the expected reasons: missing gates, missing pin, missing pointer, and stale paths such as `stage_planner.py`.

## 2. Gate: DEV-2078 released (pr-implement stage)

- [ ] 2.1 STOP unless a living-architecture release containing DEV-2078 (`living-architecture.local.yaml`) exists. Check with `git -C /home/james/GitHub/living-architecture ls-remote --tags origin`, and confirm the release notes or code. If DEV-2078 renamed the file or its keys, propose a plan amendment (design D1, D6 §4, D7.1) and wait for approval.
- [ ] 2.2 Bump `living-architecture` in `pyproject.toml` to that exact version and run `poetry lock`. Verify that `poetry install -E all --with dev` succeeds and that `poetry run la-doctor --plugin <plugin dir>` reports the matching version.
- [ ] 2.3 Check whether DEV-2079 (`ruff format`) has landed on `origin/main`. If it has, follow the reconciliation branch of design D5 (keep the hook; document `ruff format --check`). Verify by inspecting `.pre-commit-config.yaml` and `ci.yml` on main.

## 3. Docs and config

- [ ] 3.1 Rewrite `CONTRIBUTING.md` per design D6, with every section in order. Verify that D7.1, D7.2 and D7.4 pass.
- [ ] 3.2 Replace the body of `docs/development.md` with a pointer to CONTRIBUTING.md, keeping the title, the URL and the `zensical.toml` nav entry. Verify that D7.3 passes.
- [ ] 3.3 Correct `docs/database-support.md` → "Adding a new dialect" per design D4. Verify that every symbol and path it names exists (`SqlDialect`, `_ALL_DIALECTS`, `_DS_TYPE_ONLY_DIALECTS`, `sqlglot_name`, `ds_type_aliases`, `tests/dialects/test_multi_dialect_generation.py`).
- [ ] 3.4 Remove the `ruff-format` hook from `.pre-commit-config.yaml` (unless 2.3 took the D5 reconciliation branch). Verify that `poetry run python -m pre_commit run --files CONTRIBUTING.md` passes.
- [ ] 3.5 Add `living-architecture.local.yaml` to `.gitignore`. Verify with `git check-ignore living-architecture.local.yaml`.

## 4. Verification

- [ ] 4.1 Run the full unit suite (`poetry run pytest -m "not integration"`), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` and `poetry run la-arch-check`. All must be green.
