## 1. Tests (pr-tests stage)

- [x] 1.1 No new tests (design decision 8): `tests/test_sql_client_cancellation.py` from 1.6.1 is the suite; verify it is present on the branch

## 2. Merge (pr-implement stage)

- [ ] 2.1 `git fetch origin`, then `git merge --no-ff origin/main -m "Merge branch 'origin/main' into egor/dev-2206-cancelling-a-sql-clients-blocking-call-returns-at-once"`; resolve the four conflicts per design decisions 2–5 (`slayer/sql/client.py` and `tests/test_sql_client.py` → `main`'s side, keeping the auto-merged `_run_sync_in_thread`; spec → union with `main`'s requirements first; `pyproject.toml` → `1.7.0`); verify no conflict markers remain (`git diff --check`) and `_map_type_code` is gone from `slayer/`
- [ ] 2.2 Reword the doc sentence in `docs/configuration/sql-clients.md` "Lifecycle and concurrency" verbatim per design decision 6, in the merge commit; verify by reading the paragraph
- [ ] 2.3 Run `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` and `poetry run la-arch-check`; fix every failure in the merge commit; all green, with `tests/test_sql_client_cancellation.py` passing
- [ ] 2.4 Verify `openspec validate --specs --strict` passes on the merged corpus and that the spec carries the cancellation requirement exactly once

## 3. Close-out (pr-review stage)

- [ ] 3.1 Instead of `openspec archive`, `git rm -r` this change folder (design decision 7), since the 1.6.1 change is already archived; verify `openspec/changes/` has no `dev-2206-*` folder and the spec still carries the requirement exactly once
