## 1. Tests (spec-tests stage)

- [x] 1.1 Write failing scenario tests for every `#### Scenario` in `specs/models/sample-profiling/spec.md` as unit tests on in-process SQLite/DuckDB engines (NOT `@pytest.mark.integration`); count executed queries by wrapping the engine's SQL client / `engine.execute`; simulate a column failure with a column whose `sql` errors and a model-wide failure with a missing `sql_table`; patch the profiling clock for TTL scenarios; use `caplog` for WARNING counts. Verify: each new test fails on the current code for the stated reason.
- [x] 1.2 Cover the non-scenario D6/D1 details: persist-failure partial + total (storage double that raises), `ProfileOutcome.errors` contents, forced refresh keeps `only_columns` and the `_is_table_backed` gate. Verify: tests fail before implementation where behaviour changes.
- [x] 1.3 Rewrite tests that exercise the deleted helpers (`tests/test_engine_profiling.py`, `tests/test_entry_points_namespacing.py`, `tests/test_dev1967_identifier_columns.py`, `tests/integration/test_mcp_inspect.py`) against the owner — ask the user's consent per file before changing any test's logic; mechanical call-site renames that preserve assertions need no OK. Verify: rewritten tests pass their collection and assert the same observable behaviour.
- [x] 1.4 Codex-review the tests against the spec and design. Verify: findings resolved or dispositioned with the user.

## 2. Owner (`slayer/engine/profiling.py`)

- [x] 2.1 Add `ProfileOutcome`, `_EngineProfileState` (Pydantic), the `WeakKeyDictionary` engine state, the model/column fingerprint helper and the injectable monotonic clock (design D3). Verify: fingerprint unit tests (sample fields excluded, `sql`/`sql_table` edits change it).
- [x] 2.2 Make the categorical and numeric query helpers raise instead of returning `None`; numeric all-NULL → `all NULL`, categorical empty → `[]` / `0` / `""` (D4). Verify: the successful-empty scenarios pass.
- [x] 2.3 Implement `ensure_samples_fresh`: filtering, probe-on-first-failure, 3-consecutive breaker, per-column numeric isolation, failure cache, logging, persist-failure handling, `force` (D1, D2, D6). Verify: classification, bounding, failure-cache and logging scenarios pass.
- [x] 2.4 Implement the policy branch (D5): no storage reads/writes, engine-scoped positive cache, returned columns never carry stored samples. Verify: the RLS scenarios for the owner pass.
- [x] 2.5 Re-implement `refresh_table_backed_model_sampled` / `refresh_all_table_backed_sampled` on the owner with `force=True`, keeping signatures and the table-backed gate. Verify: forced-refresh scenarios + `slayer search refresh-samples` CLI tests pass.
- [x] 2.6 Delete `ensure_column_sample_fresh`, `profile_column`, `_refresh_one_column`, `_profile_categorical_column`, `_collect_dim_profile`, `_DimProfileEntry`, `_format_dim_profile_value`; trim the module docstring to a concise current-state summary with no issue numbers. Verify: LSP finds no references; `ruff` clean.

## 3. Callers

- [x] 3.1 `slayer/inspect/model_render.py`: replace the categorical loop, inline numeric batch, `_persist_sample` and `_collect_measure_profile` with one owner call; render only from returned columns (no stored-sample pre-pass under a policy). Verify: `inspect_model` scenarios (one numeric query, parity, policy) pass.
- [x] 3.2 `slayer/inspect/service.py` `_maybe_refresh_leaf_sample`: call the owner with `[col]`. Verify: `inspect` column scenarios pass.
- [x] 3.3 `slayer/search/service.py` column-hit refresh: one owner call per model group; re-render hit text from the returned column when changed or when the engine has a policy. Verify: search scenarios (bounded queries, policy hit text) pass.
- [x] 3.4 Remove issue-number references and over-long comments in the touched code of all three callers. Verify: grep for `DEV-` in touched hunks is empty.

## 4. Docs

- [x] 4.1 `docs/concepts/search.md` "Sample-value cache": one sentence — failures are cached per engine for an hour, a failing model is probed once then skipped. Verify: page renders in `zensical` nav (already linked).
- [x] 4.2 `docs/concepts/row-level-security.md`: one sentence — under a policy, samples are profiled per engine and never read from or written to shared storage. Verify: text present.

## 5. Gates

- [x] 5.1 `poetry run pytest -m "not integration"` all green; `poetry run ruff check slayer/ tests/` clean; `poetry run basedpyright` no new errors vs baseline; `la-arch-check` (pinned uvx form in CLAUDE.md) green. Verify: command outputs.
- [x] 5.2 Integration suite with the CI invocation (CLAUDE.md) for `tests/integration/test_mcp_inspect.py` and any other touched integration file. Verify: green or skipped-for-unavailable-DB only.
