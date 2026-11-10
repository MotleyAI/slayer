## 1. Failing tests (pr-tests stage)

- [ ] 1.1 `models/list-columns` tests (extend `tests/test_dev2137_list_columns.py` / `tests/test_dev2137_element_types.py`): table-backed `ARRAY` without `element_type` refused naming the column; sql-mode undeclared → `UNKNOWN` and `skills.value` fails naming the opaque type; a stored sql-mode document without `element_type` loads and re-saves with `UNKNOWN` (YAML and SQLite); declared `TEXT` kept across a DuckDB re-ingest and `contains(skills, 'go')` runs; undeclared stays `UNKNOWN` on a DuckDB re-ingest; Snowflake sql-mode `UNKNOWN` re-inferred to `INT` (fake/emission-level client, as the existing Snowflake inference tests do). Verify each fails on `main` where the behaviour is missing
- [ ] 1.2 Item-1 edge coverage (design D16): standalone `Column(type=ARRAY)` without `element_type` is accepted; MCP `create_model` / `edit_model` canonicalise an sql-mode column to `UNKNOWN` and refuse a table-backed one; verify they fail where expected
- [ ] 1.3 `models/identifier-columns` tests: `collect(id)` accepted on a sole primary key, `sum(id)` still refused; `allowed_aggregations: [count, collect]` on a sole primary key saves (client) and loads (stored); port `1.x`'s `test_join_back_fallback_is_untyped` (every `NATIVE_LIST_DIALECTS` dialect) and `test_join_back_without_a_row_is_empty` from `origin/1.x:tests/test_dev2137_collect.py`, adapting fixtures to `main`'s (`assignments_model` etc.); facade catalog lists a `collect` metric for a sole primary key; verify they fail
- [ ] 1.4 Port the dropped `1.x` tests for the four spec-only losses onto `main`'s fixtures: the stored `contains` aggregation scenario; `TestEverySurface` (dry run, REST, MCP) into `tests/test_library_name_shadow_warnings.py`; `test_re_typing_field_effects_converge` and `TestListColumnRetype` into `tests/test_drift_list_retype.py` (description, label and meta kept; `format` and granularity dropped; second re-ingest changes nothing); `test_a_rename_converges` on YAML and SQLite. These are expected to PASS already (code survived); if any fails, it is a real loss — stop and raise it with the user
- [ ] 1.5 `compatibility/forward-merge-specs` tests (`tests/test_forward_merge_specs.py`): build throwaway git repos in `tmp_path` with a `1.x`-like branch and a `main`-like branch, one test per spec scenario (no 1.x commits enter; added requirement missing; modified requirement resolved toward base; moved capability; ledgered divergence; empty reason; stale entry; removal on 1.x; branch behind 1.x; cherry-pick caught at the next forward merge; several merge bases fail naming them). Verify they fail (module missing)
- [ ] 1.6 Corpus test `tests/test_upgrade_corpus_v1_8_2.py`, modelled on `tests/test_upgrade_corpus_v0_10_2.py`: both backends, online (load, re-save, re-ingest, recorded rows) and offline (load, no connection attempted). It needs the corpus from 2.1; verify it fails on `main` (sql-mode `ARRAY` and primary-key `collect` refused)

## 2. 1.8.2 corpus

- [ ] 2.1 Write `tests/fixtures/upgrade/v1_8_2/generate.py` in the style of `v0_10_2/generate.py` (docstring with the `uv venv` + `uv pip install motley-slayer==1.8.2 duckdb==<pin>` commands); one DuckDB datasource: ingested `contractors` (sole PK `id` with `allowed_aggregations: [count, collect]`, `skills VARCHAR[]`, `scores INTEGER[]`) and `assignments` joined to it; sql-mode model `dev_skills` with `skills: ARRAY` and no `element_type`; stored measures `collect(id)`, `collect(skills.value)`, `count(skills)`; a query-backed model using `contains(skills, 'go')` and `contains_any(skills, ["go", "sql"])` (1.8.2's double-quoted spelling, design D15); a memory holding a `collect(assignments.id)` query. Only queries whose semantics 2.0 did not deliberately change (design D14). Write YAML and SQLite stores and `expected.json`
- [ ] 2.2 Run the generator under `motley-slayer==1.8.2`; commit the generated `data.duckdb`, `yaml_store/`, `sqlite_store.db` and `expected.json`. If 1.8.2 cannot write a listed shape, drop it, note it here, and raise it at pr-implement — never hand-forge a document

## 3. Implementation

- [ ] 3.1 `slayer/core/models.py`: `check_element_type` stops requiring `element_type` on `ARRAY`; add `SlayerModel._validate_list_element_types` (after-validator, as on `origin/1.x`) refusing a table-backed `ARRAY` without one and setting sql-mode ones to `UNKNOWN`; verify 1.1, 1.2 pass
- [ ] 3.2 `slayer/core/enums.py`: add `"collect"` to `PRIMARY_KEY_AGGREGATIONS` and its comment; verify 1.3 passes
- [ ] 3.3 `scripts/forward_merge_specs.py` and an empty `openspec/forward-merge-ledger.yaml` (Pydantic models for the ledger and requirements; `git` via subprocess); verify 1.5 passes and `python -m scripts.forward_merge_specs --base origin/main --head HEAD` passes on this branch
- [ ] 3.4 `.github/workflows/ci.yml` `lint` job: `fetch-depth: 0`, fetch `1.x`, and a `pull_request`-only step running the guard against `github.event.pull_request.base.sha` and the head SHA; verify the YAML parses and the step's command runs locally
- [ ] 3.5 Verify 1.4 and 1.6 pass; run the full unit suite `poetry run pytest -m "not integration"`

## 4. Docs and help

- [ ] 4.1 `docs/concepts/models.md:82` and `:128`: the sole-primary-key list gains `collect`; fix `:128`'s "On a list column it may name only `count`" (`count` and `count_distinct`, design D17)
- [ ] 4.2 Document that an sql-mode `ARRAY` column without `element_type` is `UNKNOWN` wherever docs cover sql-mode list columns (grep `docs/` for `element_type`)
- [ ] 4.3 Grep `slayer/` help memories and MCP descriptions for the primary-key aggregation list and update; re-run (`jupyter nbconvert --to notebook --execute --inplace`) any notebook under `docs/examples/` whose output shows that list or changes
- [ ] 4.4 Document the forward-merge guard and ledger where contributor docs cover forward merges (`CONTRIBUTING.md`), one sentence

## 5. Gates

- [ ] 5.1 `poetry run ruff check slayer/ tests/ scripts/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`, `openspec validate dev-2249-1x-main-forward-merge-dropped-two-list-column --strict`
- [ ] 5.2 Integration suite with the CI invocation from CLAUDE.md
