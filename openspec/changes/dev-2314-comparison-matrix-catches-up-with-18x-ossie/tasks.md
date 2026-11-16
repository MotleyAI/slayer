## 1. Tests (pr-tests stage)

- [ ] 1.1 Port `main`'s `test_list_columns_row_has_probes_on_every_engine` (and its `import yaml`) byte for byte into `tests/test_comparison_matrix.py` (`git show origin/main:tests/test_comparison_matrix.py`); verify it fails on `1.x` (no Q27 row)
- [ ] 1.2 Add a version → probe-ref unit test to `tests/test_comparison_matrix.py` (`1.8.5` → `1.x`, `2.0.0.dev0` → `main`, `2.1.0` → `main`) and assert the committed `matrix.json` links use `blob/1.x` (`probes[*].url`) and `tree/1.x` (`source`); verify both fail before 3.1
- [ ] 1.3 Add the 7 `Q27-*` probes to `probes.yaml` byte for byte from `main` (after the Q26 block, as on `main`); verify `run_slayer.py --row Q27` fails before task 2.1 (no `tags` column)
- [ ] 1.4 Add the 5 C23 probes to `probes.yaml` (design Decision 8): `C23-ossie-roundtrip-models`, `-roundtrip-transform`, `-roundtrip-query-backed`, `-report-extension`, `-report-plain`; write their `truth_sql` against `dataset.sql` and their `contains` strings from the real export report; verify `run_slayer.py --row C23` fails before 3.2 (unknown `osi_roundtrip` field)

## 2. Q27 / Q22 / C4 data and fixtures from `main`

- [ ] 2.1 Take byte for byte from `main`: `dataset.sql`, `slayer/models/customers.yaml`, `cube/model/customers.yml`, `cube/model_declared/customer_tags.yml`, `metricflow/dbt_project/models/{semantic.yml,model_declared.yml,stg_customer_tags.sql}`, the list `canon` in `metricflow/run_metricflow.py`, the array `canon` in `malloy/run_malloy.mjs`, the README `customers.tags` bullet; verify `git diff origin/main -- <each file>` is empty, except `run_metricflow.py`, `run_malloy.mjs` and `README.md`, whose only diffs vs `main` are outside those hunks
- [ ] 2.2 `matrix.yaml`: add the Q27 row after Q26 and replace the Q22 SLayer cell, both byte for byte from `main`; set C4 SLayer text to "9 engines tested live, 4 more by SQL generation."; set the C23 SLayer cell and comment to design Decision 3's approved wording; leave C9 and Q6 untouched; verify by diffing those rows against `main`

## 3. Runner and exporter

- [ ] 3.1 `export_matrix.py`: derive the ref from `pyproject.toml` `version` (below 2 → `1.x`, else `main`) for `PROBES_URL` (`blob/<ref>/`) and `source` (`tree/<ref>/`); verify 1.2 passes
- [ ] 3.2 `slayer/run_slayer.py`: `osi_roundtrip: {extensions: bool}` mode per design Decisions 5–7: `[sys.executable, "-m", "slayer", ...]`, export and import captured separately with non-zero return codes failing the probe, a fresh import store holding only the `probe` datasource, and `expect` as `match` (query on the re-imported store) / `{same_models: true}` / `{contains, lacks}` on the export report; refuse it with `steps` / `run_by_name`; document the field in the `probes.yaml` header; verify `run_slayer.py --row C23` gives 5 PASS
- [ ] 3.3 README: one sentence on the C23 Ossie round-trip probes next to the `steps` note; verify by reading the rendered paragraph

## 4. Regenerate and verify

- [ ] 4.1 Regenerate `matrix.json` with `export_matrix.py`; verify `export_matrix.py --check` exits 0 and the file has 56 rows
- [ ] 4.2 Run the full SLayer runner; verify there is no FAIL and no FIXED
- [ ] 4.3 Run the full Malloy runner (`npm ci --ignore-scripts`); verify there is no FAIL and no FIXED, with the Q27 probes PASS
- [ ] 4.4 Run the full Cube runner (`npm ci`, both planners); verify there is no FAIL and no FIXED, with Q27 matching `main`'s expectations (the `Q27-region-tags` by-design rows reproduce)
- [ ] 4.5 Run the full MetricFlow runner (its venv from `requirements.txt`); verify there is no FAIL and no FIXED, with the Q27 probes PASS
- [ ] 4.6 Grep the repo for capability counts ("54", "55", "56" next to capabilities / rows); verify only the `54-capabilities` slug remains (design Decision 12)
- [ ] 4.7 Run the local test scope (`la-affected-tests --base 1.x` plus `tests/test_comparison_matrix.py`), `ruff check slayer/ tests/ examples/comparisons/` and `la-typecheck`; verify all green
