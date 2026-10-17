## 1. Baseline and runner setup (pr-tests)

- [x] 1.1 Set up the runners (`npm ci` in `malloy/` and `cube/`, the MetricFlow venv per `examples/comparisons/README.md`) and verify each runs `--row Q25` without `FAIL`

## 2. Q26 probes (pr-tests)

- [x] 2.1 Add the D5 probes to `probes.yaml` under a `Q26` section after Q25, each with one shared `truth_sql`, the D5 per-engine blocks, specific error substrings, `warning: unrelated_dataset` on SLayer's positive probes, and "model-declared workaround (not counted)" in workaround titles; verify the SLayer runner passes `--row Q26`
- [x] 2.2 Verify `tests/test_comparison_matrix.py` fails (probe row Q26 not in `matrix.yaml`) and the Malloy, Cube and MetricFlow runners fail Q26 (no `events` source yet)

## 3. Rival models (pr-implement)

- [ ] 3.1 Add a standalone `events` source to `malloy/model.malloy`, an `events` cube to `cube/model/`, and an `events` semantic model with a simple count metric to the MetricFlow dbt project (cross-model derived metric, if used, in `model_declared.yml`); verify each runner reaches Q26's expected outcomes
- [ ] 3.2 Run all four runners (Cube `--planner both`) both in a temporary `git worktree` at `git merge-base HEAD origin/main` and on the branch; verify every pre-existing probe's status is identical, no probe is `FAIL`, then remove the worktree
- [ ] 3.3 If a Q26 outcome contradicts D5, apply D6 (cell follows the probe; a genuine engine bug gets `known_bug` plus a "Known issues" entry in `examples/comparisons/README.md`) and verify the runners exit zero

## 4. Matrix (pr-implement)

- [ ] 4.1 Edit C13 per D2 and C16 per D3 in `matrix.yaml`; verify by diff
- [ ] 4.2 Add Q26 per D4 after Q25, with verdicts and text set from the actual 3.x probe outcomes under D6; verify by diff against the runner output
- [ ] 4.3 Run `poetry run python examples/comparisons/export_matrix.py` and verify `tests/test_comparison_matrix.py` passes

## 5. README picture (pr-implement)

- [ ] 5.1 Write `examples/readme/sql_vs_slayer.html` per D1 with the render command in a header comment; render `docs/images/sql-vs-slayer.png` with headless Chrome and verify the PNG visually (Read it)
- [ ] 5.2 Point `README.md` line 47 at the raw `main` URL of `docs/images/sql-vs-slayer.png`; verify by diff

## 6. Gates (pr-implement)

- [ ] 6.1 Run `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/` and `poetry run basedpyright`; verify all green
