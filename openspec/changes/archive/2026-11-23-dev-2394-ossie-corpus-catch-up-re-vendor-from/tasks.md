## 1. Tests first (pr-tests)

- [x] 1.1 `--vendor` tests on a throwaway git repo (`tests/test_ossie_schema_drift.py`): byte-identical copies, computed blob shas and commit/date in provenance, removal of a file the previous provenance listed, an unrelated local fixture kept, path traversal refused, a failed harvest leaving every vendored file and the provenance unchanged; verify they fail before the mode exists.
- [x] 1.2 Native validity tests: the rule 7–9 matrix (dataset `AGG(source.measure)` forms accepted, every invalid form from upstream's `test_rejects_an_invalid_dataset_scoped_agg_call` refused with upstream's text, `FILTER`, field named `source`, field-expression `AGG` wording, model-scoped acceptance, superseded portable entry, vendor-dialect exemption); the four `3719487` constructs; the `d4dca4e` field-aggregate cases (unwindowed, windowed, `PARTITION BY`/`ORDER BY`, subquery, other dialect, superseded entry); verify they fail on today's code.
- [x] 1.3 Both sqlglot shapes: run each `AGG` case parsed as `exp.Anonymous` and as `exp.Agg` (the generic shape forced by removing the parser's `AGG` builder; the `exp.Agg` half skips where sqlglot lacks the node, and a CI job on the newest `sqlglot[c]` runs it) through the validity check, the model-metric save rules and the document loader's warning; verify the `exp.Agg` half fails today under the newest sqlglot.
- [x] 1.4 Expression-fixture parity without `uv`: every `compliance/expressions/valid.txt` line passes the `OSSIE_SQL_2026` expression syntax check, every `invalid.txt` line fails it, except upstream-deferred `SELECT a FROM t` (pinned accepted); keep a `uv`-gated comparison with upstream `validate.py` on the same lines.
- [x] 1.5 Document-text parser tests: repeated key, malformed `!!timestamp` / `!!int`, merge-key precedence (`<<: [*first, *second]`), each through direct storage load, flat `import-osi`, legacy `import-osi`, and export of already-stored text.
- [x] 1.6 Unevaluable source-measure tests: load warning on `inspect`, typed query error naming the metric and reason, sibling metric unaffected; transitive chain outer → inner → `AGG(orders.revenue_total)` named in the error; import report lists it; dataset-tool save adding `AGG(source.x)` refused (also with a relationship named `source`); unrelated edit keeps the warned metric's text.
- [x] 1.7 Portable-SQL writer tests: `INTERVAL` respelled as `DATEADD` for a saved field, a dataset metric, a model-scoped metric `FILTER`, a legacy import upgrade, and export of a standalone dataset stored with `INTERVAL`; a second save is stable; values unchanged; every written text passes the native check.
- [x] 1.8 Corpus test: classify a dataset metric calling `AGG(source.…)`, and model-scoped metrics reaching one, as `unevaluable` in `expected_outcomes`; the `0c27bf637ef2.yaml` cases go green once the corpus is re-vendored (task 2.2).

## 2. Re-vendor

- [x] 2.1 Implement `scripts/ossie_schema_drift.py --vendor <checkout> [--out DIR]` with the managed file set (today's set + core-spec text + `compliance/expressions/*.txt`); verify task 1.1 passes.
- [x] 2.2 Run `--vendor` on a fresh clone of apache/ossie `main`; verify provenance names the commit, the corpus test runs on the new snapshot, and only `harvested/0c27bf637ef2.yaml` and the new tests fail before task 3. Any other new failure the snapshot surfaces is raised with the owner before continuing.

## 3. Validity and parsing

- [x] 3.1 One shared `AGG` iterator + predicate covering both sqlglot shapes; replace every enumeration and predicate use (`core/ossie_validity.py`, `storage/semantic_models.py`, `storage/model_metric_rules.py`, `osi/exporter.py`, any other caller found by LSP); verify task 1.3 passes.
- [x] 3.2 Native validity: dataset-scoped `AGG(source.measure)` rules, field-expression wording, `3719487` construct rejections, `d4dca4e` field-aggregate rejection; verify tasks 1.2 and 1.4 pass.
- [x] 3.3 One document-text parser (duplicate keys, constructor errors, merge precedence) used by `storage/semantic_models.py`, `storage/base.py`, `osi/importer.py` (incl. the legacy re-parse) and `osi/exporter.py`; verify task 1.5 passes.

## 4. Load, save, import

- [x] 4.1 Load warning for dataset metrics calling `AGG(source.…)`, transitive and cycle-safe propagation through model-scoped metrics, typed query error with the chain; verify task 1.6's load and query cases pass.
- [x] 4.2 Save-time refusal for SLayer-authored `AGG(source.x)` dataset metrics (always the source reading); unchanged warned entities survive document saves; verify task 1.6's save cases pass.
- [x] 4.3 `import-osi` reports source-measure metrics as not evaluable yet; verify task 1.6's import case passes.

## 5. Portable SQL writer

- [x] 5.1 Inventory every path writing `OSSIE_SQL_2026` (dataset tools, `edit_semantic_model`, model-metric saves, import upgrade, exporter) and route them through one canonical writer that respells `INTERVAL` as `DATEADD`; verify task 1.7 passes.
- [x] 5.2 Apply the approved engine principle 8 wording (design.md decision 8) to `architecture/python.engine.arc42.md`; verify `poetry run la-arch-check` passes.

## 6. Docs

- [x] 6.1 Ossie pages and the model-scoped-metrics page state where `AGG(…)` is allowed (`AGG(dataset.metric)` in model-scoped metrics; `AGG(source.measure)` in dataset-scoped metrics, accepted but not evaluable yet); `docs/concepts/references.md` notes stored `INTERVAL` is written as `DATEADD`; agent help text on `AGG` placement (`slayer/memories/help_content/`) updated; verify every touched page is in the `zensical.toml` nav.

## 7. Gates

- [x] 7.1 Local test scope green (`la-affected-tests --base main` plus the corpus, oracle, grader and drift tests); `poetry run ruff check slayer/ tests/`, `la-typecheck` ratchet, `poetry run la-arch-check`, `npx -y likec4@1.47.0 validate architecture` green.
- [x] 7.2 Manual dispatch of `ossie-corpus-weekly.yml` on this branch is green.
