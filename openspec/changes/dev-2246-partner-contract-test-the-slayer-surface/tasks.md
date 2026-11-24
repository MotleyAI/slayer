## 1. Tests first (pr-tests)

- [ ] 1.1 Build the v15 fixture store `tests/fixtures/integrator_1x/models/warehouse/{orders,customers}.yaml`, plus fixture data for DuckDB/Postgres. The store has two models, a many_to_one join, saved measures (a sibling ratio `aov = revenue / order_count`, a filtered-column measure, a joined `count_distinct`, a custom aggregation), a filtered column, an expression column (`UPPER(status)`), an ARRAY `tags` column, a model filter and a TIMESTAMP default time dimension. Verify: `SlayerModel.model_validate` loads each file on `main`.
- [ ] 1.2 Compute the 1.8.4 expected values by running each call shape from design.md › The integrator's call set against `motley-slayer==1.8.4` (`uv run --no-project --with "motley-slayer[all]==1.8.4" python -I <probe>`). Write them into the tests as literals; the probe is not committed.
- [ ] 1.3 Write `tests/test_integrator_1x_calls.py`, one test per scenario of `specs/compatibility/integrator-1x-calls`. Each asserts that the call works, the exact warning (kind, removal 3.0, named replacement), and the 1.8.4 value. Verify: every test fails on `main` for the right reason, or passes where the spec says the form already works.
- [ ] 1.4 Write the dry-run query-set tests: render on 5 dialects, execute on in-process DuckDB and Postgres (pytest-postgresql), check result column names, check the Redshift list refusal. Use the read-only backend written against the 1.8.4 hook names. Verify: the time-dimension and order queries fail on `main` at construction.
- [ ] 1.5 Write the upgrade tests from `specs/models/stored-upgrade` (load never writes, load warning, `slayer storage upgrade`, pure entry point) and `specs/cli/commands`. Include legacy-cycle and fanning-reference fixtures, the corrupt-document isolation case, the concurrent-edit conflict, the interrupted settle and the missing-dialect case. Verify: they fail on `main`.
- [ ] 1.6 Write `tests/test_law_load_never_writes.py`. It compares logical contents (YAML document bytes; SQLite rows via a read-only snapshot) of the 0.10.2 and 1.8.2 corpora plus repair/settle fixtures after loading everything twice, spies the write hooks, and tests the pre-v4 layout move separately. Verify: it fails on `main`.
- [ ] 1.7 Write the drift tests from `specs/models/schema-drift`: filtered alias, re-type, quoted physical name with two fields, metric cascade, relationship-key refusal unchanged. Verify: the drop tests fail on `main`.
- [ ] 1.8 Write the partial-shim tripwire tests from `specs/compatibility/removal-tripwire`: module import silent, each listed name warns once, an unlisted name raises the moved error.
- [ ] 1.9 Find the existing tests that contradict the plan, for example tests pinning write-back on load ("written back at the current version", re-saved settle documents) and the four modules' `ImportError` in the moved-module coverage. Also the tripwire scanner's one-site-per-module pin. List each file and the change, and get the user's approval before aligning them.
- [ ] 1.10 Codex review of the tests against this plan.

## 2. Deprecated forms (pr-implement)

- [ ] 2.1 Partial shims for `slayer.core.formula`, `slayer.engine.syntax`, `slayer.engine.schema_drift` and `slayer.sql.column_expansion`. Each is a module `__getattr__` serving only the listed names through the shared deprecation helper, with the moved `ImportError` for everything else. Extend the tripwire's pinned features (one per name plus the fallback). Verify: 1.8 and the tripwire law pass.
- [ ] 2.2 1.x formula adapter over `core.syntax.parse_expr`: result classes, `named_measures` expansion, `extra_agg_names`, `ValueError`/`RecursionError`, colon refusal. Verify: the formula tests in 1.3 pass, as do `tests/test_dev2172_one_mode_b_reader.py` and `tests/test_dev2187_one_reader_law.py`.
- [ ] 2.3 `parse_filter_expr` projection of the 2.0-only node classes onto their 1.8.4 shape, refusing 1.x-refused text. Verify: the filter-walk tests pass.
- [ ] 2.4 `has_variable_syntax`, `is_base_column_sql`, and the old keywords of `is_identifier` and `resolve_ref_target`. Verify: their tests pass.
- [ ] 2.5 Drift: old keywords on `diff_sql_table_dataset` (as `diff_sql_table_model`), `compute_datasource_drops` and `retype_findings`; deprecated result attributes (`model_name`, `columns`, `measures`, `join_edges` with 1.x dumps, `column`). Verify: the drift-call tests pass.
- [ ] 2.6 `DataType` legacy names through a metaclass lookup. Verify: the identity, iteration, `__members__`, pydantic validation/schema, pickle and one-warning tests pass.
- [ ] 2.7 `TimeDimension` / `OrderItem` accept a column-reference object under `dimension=` / `column=`. Verify: 1.4 passes on all dialects.

## 3. Loads never write, and the upgrade (pr-implement)

- [ ] 3.1 Extract the upgrade service: a snapshot context in, upgraded documents plus diagnostics plus a "changes this document" flag out. It covers datasets, semantic-model documents, saved queries, memories, datasources and the settle's rename map. Verify: the existing stored-upgrade suite passes through it.
- [ ] 3.2 Route every load path (YAML, SQLite, the base pipeline) through the service without writing. Remove the write-back, the settle's persistence and the peer-repair save from load. Add the once-per-document-per-instance load warning. Verify: 1.6 and the load-warning tests pass.
- [ ] 3.3 The upgrade write path: migration-write semantics (no client-save semantic validation), conditional on an unchanged stored form, dependents before datasources. Verify: the conflict, legacy-cycle and interrupted-settle tests pass.
- [ ] 3.4 `slayer storage upgrade` CLI (`--data-source`, `--dry-run` diff, `--datasource-type NAME=TYPE`, `--storage`, per-document lines, exit codes). Verify: the CLI tests pass.
- [ ] 3.5 The pure entry point over the service. Verify: the pure-upgrade tests pass, as does the upgraded-files version and key-walk test in 1.3.

## 4. Drift fix (pr-implement)

- [ ] 4.1 Add the "reads a physical column" predicate. Move every drift and cascade use of `is_physical_reader` onto it, and drop "drift" from `is_physical_reader`'s docstring. Verify: 1.7 passes, and the drift drop set equals 1.8.4's in 1.3.

## 5. Architecture, docs, wrap-up (pr-implement)

- [ ] 5.1 Apply the approved arc42 edit to `architecture/python.storage.arc42.md` 3.1 verbatim (decisions log 18). Verify: `poetry run la-arch-check` passes, as do `npx likec4 validate` and `la-typecheck` (no new errors versus the baseline).
- [ ] 5.2 `docs/upgrading/2.0.md`: a section "Integrators reading SLayer objects" (the deprecated call forms with their replacements; the representation changes from design.md › Non-Goals; `slayer storage upgrade`), plus a Part 2 entry for each new deprecated form. `docs/reference/cli.md`: `storage upgrade`. `docs/configuration/storage.md`: loads never write, and how to upgrade. Verify: the pages are in the `zensical.toml` nav, and the doc-example checks pass.
- [ ] 5.3 Run the local test scope (`la-affected-tests --base main` plus the stored-upgrade, tripwire and drift suites), then `poetry run ruff check slayer/ tests/`. Verify: both are green.
- [ ] 5.4 Tracker: the PR description says it closes the load-write-back issue (storage 3.1's target issue). After merge, unblock the 1.x back-port issue.
