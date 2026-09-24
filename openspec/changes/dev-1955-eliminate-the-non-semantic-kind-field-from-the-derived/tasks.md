## 1. Tests (spec-tests stage)

- [x] 1.1 New `tests/test_dev1955_kind_free_vocabulary.py`: neither error accepts a `kind` kwarg nor exposes `.kind`; `DerivedColumnCircularError` rejects a `model` kwarg and `.model == .root_model`; `DerivedColumnFanningError` requires `reference`; exact new opening clauses of both messages and the unproven warning, with no `sql reference` / `filter reference` label — verify each fails before implementation
- [x] 1.2 Same file: engine-door cross-model transitive attribution (`customers.bad` stored `_validate=False` with `filter` `regions.customers.spend > 0`; `orders.outer` `sql` `customers.bad * 2` saved via `SlayerQueryEngine.save_model`) → `column == "bad"`, `model == "customers"`, message says `not a column of 'customers'` — verify it fails today (`model == "orders"`)
- [x] 1.3 Same file: a column whose `sql` and `filter` both cross the same unproven hop warns exactly once (storage save) — verify it fails today (two warnings)
- [x] 1.4 Same file: host-prefixed fanning `orders.line_items.qty` → `reference == "orders.line_items.qty"` and remedy `orders.line_items.qty:<aggregation>` in the message; and `test_host_prefixed_reference_rejected` in `tests/test_dev1930_save_time_arity.py` gains the `reference` assertion (addition, user-approved)
- [x] 1.5 User-approved edits to existing tests: `tests/test_dev1952_derived_revisit.py` — drop `model=`/`kind=` constructor args and every `.kind` assertion, message `"sql reference 'regions.customers.spend'"` → `"references 'regions.customers.spend'"`, keep `.model` assertions; `tests/test_dev1930_save_time_arity.py:147,178` and `tests/test_dev1853_traversal_execution.py:254` — replace the `.kind` assertion with an assertion on `.reference` (the offending spelling)

## 2. Implementation (spec-implement stage)

- [ ] 2.1 `slayer/core/errors.py`: `DerivedColumnFanningError` drops `kind`, `reference: str` required, new message; `DerivedColumnCircularError` drops `kind`/`model` params, `model` read-only property → `root_model`, new message; concise docstrings — verify 1.1 passes
- [ ] 2.2 `slayer/engine/column_dependency.py`: `_arity_reference_sources` → `list[str]`; `_iter_arity_refs` yields `(column, hop_path, leaf, quals)`; `_check_reference_arity` drops `kind`, fanning `reference` from `(*quals, leaf)`, warning dedup `(column, hop)`; `_unproven_arity_message(column, model, hop)` — verify 1.3/1.4 pass
- [ ] 2.3 `slayer/engine/query_engine.py::_validate_mode_a_join_paths`: loop over `(col.sql, col.filter)`; raise with `root_model=exc.root_model`, no `kind`/`model` — verify 1.2 passes
- [ ] 2.4 Grep `slayer/`, `docs/`, `tests/` for residual `kind=` on these errors and for `sql reference` / `filter reference` message text — verify none remain
- [ ] 2.5 Full unit suite (`poetry run pytest -m "not integration"`), `poetry run ruff check slayer/ tests/`, basedpyright baseline not grown — all green
