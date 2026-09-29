## 1. Tests (pr-tests)

- [ ] 1.1 Add a fixture module reusing the DEV-1840 / DEV-1910 seeds (append rows post-seed, never modify shared constants): a dangling-FK order, a partially-NULL composite-FK order plus an unreferenced store, a NULL-name region owning a customer with orders; verify each new oracle is hand-computed in a comment-free constant
- [ ] 1.2 `tests/test_dev1995_virtual_model.py` — orphan NULL cell 47 / 2 (sum + count) for customers by `regions.name` under broadcast, associate, error and inferred population, SQLite + DuckDB; verify green before and after the fix (pins the by-design value)
- [ ] 1.3 Parity with the materialised aggregate (`create_model_from_query`, both root spellings), every cell incl. NULL, matched null-safely in Python plus an explicit NULL-cell assert; single-hop, multi-hop (regions by name), dangling and partial-composite shapes; verify green
- [ ] 1.4 Spelling-invariant association: orders-rooted and customers-rooted `customers.spend:sum` by `status` under associate on the null-status seed both give NULL cell 155; derived `customers.last_status` gives 155; explicit `partition_by=status`, filter-only and order-only uses agree; verify RED today (100)
- [ ] 1.5 Filtered association: orders-rooted associate by `status` with `channel = 'app'`, orderless customer excluded, hand oracle; verify value before/after is recorded in the test
- [ ] 1.6 Rewrite DEV-1910 guard tests (consented): `TestPresenceGuardOnTheBackHop` and `TestDerivedDimensionCrossingBackIsGuarded` assert 155 with renamed intent; collapse `PRESENCE_NULL_CELL*` in `tests/_dev1910_fixtures.py`; delete `TestPresenceKeys` in `tests/test_dev1910_association_plan.py` and drop `present_keys` from its round-trip; update the module docstrings, the mixed-dimension test wording and fixture comments; verify they fail today
- [ ] 1.7 Codex-review the tests against this change's specs

## 2. Implementation (pr-implement)

- [ ] 2.1 Delete `_association_present_keys` and its plumbing in `_association_arm` / `_synthesize_cross_model_producer` (`slayer/engine/compile/stages.py`); verify 1.4 passes
- [ ] 2.2 Delete `AssociationProducerKernel.present_keys` (`slayer/ir/planned.py`) and the level-1 guard in `slayer/sql/generator.py`; verify no reference remains (LSP find-references)
- [ ] 2.3 Re-bless the association goldens (dev1841, dev1859, dev1892, dev1902, dev1908); verify each diff only removes the `NOT … IS NULL` guard
- [ ] 2.4 Apply the approved arc42 edits: Axiom 6 virtual-model clause with `[enforced: test:tests/test_dev1995_virtual_model.py]`, Axiom 2.3 "(the virtual-model join of Axiom 6)"; verify `poetry run python tools/arch_check.py` passes
- [ ] 2.5 Replace the presence parenthetical at `docs/concepts/queries.md:740` with one sentence (an aggregate reads like a field of a model keyed by its grain, so an entity with no related row counts in the NULL cell); verify no doc still states the presence rule
- [ ] 2.6 Run the full unit suite, ruff, and the conventions / basedpyright gates; verify all green

## 3. Review (pr-review)

- [ ] 3.1 Process reviews until every source is green, then archive this change with explicit permission
