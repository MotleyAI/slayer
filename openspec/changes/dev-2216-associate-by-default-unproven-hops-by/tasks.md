## 1. Tests first (pr-tests)

- [x] 1.1 `tests/test_dev2216_associate_default.py`: every scenario of "Mode selection and
  the association default", "Broadcast fallback for association-ineligible aggregates",
  "Ungrained re-aggregation inners resolve per the mode" and the row-supply scenario, plus
  default-mode parity for local ranked aggregates (the `local_broadcasts` early return),
  product/unrelated datasets, list children, explicit cross-model `partition_by=`,
  aggregation parameters (no broadcast-legal shape leaves a row-valued parameter undetermined
  by the entity grain, so that fallback reason has no executable case) and null-safe re-aggregation; one expression mixing two fallback reasons
  (warning merge). Verify: the file collects and fails against the unchanged code.
- [x] 1.2 `tests/test_dev2216_keyless_association.py`: Ossie Appendix B.1 fixture
  (customers keyless, `orders → customers` undeclared) — region × revenue root-less and
  rooted at `orders`, explicit broadcast; duplicate physical row + two same-status orders;
  fan-out under a joined-dataset filter; keyless `wsum` / `weighted_avg` parameters;
  explicit `associate` on a keyless home; row-number rendering for every dialect
  (emission golden). Verify: collects and fails.
- [x] 1.3 Migration tests (YAML and SQLite): v9 `"broadcast"` → omitted, `"associate"` /
  `"error"` kept, nested stages with and without `version` incl. non-final stages, fresh
  explicit `"broadcast"` kept, a mode-less save stores no mode. Verify: fail.
- [x] 1.4 Field / refinement / usage-reporting / warning-payload tests: `None` default,
  refinement inheritance, `association_unsupported` round-trip and merge identity, the
  reworded hints. Verify: fail.
- [x] 1.5 `tests/test_law_dice_slice.py`: a default-mode instance beside the associate one.
  Verify: fails.
- [x] 1.6 Align existing tests that pin the old default — ask the user's approval per file
  first, listing each file and the change: broadcast-mechanics tests get an explicit
  `to_many_handling="broadcast"`; default-defining tests (e.g.
  `test_dev1841_mode_field.py::test_default_is_broadcast`,
  `test_dev1836_producer_execution.py::test_unproven_hop_dim_broadcasts`,
  `test_dev1853_traversal_execution.py::test_undeclared_unproven_orientation_broadcasts`,
  `test_dev1841_association_errors.py::TestMissingUniqueKey`,
  `test_unrelated_datasets.py` grouped default) flip to the associated outcome.
  Verify: the full unit suite lists only the new-behaviour failures.

## 2. Mode policy and dispatch

- [ ] 2.1 `SlayerQuery.to_many_handling: Literal[...] | None = None`; drop it from
  `_NON_NULLABLE_SETTINGS`; reword the field and `measures` descriptions; REST field stays
  optional. Verify: 1.4 field tests pass.
- [ ] 2.2 Resolve the mode once at bind into a typed policy on `PreboundQuery`
  (association requested / fallback allowed / explicit mode); replace every raw-string
  comparison in `stages.py`, `discovery.py`, `elaborate_env.py` and `query_engine.py`.
  Verify: `grep` finds no `to_many_handling ==` outside the resolver; parity tests pass.
- [ ] 2.3 Classify association eligibility before `_association_arm` (window/first-last,
  undetermined row-valued parameter) into a typed reason; default → broadcast with the
  reason, explicit `associate` → typed error, explicit `partition_by=` needing association
  → typed error in both. Verify: fallback scenarios pass.
- [ ] 2.4 Route ungrained re-aggregation inners through the mode (DEV-1932 Case 2).
  Verify: re-aggregation scenarios pass.

## 3. Row identity for keyless homes

- [ ] 3.1 A row-identity entity key in the IR (`planned.py`), part of `entity_grain` and
  determining every column of its row; remove `check_association_root_unique_key`; keep
  `list_rows` untouched. Verify: keyless parameter tests pass.
- [ ] 3.2 Mint it in a subrelation of the home's filtered rows (relation door) before any
  association join, named via `naming.py`, carried through joins and filters; dialect
  `ORDER BY` hook in `dialects/` where `ROW_NUMBER()` requires it. Verify: keyless tests
  pass on SQLite/DuckDB; point-check `ROW_NUMBER` spelling live on Postgres, MySQL,
  ClickHouse, SQL Server and Trino.

## 4. Warnings and messages

- [ ] 4.1 `BroadcastGrainWarningPayload.association_unsupported`, fallback hint, reworded
  `DICE_SLICE_HINT`; `BroadcastGrainWarning` carries the reason; merge by
  `(location, measure, association_unsupported)`. Verify: 1.4 warning tests pass.
- [ ] 4.2 Error-mode remedy text; `column_dependency._unproven_arity_message` (aggregating
  fails at query time); `audit_join_safety` text (dimensions associate, inputs refused).
  Verify: the related tests pass.

## 5. Storage and reporting

- [ ] 5.1 Stored-only SlayerQuery v9 → v10 migration (bump `CURRENT_VERSIONS`), reaching
  nested `source_queries`; fix the v3 → v4 docstring. Verify: 1.3 passes.
- [ ] 5.2 Usage reporting flags any non-`None` mode. Verify: usage-reporting tests pass.

## 6. Architecture, docs, wrap-up

- [ ] 6.1 Apply the approved Axiom 8 and Law 5 wording to `architecture/semantics.arc42.md`
  verbatim (design.md decisions 12–13). Verify: `poetry run la-arch-check`.
- [ ] 6.2 Docs: `docs/concepts/queries.md` (table row, list example, refine, cross-model
  measures), `docs/concepts/formulas.md`, `docs/reference/{rest-api,mcp,usage-reporting}.md`,
  `slayer/memories/help_content/06_joins.md`, `docs/examples/16_list_columns` (re-run and
  save the notebook), `examples/comparisons/*` where they state the default.
  Verify: `grep -rn 'default.*broadcast\|broadcast.*default' docs slayer/memories` is clean.
- [ ] 6.3 Full unit suite, `ruff`, `basedpyright` (no new errors vs baseline), the
  enforcement bundle. Verify: all green.
- [ ] 6.4 At archive time, update the stale Purpose of
  `openspec/specs/queries/cross-model-aggregates/spec.md` (it still names the broadcast
  default and the retired `strict`). Verify: `openspec validate --specs --strict`.

## 7. Tracker gates

- [ ] 7.1 Merge gate: DEV-2221 (raise the Ossie "NULL when not functionally dependent"
  question upstream) is done before this PR merges.
- [ ] 7.2 After merge: superseding comments on DEV-1873 and DEV-1998 (broadcast-default
  rulings superseded; Law 5 now holds by default).
