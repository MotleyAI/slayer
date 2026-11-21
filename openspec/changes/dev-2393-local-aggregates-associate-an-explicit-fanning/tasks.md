## 1. Tests (pr-tests)

- [ ] 1.1 New suite `tests/test_dev2393_partition_key_mode_gate.py`: executed values on SQLite and DuckDB for every scenario in `specs/queries/attribution-modes` (ADDED requirement) and `specs/queries/cross-model-aggregates` (the local scenarios, the fanning key with a fanning window axis, the local time key, bucket spanning on the `make_same_row_engine` fixture). Fails on main.
- [ ] 1.2 Multi-key, same-branch and separate-branch grains (decision 15): oracles from the reference data, including NULL relationships; the first offending key named under broadcast/error. Fails on main.
- [ ] 1.3 Representation parity (decision 16): keyless home (the `RANKED` keyless shape as an executed oracle), list child (`partition_by=contractors.skills.value`), saved measure, inner/outer stage. Default executes, `"error"` refuses, siblings unchanged. Fails on main.
- [ ] 1.4 Unanalysable keys fail closed in every mode, direct and chained, in measure, transform partition set and attached-parameter positions, naming no hop. Already passes on main (regression guard).
- [ ] 1.5 Transform partition set (decision 12): `rank(sum(pop), partition_by=bad_pop)` by `bad_pop` executes; a transform key outside its operand grain still fails. Re-aggregation inner `avg(sum(pop, partition_by=bad_pop))` executes.
- [ ] 1.6 `time_shift` leaf scenario (`specs/queries/transforms`): ok Jan NULL, Feb 100, Mar 150, Apr 60; new Jan NULL, Feb 100, Mar 150; NULL Apr NULL; refused under broadcast/error.
- [ ] 1.7 Attached parameter (`specs/queries/partitioned-aggregates`): `OWN_FAN_BADPOP` by `status` and by `customers.regions.bad_pop` raises the determination error naming `weight` under default/associate and `PartitionKeyError` under broadcast/error. The naive probe returned values (new 22.5, ok 12.5) for the by-status default, so it must NOT.
- [ ] 1.8 Exact-message tests (decision 8): local, cross-model and re-aggregation-outer refusals. No "cross-dataset" for a local aggregate, both remedies named, no issue reference.
- [ ] 1.9 Align existing tests that pin the every-mode refusal (ask per file before editing): `tests/test_dev1911_fanning_partition_key.py`, `tests/test_dev1836_unsafe_inputs.py::TestUnsafeExplicitPartitionKeys`, `tests/test_dev1914_association_parameters.py::test_keyless_home_partition_key_error`, `tests/test_dev1919_home_rooted_attached_inputs.py::test_parameter_key_fanning_from_its_own_home`, and any test pinning the replaced transforms / partitioned-aggregates scenarios, plus the `tests/_dev1871_raise_ledger.py` rows.

## 2. Implementation (pr-implement)

- [ ] 2.1 Three-way partition-key analysis (decision 11) in `relationship_safety.py`; `bind_inputs.py::_validate_partition_keys` fails only on unanalysable and no longer judges rank-family transform keys (decision 12). Verify with 1.4 and 1.5.
- [ ] 2.2 `discovery.py::_Walker.local_broadcasts`: route a row-level local aggregate with an unattributable explicit key `target_rooted`, excluding second-order outers (decision 13). Verify 1.1 plus the full re-aggregation suite green (`tests/test_reagg_outer_grain_attribution.py` broke under the naive probe).
- [ ] 2.3 `stages.py` mode gate: the association arm takes local explicit keys without a warning; the broadcast/error arm raises the shared checker. Verify 1.1 to 1.3, 1.6.
- [ ] 2.4 `elaborate_env.py`: generalise `check_cross_dataset_partition_keys_attributable` into the shared ordinary-producer checker (wording and suggestion; rename via `la:deterministic-refactor` if referenced from more than one file); delete `check_partition_key_attributable` or narrow it to the unanalysable case; update the raise ledger. Verify 1.8.
- [ ] 2.5 Enclosing determination precedence for attached parameters. Verify 1.7.
- [ ] 2.6 `architecture/semantics.arc42.md` Axiom 8: apply the decision-4 wording verbatim. Verify `poetry run la-arch-check` is green.
- [ ] 2.7 Docs, `docs/concepts/queries.md`: the association section states that an explicit `partition_by=` key resolves by the same mode, local or cross-dataset; correct "Cross-dataset aggregates also support … `partition_by=` … (broadcast, as above …)". Verify `tests/test_doc_examples.py` passes.
- [ ] 2.8 Gates: tests in local scope, `poetry run ruff check slayer/ tests/`, `la-typecheck` with no new errors, `la-arch-check`, LikeC4 validation. All green.

## 3. Close-out

- [ ] 3.1 PR body lists DEV-2393 and DEV-1941 as closing references. Verify in the PR description.
- [ ] 3.2 After archive, propose (owner approval) retitling the two scenarios whose titles a MODIFIED delta cannot rename: `queries/partitioned-aggregates` "Attached parameter whose own partition key fans from its own home fails closed" and `queries/transforms` "Partition key across a fanning hop stays a typed error". Their bodies already state the per-mode behaviour. Verify `openspec validate --specs --strict` passes after the edit.
