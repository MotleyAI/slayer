## 1. Error vocabulary

- [ ] 1.1 Add `CircularJoinPathError(SlayerError, ValueError)` (fields `reference`, `root_model`, `revisited`, `hop`, `via`, optional `column`) and `DerivedColumnCircularError(CircularJoinPathError)` (adds `column`, `model`, `kind`; remedy message) to `slayer/core/errors.py` per design; verify the isinstance/field/message pins in `tests/test_dev1952_derived_revisit.py` and the extended `tests/test_errors_hierarchy.py` pass.

## 2. Walker and best-effort consumers (no behaviour change)

- [ ] 2.1 `slayer/core/join_walker.py`: `walk` raises `CircularJoinPathError` on a revisit (reference = the dotted tokens, root, revisited, hop, via) and returns `None` only on an unknown/unloaded hop; `terminal_model` catches → `None`; docstrings updated; verify the walker unit tests (raise with all fields; `None` on an unknown token; ambiguity unchanged; self-join first hop raises — built via `SlayerModel.model_construct`, since a real self-join is rejected at model construction so save/query paths never see one) and the flipped `tests/test_dev1853_join_walker.py::test_revisit_is_guarded`.
- [ ] 2.2 `slayer/engine/join_safety.py`: `safe_reachable` → `False`, `_back_path` → `(host_name,)`, `_hop_walk_reason` → `None`, `_path_grain_determined` → `False` on the raise; verify the via-host reroot regression test (`attributable_from_root` / `reroot_from_root` on a composed revisiting path still broadcast) and `tests/test_dev1900_closure.py`, `tests/test_dev1910_home_rooted_association.py`, `tests/test_dev1911_fanning_partition_key.py` unchanged.
- [ ] 2.3 `slayer/engine/compile/stages.py`: `_canonical_path` → `tuple(path)`; `_reverse_hops` sets `fwd = None` in the `except` so its `_PushBlocked` raise stays one site; verify `tests/test_dev1871_raise_parity.py` (ledger unchanged) and `tests/test_dev1909_population_pushdown.py` pass.
- [ ] 2.4 `slayer/sql/column_expansion.py`: `resolve_ref_target`, `_lenient_path`, `resolve_default_qualifier_path` catch → `None`; verify the definition-default regression tests (scalar and expression default in the DEV-1900 back-hop shape; a root-spelled default probed from the owner frame resolves from the root as before) and `tests/test_dev1931_default_home.py`, `tests/test_dev1892_parameter_typing.py` unchanged.

## 3. Expansion door and closure (query-time backstop)

- [ ] 3.1 `slayer/sql/column_expansion.py`: `_walk_exact` propagates; `_resolve_qualifiers` re-raises with the complete pre-strip reference, `root_model`, and the optional `column` threaded from `_process_reference_site`'s innermost `visited` entry; verify the door unit test (reference keeps the leading host qualifier; `column` set inside a derived chain).
- [ ] 3.2 `slayer/engine/reference_closure.py`: `fragment_closure`, `fragment_null_propagates`, `_expand_derived_refs_any_dialect` re-raise `CircularJoinPathError` next to `ColumnCycleError`; verify every query-time scenario of the delta spec (aggregate from `orders` and from `customers`, dimension, filter, raw rows, derived chain, filter-kind column; sqlite + duckdb) raises the circular error before SQL, never "no supported dialect can analyse", a `no such column`, or a value.

## 4. Save-time gates (one error from both doors)

- [ ] 4.1 `slayer/engine/column_dependency.py`: thread the raw qualifiers through `_iter_arity_refs`; `_classify_hop_path` / `_check_reference_arity` raise `DerivedColumnCircularError` from the caught walker error with the complete reference; verify the storage-save scenarios (sql and filter revisit; revisit declared on `orders`; leading declaring-model qualifier; to-one round trip; precedence over an earlier fanning hop; known-but-unloaded intermediate skips; `_validate=False` still saves; ambiguous still skips).
- [ ] 4.2 `slayer/engine/query_engine.py::_validate_mode_a_join_paths`: iterate `(column, kind, fragment)` per column surface plus model filters; convert a caught `CircularJoinPathError` on a column surface into the same `DerivedColumnCircularError`, propagate it for a model filter; verify the engine-save scenario asserts identical fields and message to the storage-save error.

## 5. Binder parity

- [ ] 5.1 `slayer/engine/binding.py`: the query-typed circular raise becomes `CircularJoinPathError` with the binder's own root/hop/via/revisited; verify the parity test (isinstance + message substrings) and `tests/test_dev1780_missing_join_path.py`, `tests/test_dev1450fix_group2_correctness.py`, `tests/test_dev1853_pushdown.py`, `tests/test_dev1842_round_trip.py` unchanged.

## 6. Normative harness and docs (arc42 edit approved)

- [ ] 6.1 Apply the approved Axiom 1 clause and `[enforced: test:tests/test_dev1952_derived_revisit.py]` tag in `architecture/semantics.arc42.md`; verify `poetry run python tools/arch_check.py` and `npx -y likec4@1.47.0 validate architecture` pass.
- [ ] 6.2 Append the one approved sentence to the derived-columns paragraph in `docs/concepts/models.md`; verify no new page (nav in `zensical.toml` unchanged).

## 7. Tests

- [ ] 7.1 Write `tests/test_dev1952_derived_revisit.py` (unit only; sqlite + duckdb params for the query-time block) covering every scenario in the delta spec plus the regression guards (via-host reroot; definition defaults; ambiguity; `_hop_walk_reason`/`terminal_model` unchanged); verify each new pin fails without the fix by a revert check.
- [ ] 7.2 Consent-covered edits: flip `test_revisit_is_guarded` to `pytest.raises(CircularJoinPathError)`; add both classes to `tests/test_errors_hierarchy.py`; verify both files pass.

## 8. Gate

- [ ] 8.1 `openspec validate dev-1952-reject-a-derived-column-whose-columnsql-revisits-a-model-on --strict` passes.
- [ ] 8.2 `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run python tools/arch_check.py` pass and `poetry run basedpyright` shows no new errors vs baseline.
