## 1. Classifier

- [x] 1.1 Add `provably_fans(*, edge, target_model)` to `slayer/engine/join_safety.py` (False when `provably_to_one`, else declared `one_to_many`/`many_to_many`); add to `__all__`; verify a unit test covers declared-to-many→True, reverse-PK-covered→False, declared-to-many-but-covers-unique-key→False.

## 2. Error type

- [x] 2.1 Add `DerivedColumnFanningError(SlayerError, ValueError)` to `slayer/core/errors.py` carrying `column`, `model`, `hop`, `kind`, with the aggregate-or-filter remedy message; verify `tests/test_errors_hierarchy.py` (or the new arity test) asserts it is a `ValueError`.

## 3. Save-time pass

- [x] 3.1 In `slayer/engine/column_dependency.py`, add a hop-path classifier that strips the host-name prefix, `walk`s the tokens, and returns `("fanning", token)` / `("unproven", token)` / `None` (ambiguous / unresolvable / unloaded → skip); verify with a direct unit test over crafted models.
- [x] 3.2 Add `_check_reference_arity(model, reachable)` over `model.columns` (both `Column.sql` and `Column.filter` refs, reusing `_fragment_refs`): raise `DerivedColumnFanningError` on the first fanning hop; collect unique `(column, kind, hop)` and emit one `UserWarning` each; verify with unit tests for fanning-raises and unproven-warns-once (including a doubly-referenced hop).
- [x] 3.3 Rename `validate_no_column_cycles` → `validate_derived_columns`: prefetch `reachable` once, run `_check_reference_arity(model)` then the existing cycle DFS; update the caller in `slayer/storage/base.py::save_model` and rename usages in `tests/test_column_dependency.py`; verify the full `tests/test_column_dependency.py` passes.

## 4. Normative harness & docs (arc42 edit already approved)

- [x] 4.1 Append the approved clause to Axiom 1 in `architecture/semantics.arc42.md` with an `[enforced: test:tests/test_dev1930_save_time_arity.py]` tag; verify `poetry run python tools/arch_check.py` passes.
- [x] 4.2 Add one sentence to the derived-columns section of `docs/concepts/models.md` (provably-fanning target rejected at save time; unproven saves with a warning); verify the page is still linked in `zensical.toml` nav (no new page).

## 5. Tests

- [x] 5.1 Add `tests/test_dev1930_save_time_arity.py` (unit, no DB): fanning `sql` → error naming column/hop/remedy; fanning `filter` → same; unproven (reverse-PK and fully-undeclared) → `pytest.warns(UserWarning)` + save succeeds; to-one and declared-to-many-but-covers-unique-key → silent; unloaded target → skip; multi-hop paths (unproven→fanning and fanning→unproven); host-prefixed reference; join declared on the peer/target side; ambiguous/parallel edge → skip. Verify each fanning pin fails without the fix (revert-check).
- [x] 5.2 Confirm `tests/integration/test_integration_duckdb.py::TestDev1709SiblingProtection` is unchanged and still green (undeclared fixture saves with a warning; `li_qty:sum` still refused at query time).

## 6. Gate

- [x] 6.1 `openspec validate dev-1930-reject-derived-columns-and-column-filters-that-reference-a --strict` passes.
- [x] 6.2 `poetry run pytest -m "not integration"` and `poetry run ruff check slayer/ tests/` pass; `poetry run python tools/arch_check.py` and `poetry run basedpyright` show no new errors vs baseline.
