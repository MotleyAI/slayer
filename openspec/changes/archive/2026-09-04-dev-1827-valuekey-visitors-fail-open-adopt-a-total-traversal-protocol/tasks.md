# Tasks

## 1. Protocol & registry (core/keys.py)

- [x] 1.1 Add raising `children()`/`map_children()` defaults on `_FrozenKey` and per-kind
      overrides on all 11 union members + `SqlExprKey` per design decisions 1–3; verify
      via protocol totality test (every member overrides both; `map_children(identity) is
      key`; `NotImplementedError` message names the protocol).
- [x] 1.2 Add coherence test: per kind, on a fully-populated fixture, `fn` invocations
      recorded as a list match `children()` positionally by identity, and each mapped
      result (including an equal-but-distinct replacement) lands in the corresponding
      field; verify test passes.
- [x] 1.3 Add `KindPolicy` (frozen Pydantic) + `KIND_POLICY` with `slottable` /
      `slot_composite` / `materialised_order` flags and derived tuples + a single
      `VALUE_KEY_TYPES` constant from `get_args(ValueKey)`; verify via registry test
      (keys == `get_args(ValueKey)`, explicit expected membership per flag).

## 2. Rewriters in core/keys.py

- [x] 2.1 Collapse `_map_value_key` composite arms onto `map_children` (keep leaf
      path-mapping arms + standalone `SqlExprKey`); verify existing dev1747/dev1842
      totality tests and goldens stay green.
- [x] 2.2 Rebase `substitute_value_keys` on pre-order match-before-recurse over
      `map_children`; verify existing dev1825 tests + a matched-key-not-descended test.

## 3. Engine walkers

- [x] 3.1 Route `walk_value_keys` (binding.py) through `children()` and drop the local
      `_VALUE_KEY_TYPES`; audit every call site for the B1 widening
      (TimeTruncKey.column); verify focused assertions that the wrapped raw column is not
      auto-added/materialised (slot deps, filter phase, HAVING, join discovery) plus
      goldens.
- [x] 3.2 Rebase `lower_sugar_transforms` on post-order `map_children` per design
      decision 5, fixing the InKey-under-ScalarCall omission; verify regression test that
      fails on pre-change code + no-op tests for transforms nested in aggregate sources /
      partition keys / time keys.
- [x] 3.3 Rebase `rewrite_rank_partition_keys` on post-order `map_children` with
      `rewrite_fn` receiving the pre-rebuild node; verify existing rank tests + identity-
      preservation test.
- [x] 3.4 Give `_iter_slot_deps` a documented-asymmetry comment + fail-closed raise tail,
      deriving `_SLOTTABLE_KIND` from the registry; verify dummy-kind raise test +
      existing planning tests.
- [x] 3.5 Migrate join-path discovery (`aggregate_input_paths.py`,
      `column_filter_paths.py`) and the HAVING validation walk (`stage_planner.py`) onto
      `children()`; verify existing suite + dummy-kind traversal test.

## 4. Generator & renderer

- [x] 4.1 Route `_collect_base_aux_slot_ids` recursion through `children()` (keep its
      documented top-level passes); replace `composite_kinds` and
      `_MATERIALISED_ORDER_KINDS` with registry-derived tuples; verify goldens
      byte-identical.
- [x] 4.2 Route `contains_aggregate` through `children()` (keep the AggregateKey
      short-circuit), drop the local `_VALUE_KEY_TYPES`, and make renderer dispatch raise
      on an unhandled `_FrozenKey` subclass while scalars still render; verify dummy-kind
      raise tests + goldens.

## 5. Audit sweep & dummy-member test

- [x] 5.1 Sweep every `isinstance(...Key)` chain in the impacted files (proposal Impact
      list) and classify each as migrated / asymmetric-documented / policy-derived /
      raise-tail; verify by a short classification table in the PR description and no
      remaining silent fall-through on the dummy kind.
- [x] 5.2 Add the dummy-member test: `DummyKey(_FrozenKey)` implementing the protocol
      flows through every generic visitor; `DummyOpaqueKey` without overrides makes each
      generic visitor raise `NotImplementedError`; kind-dispatch visitors
      (`_iter_slot_deps`, renderer) raise on both; verify test passes.

## 6. Verification & docs

- [x] 6.1 Run the full non-integration suite + goldens byte-identical + `ruff check`;
      verify all green.
- [x] 6.2 Add a concise traversal-protocol note to the relevant `docs/architecture/` page
      (linked in `zensical.toml` nav already, no new page); verify page renders and no
      Linear refs in docstrings.
