# Tasks — Grain as a first-class type

## 1. Tests first (TDD — the suite from the spec-tests stage)

- [x] 1.1 Write `tests/test_dev1867_grain.py` covering: union incl. `EMPTY` identity;
  `is_subgrain_of` reflexive / `is_strict_subgrain_of` irreflexive; `broadcasts_into`
  both directions plus equal-grain; membership / iteration / `len` / `bool` /
  `is_empty`; hash-equality and dict-key use; non-equality with a raw frozenset;
  mixed-operand `TypeError` for `grain <= frozenset(...)` and reflected form;
  `__or__` / `__sub__` with `Grain` and raw sets, results `type(...) is Grain`;
  constructor coercion (list / set / generator), dedup, rejection of non-`ValueKey`
  members, frozen mutation failure; `Grain.of([]) == Grain.EMPTY` (equality, not
  identity); operator/named-method agreement. Verify: file fails on import (no
  `slayer/core/grain.py` yet), passes after task 2.1.
- [x] 1.2 Add direct tests for `regroup_root_grain`: bare partitioned root → own grain,
  transform root → union of all inner partition grains, unpartitioned root → empty;
  returns `Grain`. Verify: fail before task 3, pass after.
- [x] 1.3 Check existing DEV-1839 coverage of `_validate_nested_producer_plan`; add
  focused tests for uncovered branches (equal-grain windowed admitted, equal-grain
  non-windowed rejected, strict subgrain admitted, supergrain rejected) and an
  exact-string assertion on the admission message if no test pins it. Verify: new
  branch tests pass after task 3 with messages byte-identical.
- [x] 1.4 Add type-preservation tests for `_prune_functionally_determined_grain`
  (`Grain` in → `Grain` out, input unchanged) in `tests/test_dev1867_grain.py`, and
  wrap the two raw-frozenset args in `tests/test_dev1835_grain_prune.py` in
  `Grain.of(...)` (assertions untouched). Verify: both files pass after task 3.

## 2. The Grain type

- [x] 2.1 Implement `slayer/core/grain.py` per design.md decisions 2–5 (frozen Pydantic
  model, `keys: frozenset[ValueKey]`, `of` / `EMPTY` / `union` / `__or__` / `__sub__` /
  `is_subgrain_of` / `is_strict_subgrain_of` / `broadcasts_into` / set protocol /
  Grain-only `__eq__` + `__hash__`; module docstring states the lattice reading and
  operator-vs-predicate convention). Verify: `tests/test_dev1867_grain.py` green.

## 3. Planner conversion (pure refactor)

- [x] 3.1 `regroup_planner.py`: `regroup_root_grain` returns `Grain`. Verify: 1.2 green.
- [x] 3.2 `stage_planner.py`: convert `_effective_root_grain` (incl. 1256–1263
  union/difference), `_root_grain`, `own_grain`, equality filters (2583/2585),
  `_validate_nested_producer_plan` (named predicates at 1090, messages unchanged),
  `_prune_functionally_determined_grain`, producer grouping `gkey`/`group_meta`,
  ordering entry points (`_regroup_partition_order` / `_combined_order` /
  `_regroup_producer_prebound(pks=…)`), wrap-producer construction (1795–1798), and
  `grain_keys` (2354) including all its uses; temporal-axis guard membership via
  `__contains__`. Boundary sites from design.md decision 6 stay frozenset. Verify:
  full unit suite green.

## 4. Verification gates

- [x] 4.1 `poetry run pytest -m "not integration"` fully green with golden SQL
  byte-identical (no baseline re-blessing; verify `git status` shows no
  `tests/golden/` changes).
- [x] 4.2 Enforcement bundle green: `poetry run lint-imports`,
  `poetry run python tools/arch_check.py`, `npx -y likec4@1.47.0 validate
  architecture`, `poetry run basedpyright` (no new errors vs baseline),
  `poetry run ruff check slayer/ tests/`.
