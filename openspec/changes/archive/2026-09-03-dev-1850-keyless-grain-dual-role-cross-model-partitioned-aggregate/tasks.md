# Tasks: dev-1850-keyless-grain-dual-role-cross-model-partitioned-aggregate

## 1. Tests first (TDD — land failing for the right reason)

- [x] 1.1 Create `tests/test_dev1850_keyless_grain.py` on `tests/_dev1838_fixtures.py` with clean-error pins parametrized over paired (local, cross-model) formulas — dual-role measure, raw ORDER BY target, composite measure operand, transform-input measure — each asserting ValueError with the partition-key name and remedy; verify the cross-model cases fail today with the internal RuntimeError (feature missing) while the local twins pass.
- [x] 1.2 Add execution tests (sqlite + duckdb): keyless filter over the dimension's own cross-model aggregate (executed-value pins mirroring the local row-routing semantics; exactly one `_cm_` producer, no combined twin) and keyless ORDER-BY-name (row-order pins; one `_cm_` producer); verify both fail today with the RuntimeError.
- [x] 1.3 Check whether the already-clean measure-only / filter-only cross-model keyless errors are pinned anywhere; add cheap pins to the 1850 file if not. Verify with a targeted grep + run.
- [x] 1.4 Remove `test_dual_role_without_partition_key_in_grain_unsupported` from `tests/test_dev1838_interning.py` (replaced by 1.1; consented via the issue and review) and verify that file still passes except for nothing (no other test touches the shape).

## 2. Unified discovery + validation

- [x] 2.1 Generalize `combined_partitioned_aggregates` (`slayer/engine/regroup_planner.py`) into the unified combined-consumer discovery with explicit buckets (local partitioned / cross-model partitioned / cross-model bare / alias map / declared-type map), row-routing exclusions applied only to partitioned aggregates with a row role; verify with the 1.1/1.2 pins and the existing local suites.
- [x] 2.2 Retire `_discover_cross_model_combined` (`slayer/engine/stage_planner.py`) into the unified walk; `_plan_regroups` consumes the buckets, preserving the `local_discovery=False` producer-recursion contract (local buckets empty, cross-model on); verify `tests/test_dev1836_total_routing.py` and the windowed/nested suites pass.
- [x] 2.3 Union the cross-model partitioned bucket into `_combined_consumer_keys` at bind time; verify the 1.1 error pins go green with unchanged message wording.
- [x] 2.4 Re-point the `test_dev1836_total_routing.py` blinding monkeypatches at the unified discovery seam (blind the cross-model buckets), keeping the same invariant assertions; verify those tests pass and still fail when the invariant is broken (spot-check by reverting the seam locally).
- [x] 2.5 Pin the D4 contract (Codex confirmed unpinned): nested cross-model discovery inside a producer sub-plan with `local_discovery=False` — local buckets empty, cross-model bucket still populated. Land it against the unified seam once it exists (a plan-time pin in the 1850 file, or a strengthened `test_dev1836_total_routing.py` blinding assertion). Verify it fails when the contract is broken.

## 3. Suite, docs, spec hygiene

- [x] 3.1 Run the full non-integration suite (`poetry run pytest -m "not integration"`) and fix any failure; confirm zero golden SQL diffs (any diff is investigated, not re-blessed). — 14754 passed, 100 skipped, 0 failed; no golden diffs.
- [x] 3.2 Update `docs/concepts/formulas.md` partition_by wording (combined positions error uniformly; dim-role filter / ORDER-BY-name references stay legal keyless) and grep docs/ + .claude/skills/ for other statements of the rule; verify with a docs grep.
- [x] 3.3 Run `poetry run ruff check slayer/ tests/` and `openspec validate dev-1850-keyless-grain-dual-role-cross-model-partitioned-aggregate --strict`; both clean.
