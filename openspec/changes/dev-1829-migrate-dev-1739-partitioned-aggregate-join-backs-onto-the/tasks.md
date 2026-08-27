## 1. Tests first (TDD — land before implementation)

- [x] 1.1 Confirm DEV-1739's own order-by / hidden-consumer support by reading `tests/test_dev1739_execution.py` so new tests match, not exceed, it; verify the baseline suite is green (`poetry run pytest tests/test_dev1739_*.py tests/test_dev1740_regroup_*.py tests/test_dev1825_*.py`).
- [x] 1.2 New planner test: a local partitioned measure yields `RegroupAttachPlan(attach_phase="combined")` and **no** `CrossModelAggregatePlan`; producer is a host-rooted single-SELECT; the measure slot maps to a placeholder `ColumnKey`. Fails on current tree.
- [x] 1.3 New byte-level test (F1): producer output column carries the consumer's public alias for a named measure and the canonical alias for a composite leaf, across `postgres,sqlite,duckdb,tsql,bigquery`.
- [x] 1.4 New interning test: two partitioned measures sharing one `partition_keys` set → one `_cm_` producer with two outputs; one structural aggregate under two public names.
- [x] 1.5 New guard-preservation tests (F3): each `guard/*` shape raises the identical message/type via the combined path; a mixed query (legitimate computed-dimension filter + a separately-guarded partitioned-measure filter) proves the latter still raises the exact DEV-1739 error.
- [x] 1.6 New dispatch-deferral tests (F4): combined regroup attach + a cross-model measure → `NotImplementedError`; row+combined coexistence → `NotImplementedError` (DEV-1824 ref) — pin the status-quo raise.
- [x] 1.7 New routing tests (F5): partitioned aggregate only in ORDER BY; composite only in ORDER BY; aggregate/post filter over a partitioned-aggregate composite (asserts the in-filter raise); duplicate aliases; limit/offset with hidden ordering; assert result keys / meta / warnings carry no hidden placeholder or producer columns.

## 2. Planner — discovery, desugar, guard ordering

- [x] 2.1 Generalize discovery in `engine/regroup_planner.py` to partitioned `AggregateKey`s in measures / composites / order specs (→ combined) as well as computed dimensions (→ row); total `walk_value_keys`, deduped by identity. Verify 1.2 progresses.
- [x] 2.2 Generalize the desugar (`_plan_dimension_regroups` → `_plan_regroups`) in `engine/stage_planner.py`: group by (`partition_keys`, attach-phase), synthesize one host-rooted producer per group via `plan_query(disable_host_rooted_isolation=True)`, substitute → placeholder, emit `RegroupAttachPlan` with the right phase. Verify 1.2 / 1.4 pass at the planner level.
- [x] 2.3 Thread the consumer public alias into producer synthesis (F1): public alias for a directly-named measure, canonical for a composite leaf. Verify 1.3 passes.
- [x] 2.4 Run the DEV-1824 guards (window= / first-last / nested-transform / in-filter) on an immutable original-prebound snapshot before discovery / classification / producer recursion; retain the post-substitution computed-dimension filter pass (F3). Verify 1.5 passes.

## 3. Retire the local partition arms

- [x] 3.1 Remove `classify_isolation`'s `partition_keys is not None` branch (`engine/isolation.py`); add a fail-closed assert that no local partitioned aggregate reaches the aggregate loop. Verify `tests/test_dev1746_isolation_classifier.py` + 1.2 stay green.
- [x] 3.2 Remove the `has_partition` accept in `_dispatch_filtered_local` and the `partition_keys is not None` grain-narrow arm in `_plan_filtered_local` (`engine/cross_model_planner.py`); leave non-partition filtered-local and `_narrow_shared_grain_to_partition` unchanged. Verify the non-partition filtered-local suites stay green.

## 4. Generator — combined attach rendering

- [x] 4.1 Generalize `_render_with_cross_model_plans` to render `RegroupAttachPlan(attach_phase="combined")` as a first-class join-back producer: rerooted-plan producer CTE (dotted aliases, public-alias agg names), composite classification + local-dependency promotion into `_base` (F2), P-I join-back / CROSS JOIN for grand total, placeholder resolution via `regroup_env` in the combined scope. Verify `local/*` execution tests pass.
- [x] 4.2 Wire the dispatch in `generate_from_planned`: combined-only → generalized path; combined + {cm/wm/rk/transform} and row+combined → `NotImplementedError` (DEV-1824). Verify 1.6 passes.

## 5. Verification & golden

- [x] 5.1 Regenerate `local/*` goldens (`SLAYER_UPDATE_GOLDEN=1`) and diff vs the committed `tests/golden/dev1739_sql_baseline.json` — expect **zero** diff; surface any divergence to the reviewer before blessing. `cross_model/*` + `guard/*` untouched.
- [x] 5.2 `assert_scope_closed` clean on the new path (`SLAYER_VALIDATE_SCOPES=1`) across the DEV-1739 suites.
- [x] 5.3 Full non-integration suite green (`poetry run pytest -m "not integration"`); `poetry run ruff check slayer/ tests/`.
- [x] 5.4 `openspec validate dev-1829-migrate-dev-1739-partitioned-aggregate-join-backs-onto-the --strict` passes.
