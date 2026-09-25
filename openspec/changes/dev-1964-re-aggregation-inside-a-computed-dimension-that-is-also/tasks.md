## 1. Tests (spec-tests stage)

- [x] 1.1 Create `tests/test_dev1964_dual_phase_consumption.py` (sqlite + duckdb via `tests/_dev1847_fixtures.py::make_exec_engine`) with one test per `queries/computed-dimensions` scenario of this change; verify each fails on the current head for the reason in design.md › Context (not a fixture error)
- [x] 1.2 Add the `queries/partitioned-aggregates` scenarios: (e) plain measure-typed filter → `PartitionKeyError` naming `city`; (f) finer-grained re-aggregation dimension executes with North/lo 30, North/hi 60, South/hi 140, East/hi 180, Gap/lo 20, Void/lo NULL; verify red
- [x] 1.3 Add checker-level negative tests: the (f) re-aggregation as measure, raw ORDER BY, inside an arithmetic measure, inside a transform used as a measure, in a measure-typed conjunct produced by splitting an AND filter, and a cross-model outer re-aggregation with a non-dimension outer key — each asserts `PartitionKeyError`, the offending key and the consuming position; verify red where today's error is wrong or mislocated
- [x] 1.4 Add plan-level tests: a dual-phase re-aggregation interns to ONE producer; its placeholder resolves to exactly one attach per consumer phase (ROW for dimensions, COMBINED for measure / filter / order); discovery dispositions of a `rank(R)` dimension contain the transform root and no independent re-aggregation or constituent; synthesized producer grain names are unique for two expression-valued grain keys; verify red
- [x] 1.5 Assert rank values (South 1, East 2, North 3, Gap 4, Void 5) directly on both dialects, not only row order; if a dialect disagrees on Void, STOP and ask (no oracle edit)

## 2. Implementation (spec-implement stage)

- [x] 2.1 D2: one type-based "explicitly grained aggregate" predicate used by `dimension_transform_roots` / `position_classes` and `_Walker.dimension`; a covered re-aggregation subtree is owned by its dimension transform root — verify the 1.4 discovery test passes
- [x] 2.2 D1: fold re-aggregation grouping into `_group_routed_roots` (row + combined buckets, per-occurrence alias / declared type); delete `_group_reaggregation_roots` / `_ReaggregationRoots`; one attach per (root, phase) over one interned producer and one placeholder — verify the 1.4 plan tests and scenarios 1–3 pass
- [ ] 2.3 D4: row-phase re-aggregation attaches synthesized at their declared grain (DEV-1928 constituent path); producer projected grain == join coordinates — verify (f) and `tests/test_dev1903_discovery.py::TestTransformOverReaggregationDimension` pass
- [x] 2.4 D3: move "must be a query dimension" out of `bind_inputs` into a post-`type_and_split_filters` checker pass over consumer positions; delete `check_reaggregation_partition_key_is_query_dim` and its call site — verify 1.2 (e) and 1.3 pass and the existing combined-consumer partition-key tests stay green
- [x] 2.5 D5: confirm the `'grain'` collision site, then make synthesized grain names injective (declared dimension name, else deterministic key-derived identifier) and assert uniqueness at producer assembly — verify the two-dimension scenario and the 1.4 naming test pass
- [x] 2.6 D6: dimension-borne transform / re-aggregation order targets resolve at plan time — verify the order scenarios (plain rank and R) pass
- [x] 2.7 Apply the approved `architecture/engine.arc42.md` §3 item 4 edit verbatim (append after `[review]`): "A value consumed at several attach phases attaches once per phase over one interned producer — never a phase chosen by precedence. [enforced: test:tests/test_dev1964_dual_phase_consumption.py]" — verify `poetry run python tools/arch_check.py` passes

## 3. Verification

- [ ] 3.1 Run `poetry run pytest -m "not integration" -n auto`; re-bless SQL goldens only where values are unchanged; any test-logic change needs user consent — verify green
- [ ] 3.2 Run the integration suite with the CI invocation from CLAUDE.md, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline) and `poetry run python tools/arch_check.py` — verify all clean
- [x] 3.3 Update `docs/concepts/` only if user-facing behaviour documented there changes (the (f) finer-grained re-aggregation dimension becomes legal) — one concise sentence; verify `zensical.toml` nav unaffected
