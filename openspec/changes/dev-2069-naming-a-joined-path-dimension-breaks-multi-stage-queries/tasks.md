## 1. Tests (pr-tests)

- [x] 1.1 Create `tests/test_joined_path_dimension_names.py` (no issue number in the name), parametrized over SQLite + DuckDB via `tests/_dev1739_fixtures.py` `make_exec_engine`; values hand-computed from that fixture (by customer 90/70/50, by region 160/50, RegN/RegS); verify it collects.
- [x] 1.2 Must-fail-today scenarios: named one-hop single query (`orders.region_id`, plus response `attributes` keys ⊆ `columns`); named one-hop stage; named two-hop stage; downstream `sum(revenue)` by `rid`; query-backed model (`SlayerModel(source_queries=...)`); unnamed computed path single + stage; plain + named over one path, interleaved with `status`, single + stage (column order asserted). Verify each fails on the unfixed code.
- [x] 1.3 Controls (pass today): plain dotted dimension in a stage read as `customers__region_id`; local rename `cust`; order by / filter by `rid`; name collision `{"name": "region", "expression": "customers.regions.name"}` raises the computed-dimension name-collision error.
- [x] 1.4 Parity law over every query in 1.2–1.3 (dry-run): `projection_result_keys(root_planned)` equals, in order, the rendered SQL's outer aliases after the dialect's `decode_result_keys`.
- [x] 1.5 Stage-schema assertions: for named and unnamed joined paths, each `StageColumn` has `name == sql_alias ==` the flat name and `public_alias ==` the relation-stripped dotted result key.
- [x] 1.6 Unit test: `build_flat_rename_wrapper` with a mismatched expected schema and `stage="cr"`, `source_relation="orders"` raises a message naming `stage 'cr'` and not `stage 'orders'`.

## 2. Implementation (pr-implement)

- [x] 2.1 Add `slot_result_key` / `slot_result_keys` to `slayer/sql/naming.py` per design D1; verify 1.4 for unnamed paths stays green. (As built: no `slot_result_keys` — every consumer walks occurrences; `pick_slot_alias` / `next_slot_result_key` added instead.)
- [x] 2.2 Route `_full_alias_for_slot` through `slot_result_key` (alias via `_pick_alias_for_planned_slot`); verify 1.2 single-query scenarios pass. (As built: `_pick_alias_for_planned_slot` moved to `naming.pick_slot_alias`.)
- [x] 2.3 Delete `response_meta._slot_result_keys`; `projection_result_keys` and the attribute loop walk `root_planned.projection` occurrences with a per-slot alias index and call `slot_result_key`; drop the "mirror" docstring; verify 1.4.
- [x] 2.4 `_emit_stage_schema` derives `name` / `sql_alias` / `public_alias` from `slot_result_key` (design D3); replace the regroup `_flat` with `flat_name(..., strip_relation=relation)`; verify 1.2 stage scenarios and 1.5 pass.
- [x] 2.5 `build_flat_rename_wrapper` gains required `stage: str` for its message; callers pass `stage_schema.display_name` (stage chaining, virtual-model wrap) or the producer CTE name (regroup); verify 1.6.
- [x] 2.6 Docs, one sentence each in `docs/concepts/queries.md`: "Expression dimensions" — the name keys the result even for a bare joined path; replace the stale renamed cross-model measure key claim (~line 390) with `<model>.<name>`.
- [x] 2.7 Full non-integration suite, `ruff check slayer/ tests/`, `basedpyright` (no new errors vs baseline), `la-arch-check`; all green.
