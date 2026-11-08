## 1. Tests (pr-tests stage — all fail before the fix)

- [x] 1.1 Renderer unit tests (`render_value_key`, alias mode): an available composite slot reads by alias; an available slot in `value_by_slot_id` reads that value; an interned-but-unavailable composite renders structurally; an unavailable `AggregateKey`/`TransformKey`/`ColumnKey` still raises `RenderContextMissingFacilityError`; a `LiteralKey` renders as a literal — verify each fails or passes as expected against current code
- [x] 1.2 Executed DEV-2062 scenario (SQLite + DuckDB): saved `orders.aov` + `cumsum(aov)` with `date_range` and with a plain date filter; inline spelling; hand-computed values and equality with the single-measure runs — verify it fails today with `RenderContextMissingFacilityError`
- [x] 1.3 Executed consumer matrix (SQLite + DuckDB) with the composite selected, for a local ratio AND a ratio with a cross-model operand: `cumsum`, `lag`, `lead`, `change`, `change_pct`, `consecutive_periods`, `rank(direction='desc')`, `cumsum(x)/ratio`, `cumsum(ratio)+ratio`, `cumsum(ratio*2)` (ratio selected; ratio*2 selected), measure-typed filter on `cumsum(ratio)`, ORDER BY `cumsum(ratio)` — assert split equality, filter/order row selection unchanged, no internal placeholder in SQL
- [x] 1.4 Golden SQL (PostgreSQL + T-SQL): a selected composite reused by a measure-typed filter and by an order-only composite — record baselines once the fix lands
- [x] 1.5 Executed literal matrix (SQLite + DuckDB): `cumsum(1)`, `lag(1)`, `lead(1)`, `change(1)`, `change_pct(1)`, `time_shift(1, -1)`, `consecutive_periods(1 > 0)` with hand values; `rank(1, direction='desc')`, `dense_rank(1, direction='desc')`, `percent_rank(1)` with and without `partition_by=` (1/1/0); `ntile(1, n=2)` bucket multiset only
- [x] 1.6 Law harness: add `amount:sum / *:count` and a transform over it to `MEASURE_POOL` in `tests/_law_harness.py`, plus one fixed mandatory shape selecting both in `sample_shapes()` — verify `tests/test_law_split_invariance.py` fails on that shape today

## 2. Implementation (pr-implement stage)

- [x] 2.1 `render_value_key` alias mode: read any key whose slot is available (`available_alias_by_slot_id` / `value_by_slot_id`) by its slot; unavailable alias-slotted kinds raise; composites and literals render structurally — verify 1.1 passes
- [x] 2.2 Retire `AliasFacilities.composite_alias_slot_ids` and `SQLGenerator._dimension_composite_slot_ids` and their call sites — verify no references remain (LSP find-references) and computed-dimension tests stay green
- [x] 2.3 `_render_window_transform_sql`: one `render_value_key(key.input, alias ctx)` call for every input shape; remove the composite branch and the `transform input not materialised` branch — verify 1.2, 1.3, 1.5 pass
- [x] 2.4 Record the golden baselines for 1.4 after confirming executed values in 1.3 are unchanged — verify 1.4 passes
- [x] 2.5 Full unit suite (`poetry run pytest -m "not integration"`), integration suite (CI invocation), `ruff check`, `basedpyright` (no new errors), `la-arch-check` — all green; any shifted golden SQL re-blessed only after its executed values are confirmed unchanged
