## Context

Binding and interning already keep a computed dimension's name (`public_name`, `explicit_aliases`). The defect is downstream: a slot's output name is decided in three places that must agree by hand —

1. `SQLGenerator._full_alias_for_slot` (`slayer/sql/generator.py`): a ROW slot whose `ColumnKey` / `ColumnSqlKey` / `TimeTruncKey` has a non-empty path always gets the path-derived key, ignoring its aliases and not advancing `alias_index`;
2. `response_meta._slot_result_keys`: a documented hand-mirror of (1);
3. `_emit_stage_schema` (`slayer/engine/compile/stages.py`): flattens the picked alias, never the path.

(1)/(2) drop the user's name; (3) keeps it, so the stage flat-rename wrapper's check fails. A fourth inline copy, `_flat` in the regroup-producer attach, re-implements `flat_name`. This violates sql P3 (`naming.py` owns every alias and result-key decision).

## Goals / Non-Goals

**Goals:** one function decides each slot alias's result key; every consumer walks projection occurrences the same way; the stage wrapper check becomes a pure render-divergence backstop.

**Non-Goals:** changing `name_is_explicit` (an explicit name equal to the auto name stays non-explicit and keys by path); consolidating the transform / composite alias-cycling sites in the generator (they never take the path branch); identifier-fitting changes.

## Decisions

- **D1 — single authority.** `slot_result_key(*, slot, alias, source_relation) -> str` in `slayer/sql/naming.py`: an alias in `slot.explicit_aliases` → `result_key_from_alias`; any other alias of a ROW-phase `ColumnKey` / `ColumnSqlKey` / `TimeTruncKey` with a non-empty path → the canonical path key (`result_key` / `time_trunc_result_key`, unchanged); otherwise `result_key_from_alias`. `slot_result_keys(*, slot, source_relation)` maps it over `public_aliases` (else the declared name). Alternative rejected: "alias equals the flattened path" as the path test — it would key an unnamed `{"expression": "customers.region_id"}` as `orders.customers_region_id`, breaking spelling-invariance with the plain dimension and changing a working single-query key.
- **D2 — one walk.** Every consumer walks projection occurrences with a per-slot alias index and calls `slot_result_key`: `_full_alias_for_slot` (via `_pick_alias_for_planned_slot`), `projection_result_keys` (now over `root_planned.projection`, not slot kinds — so planned keys equal rendered keys in order), the response-attribute loop, and `_emit_stage_schema`. `response_meta._slot_result_keys` is deleted.
- **D3 — stage columns from the key.** Per stage column, `rk = slot_result_key(...)`; `name` / `sql_alias` = `flat_name(rk, strip_relation=source_relation)`; `public_alias` = `rk` with the relation stripped (dotted, as `StageColumn` documents). The regroup `_flat` becomes `flat_name(..., strip_relation=relation)`.
- **D4 — stage-named errors.** `build_flat_rename_wrapper` takes a required `stage: str` used only in its message: stage chaining and the query-backed virtual-model wrap pass `stage_schema.display_name`; the internal regroup producer passes its CTE name.

## Risks / Trade-offs

- [A call site relied on `_full_alias_for_slot` not advancing `alias_index` for joined ROW slots] → local slots already advance at every site; the full suite plus the parity law (planned keys == decoded rendered keys, in order) catch divergence.
- [Goldens move] → only for user-named bare joined paths and second aliases of a joined ROW slot, both broken today.
- [Parity law vs identifier fitting] → compare rendered aliases after the dialect's `decode_result_keys`, as the response path does.
