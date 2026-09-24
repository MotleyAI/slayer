## 1. Tests (spec-tests stage — all fail before implementation)

- [ ] 1.1 Grain emission (plan-level, `plan_query` / `plan_stages` over in-memory models): aggregating stage → dimensions + time dimensions; dimension-only → dimensions; `distinct_dimension_values=False` → `None`; measures-only → `[]`; `{"expression": "sum(amount, partition_by=city)", "name": "ct"}` dimension is a member; `sum(amount, partition_by=status)` measure is not; `rank(sum(amount, partition_by=city))` dimension is a member; `cumsum` stage includes its time bucket; one slot projected as dimension `ct` and measure `ct2` (same expression) → `ct` member, `ct2` not; `StageSchema` validator rejects a grain member that is not a column. Verify: tests exist and fail on main.
- [ ] 1.2 Builder unit (`model_from_stage_schema`): single-member grain → `unique=True`, no PK; composite → `primary_key` on each member, `unique=False`; `[]` / `None` → nothing stamped; `sql=None` → `sql_table=name`, else `sql`; `column_sql` applied; type / granularity / label / format / description carried; `default_time_dimension` passed through. Verify: tests fail on main (function absent).
- [ ] 1.3 Sibling execution (sqlite + duckdb, shapes that run on main): `[c over customers dims [id] measure max(tier) as tr, b over orders joining c on customer_id→id with dims [c.tr] and sum(amount), root over b]` → exact per-tier totals, no `BroadcastGrainWarning`; sibling with dims `[id, tier]` joined on `id` alone → broadcast + warning; `ModelExtension` over a sibling (adding a column) joined on the full grain → proven; chain `x(id, tier)` → `c` computing `max(tier)` succeeds. Verify: the proven-join cases fail on main (broadcast today).
- [ ] 1.4 Query-backed (sqlite + duckdb): model over `customers` dims `[id, tier]` → `max(tier)` succeeds; model with dims `["status", {"expression": "sum(amount, partition_by=city)", "name": "ct"}]` joined from a host on `status` → unproven, broadcast with warning, never multiplied (a 2-row host's `count(*)` totals 2 per cell, not 4); single-dim `[tier]` model → `tier` unique, not PK, join on `tier` proven; measure placeholder never stamped. Verify: fail on main.
- [ ] 1.5 Identifier rule: sole PK accepts `count` / `count_distinct` / `count_distinct_approx` / `min` / `max`, refuses `sum`; composite members accept `max` and `sum` (type defaults) and a composite member's `allowed_aggregations: ["sum"]` validates; composite members are sampled / type-probed / profiled (both `profile_column` and the batched `profile_dimensions` path) while a sole PK is skipped. Verify: fail on main.
- [ ] 1.6 Amend `tests/test_dev1836_query_model_stamping.py::test_dimension_only_distinct_stamps_grain` to assert `status` is unique and not a primary key (user-approved); rename `synthetic_model_from_stage_schema` → `model_from_stage_schema` in `tests/test_dev1929_column_granularity.py` (mechanical, assertions unchanged).

## 2. Typed grain

- [ ] 2.1 Add `StageSchema.grain: Optional[List[str]]` with the member-names-a-column validator (`slayer/core/scope.py`). Verify: 1.1 validator case passes.
- [ ] 2.2 Pass the declared dimension / time-dimension occurrences and `distinct_dimension_values` into `_emit_stage_schema` and record the grain from those occurrences' emitted aliases (never from phase / key shape / regroup prefix / `needs_column`). Verify: 1.1 passes.

## 3. One builder

- [ ] 3.1 Implement `model_from_stage_schema` in `slayer/ir/source_bundle.py` (D2 inputs, D3 stamping) and delete `synthetic_model_from_stage_schema`. Verify: 1.2 passes.
- [ ] 3.2 Switch `engine/plan.py`, `stage_bundle_with_siblings`, `sql/generator.py::_bundle_for_stage` to the builder. Verify: 1.3 passes.
- [ ] 3.3 Rebuild the query-backed tail of `_expand_query_backed_model` on the builder (wrapped SQL, fit map, `default_time_dimension` as inputs) and delete the `stamp_grain` / `grain_public_names` / `REGROUP_LEAF_PREFIX` block. Verify: 1.4 and `tests/test_dev1836_query_model_stamping.py` pass.

## 4. Identifier rule

- [ ] 4.1 Add the sole-primary-key identifier predicate in `slayer/core/models.py`; add `min` / `max` to `PRIMARY_KEY_AGGREGATIONS`; route every identifier-treatment site through it (binding PK gate, `allowed_aggregations` validator, `facade/catalog.py`, `inspect/model_render.py` ×4, `query_engine.py` type probe ×2, `profiling.py` `_collect_dim_profile` + ×3) and reuse it in `declares_solo_unique`; leave flag-rendering sites alone. Verify: 1.5 passes; grep shows no identifier-treatment `c.primary_key` check left.
- [ ] 4.2 Correct source docs: `Column.unique` field comment and the binding PK-gate docstring (only a sole primary key is an identifier; a composite key is unique only as a whole). Verify: diff review.

## 5. Docs and gates

- [ ] 5.1 `docs/concepts/models.md`: `primary_key` table row, the primary-key aggregation paragraph, and the `unique` row's "primary_key implies unique" (sole primary key only) — one sentence each; `docs/concepts/queries.md`: one sentence that a join onto a sibling stage covering its grain is proven. Verify: grep docs for the old claims.
- [ ] 5.2 Full unit suite, `ruff check slayer/ tests/`, `tools/arch_check.py`, basedpyright (no new errors vs baseline); golden / notebook drift → stop and ask. Verify: all green.
