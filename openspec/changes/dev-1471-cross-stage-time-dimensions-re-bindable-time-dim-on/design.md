# Design

## Context

See proposal.md — Why. `bind_time_dimension` (`slayer/engine/binding.py`) raises `IllegalScopeReferenceError` on any `StageSchema`; it also carries the "column must be temporal" `ValueError` itself. `bind_query_inputs` (`slayer/engine/bind_inputs.py`) skips `date_range` on a non-model scope and resolves the active time dimension only for a `ModelScope`. `_emit_stage_schema` (`slayer/engine/compile/stages.py`) is the one place `StageColumn`s are built, from the projection's slots. A scratch probe with the refusal removed showed the compiler and renderer already handle every stage shape in scope (re-bucketing, shift family, `cumsum`, `last`, windowed and rank producers, `date_range`, `gran(col)` order keys, suffixed keys, three-stage chains, multi-hop flat names); the work is binding, typing and one SQLite rendering bug.

Constraints: engine principle 9 (user-facing algebra type errors raise in the checker; `binding` may not import `elaborate_env`, `bind_inputs` may import both); engine principle 5 (`StageColumn` carries the per-column metadata downstream stages need); sql principle 2 (dialect quirks live only in `dialects/`); semantics Axiom 9 (closure: the rule inspects the operand's type, never its construction) and Axiom 11 (the axis is the query's active time bucket — for a stage, the stage-bound bucket).

## Goals / Non-Goals

**Goals:**
- One binder path and one checker rule for time dimensions, whatever the scope kind.
- The upstream granularity is a type fact on the stage schema, so refinement is a pure type check.
- SQLite temporal-cast suppression is a declared dialect capability applied at every declared-cast seam.

**Non-Goals:**
- Granularity on the saved query-backed model's cached `Column` (DEV-1929).
- A model-level default time dimension for stages (a stage has no model).
- Changing any non-SQLite dialect's SQL.

## Decisions

1. **The binder returns facts, the checker judges.** `bind_time_dimension` returns `BoundTimeDimension` (`slayer/ir/bound.py`: `bound`, `column_type`, `upstream_granularity`); `bind_inputs` calls `check_time_dimension_column` in `elaborate_env` right after binding each projected time dimension. The `gran(col)` order branch and the active-time-dimension binding rebind an already-checked entry and use `.bound`. Alternative (raise in the binder, as the temporal check does today) rejected: it violates principle 9 twice and cannot get ledger rows.
2. **One column-facts helper with two arms.** Model arm = today's resolution (dotted/bare, terminal-model walk, `col.type`, no upstream granularity, `routed_dotted`); stage arm = the ordinary Mode-B identifier resolution (`_resolve_ref` → `ColumnKey(path=(), leaf)`; unknown → `UnknownReferenceError` with the stage's columns; dotted → `IllegalScopeReferenceError`), facts read off the `StageColumn`. Both arms end in the same `TimeTruncKey` construction.
3. **Granularity nesting is a lattice on the enum.** `TimeGranularity.nests_into(other)`: equal; `second→minute→hour→day`; `day→week`, `day→week_sunday`; `day→month→quarter→year`; transitive closure; everything else false — so week/month are rejected both ways, since re-bucketing month starts by week (or week starts by month) is a silently wrong bucket. Alternative (duration order) rejected for admitting exactly that wrong answer.
4. **Untyped stage column fails closed.** A `StageColumn` with `type=None` is not provably temporal; Axiom 9 needs the type, so it takes the temporal error.
5. **`StageColumn.granularity` is stamped from the slot key.** `_emit_stage_schema` sets it iff the slot's key is a `TimeTruncKey`; every other column (aggregate output, raw column, computed dimension) carries `None`, which is what "accepts any granularity" means. Two granularities of one column are two slots and two columns, each with its own value.
6. **One error class, two messages.** `TimeDimensionColumnError(SlayerError, ValueError)` with plain message text; the temporal message keeps today's wording. Two ledger rows keyed by (class, function, message).
7. **Stage time axis reuses the model resolver.** `_resolve_main_time_dimension` takes `model: Optional[SlayerModel]`; the `default_time_dimension` fallback runs only with a model. Two stage time dimensions without `main_time_dimension` fall through to the existing `check_time_transforms_resolved` remedy.
8. **`date_range` binds the bare stage column.** The skip is deleted; `_build_date_range_filter` accepts either scope and its `bind_expr` yields the stage `ColumnKey`, so the inclusive `BetweenKey` lands in the stage's own WHERE as a row-phase mask, exactly as on a model.
9. **Temporal-cast suppression is a dialect capability applied through one helper.** `BaseDialect.declared_cast_type(dt)` returns `dt`; the SQLite dialect returns `None` for DATE / TIMESTAMP. The generator gets one `_cast_declared(expr, dt)` helper that applies the dialect policy and then `_wrap_cast_for_type`; the three seams that emit declared-type casts (`_slot_cast_type` for every slot site, `_resolve_sql`, `_derived_column_expr`) route through it (Codex finding: the third seam bypassed both others). `_ranked_value_cast_type` and `_filter_cast_type` stay: they encode redundancy on every dialect, not a dialect quirk. Alternative (suppress temporal casts globally) rejected: it changes five dialects' SQL and undoes the deliberate Postgres `date_trunc` overload cast.

## Risks / Trade-offs

- [Twelve direct `bind_time_dimension` test calls change shape] → mechanical `.bound.value_key` edit, approved.
- [SQLite goldens pinned the destructive cast] → re-blessed and listed in the report; DuckDB/Postgres/T-SQL/BigQuery goldens must stay byte-identical (asserted by the dialect scenario).
- [DEV-1832 edits the same active-time-dimension block and `elaborate_env` time-axis helpers] → small, local conflict expected on merge; resolve in favour of the Axiom 11.3 wording.
- [Message bytes are pinned by the ledger] → finalise both messages before writing the rows; changing a byte later means changing the row.

## Migration Plan

No data or schema migration. `StageColumn.granularity` is a new optional field on an in-memory plan object; `BoundTimeDimension` is a new IR type. Rollback = revert.
