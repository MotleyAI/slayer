## Context

See proposal.md › Why. Mechanism, verified by probe on `tests/_dev1739_fixtures.py`:

- **Shape A (partition-key filter).** The filter's `ColumnKey(customer_id)` is interned as a hidden ROW slot (`engine/compile/projection.py` `ProjectionPlanner.plan`). `staging.py::_attach_join_keys` marks the host join key of *every* attach as `needs_column` when a slot exists for it — including a `"row"` attach, whose join sits inside `_base`'s own FROM and reads row-level columns. `staged_plan.base_render_order` then projects the slot in `_base`, `generator._build_base_select_for_planned` adds every projected ROW slot to `GROUP BY`, and the outer trim wrap only drops columns. Without the filter no slot exists, so nothing is grouped.
- **Shape B (order key beside a ranked measure).** `stages.py` decides "grouped?" for the ORDER BY MIN/MAX wrap as `bool(agg_slots) or (dims and distinct_dimension_values)`. A `first`/`last` measure becomes a combined attach, so `agg_slots` is empty, no wrap is synthesised, and the raw sort column becomes a `_base` GROUP BY key.
- A throwaway probe (raise on any hidden ROW BASE `needs_column` slot in a grouped plan) fired 51 times in the unit suite: the 7 row-attach join keys (all shape A, some masked by data), one ORDER BY ColumnKey (shape B), and 37 `__regroup__` placeholder passengers read by a HAVING mask or an ORDER BY — harmless, because those positions require the attach's partition keys to be query dimensions, so the placeholder is determined by the grain.

Applicable principles: semantics Axioms 12 (grain guarantee), 13 + Law 6 (hidden-field compilation is a pure optimization); sql P9 (fail closed), P12 (attach is cardinality-neutral); engine P4 (hidden slots trimmed from the projection), P6 (placement from planner facts, never key shape at render time); ir P1 (representation + pure functions); system P12 (Pydantic). No arc42 / `.c4` edit is part of this change.

## Goals / Non-Goals

**Goals:** one planner-owned grain fact per plan, read by every grain / groupedness consumer; make the hidden-value-widens-grain class impossible to be silent.

**Non-Goals:** retyping the sql-side grain representation into a `Grain`-coherent wrapper (DEV-1895, a pure refactor that can build on this field); changing the renderer's GROUP BY construction (the invariant makes it correct as is); any change to which shapes are legal.

## Decisions

**D1 — `PlannedQuery.grain: Optional[List[SlotId]]` replaces `PlannedQuery.distinct_dimension_values`.** The deduplicated slot ids of `public_projection[:n_dims + n_tds]` in position order (computed dimensions included); `None` iff raw-row mode (which already rejects every aggregation and measure reference); `[]` for a grouped query with no dimensions. Computed once in `stages.py` where `n_dims + n_tds` is known; `_emit_stage_schema` takes that list instead of `n_grain_positions`. Generator reads of `planned_query.distinct_dimension_values` become `planned_query.grain is not None`. *Why one field, not grain + flag:* `None` vs `[]` carries exactly the raw-rows distinction, so a separate boolean could only disagree with it. *Alternative rejected:* deriving grain from non-hidden ROW slots — that is the key-shape derivation this change removes (DEV-1967 ruling: grain comes from dimension positions).

**D2 — One groupedness fact.** "Is this plan grouped?" is `grain is not None` everywhere: the ORDER BY wrap classification in `stages.py` (replaces `_has_grouping`; fixes shape B — the sort key takes the same MIN/MAX host / crossing wrap it already takes beside `sum`), and the `distinct_dimension_values` argument threaded into `stage_slots` / `_compute_needs_column` / `_grouped_order_targets`.

**D3 — A row attach's join key never needs a `_base` column.** `_attach_join_keys` marks only host keys of `"combined"` / `"shifted"` attaches (they join after `_base`, reading its columns). A `"row"` attach joins inside `_base`'s FROM on row-level references. Fixes shape A.

**D4 — Grain-determination invariant, one pure function in `ir/planned.py`.** In a grouped plan, every own slot with stage BASE, phase ROW and `needs_column=True` MUST be grain-determined, where a key is determined iff it is: the key of a grain slot; a `LiteralKey`; a placeholder substituted by a `"row"` attach of this plan all of whose `join_pairs` host keys are determined; or a non-aggregate composite (`SLOT_COMPOSITE_KINDS`) all of whose children are determined. Violation → `MaterialisationStageError`. Called from a `PlannedQuery` model validator AND from the generator's render-time plan check (alongside `_assert_stages_assigned`), recursing into every producer plan — `model_copy` / `model_construct` bypass validators (Codex F1). *Why keep the renderer's GROUP BY as "every projected ROW column":* under the invariant that set is grain ∪ grain-determined passengers, so grouping by passengers is value-neutral and the 37 existing passenger shapes keep byte-identical SQL. *Alternative rejected:* wrapping passengers in `MIN`/`ANY_VALUE` — churns goldens and needs dialect hooks for no semantic gain.

**D5 — Every grain re-derivation reads `grain`.** `_producer_grain_slot_ids` (the attach-covers-producer-grain check; Codex F2), `generator._ranked_emission_from_kernel`, `generator._windowed_emission_from_kernel`, `SQLGenerator._transform_grain_slot_ids` (grain members, then the existing axis rule and combined-placeholder exclusion) and `stages._frame_bound_columns` (grain's `TimeTruncKey` members). Golden SQL MUST stay byte-identical; any delta must be shown value-preserving and recorded through the golden module's `ALLOWED_DELTAS` with a reason.

## Risks / Trade-offs

- [A grain consumer silently relied on a non-dimension projected ROW slot (e.g. a measure-position row-attached placeholder in the transform auto-grain)] → such a slot is grain-determined, so dropping it from a PARTITION BY is value-neutral; the byte-identical golden gate surfaces any text change for explicit review.
- [The invariant fires on a legal shape not exercised by the suite] → fail-closed by design (sql P9); the full unit + integration suites run as the gate, and the probe already enumerated every shape the suite reaches.
- [`test_dev1645…ranked_scope` asserts today's wrong ORDER BY shape] → its assertion moves to the MAX-wrapped form; approved as part of this plan.
