## Context

See proposal.md — Why. The checker's raise surface is inventoried row-by-row in `tests/_dev1871_raise_ledger.py` (site · type · collapsed message · category · family · owner) and byte-pinned by `tests/test_dev1871_raise_parity.py`; goldens record `{error: type name, message: str(exc)}` and move only via per-module `ALLOWED_DELTAS`. Nothing in `slayer/` inspects checker messages or catches `NotImplementedError` from the checker; REST maps `ValueError` → 400 and lets `NotImplementedError` escape as a bare 500; MCP shows `str(e)`; Flight propagates engine errors through pyarrow's implicit exception→status conversion.

## Goals / Non-Goals

**Goals:** one typed family for every checker type error, format guaranteed by construction and ratcheted by the parity test; structured `.summary` / `.location` / `.suggestion`.

**Non-Goals:** bind rows (incl. `GranularityCallError`), compiler rows (`_topo_sort`, stage-DAG), `internal` / `control` rows (they signal bugs, not intentional failures), the rest of engine/core, and plain-message `SlayerError`s outside the family — all DEV-1969.

## Decisions

**D1 — Scope (Q1 = A).** Checker-category ledger rows + `core/window_duration.py::parse_window_duration`. A re-parented class carries all its raise sites, so the two `DistinctDimensionValuesError` raises in `core/query.py::_validate_distinct_dimension_values` convert too. Rejected: adding stage-DAG / `GranularityCallError` (not algebra type errors; own issue), engine-wide sweep (DEV-1969).

**D2 — Taxonomy (Q2 = b).** `QueryTypeError(SlayerError)` base + one concrete class per rule family, all in `slayer/core/errors.py` (core P4: the vocabulary lives in core). The base ctor is keyword-only (`summary`, `location=None`, `scope=None`, `suggestion=None`, `extras=None`), stores the parts as attributes, renders via `_format_error_message(cls_name=type(self).__name__, …)`, and raises `TypeError` when `type(self) is QueryTypeError` (Codex 5 — non-instantiable in fact, not by convention; cheaper than an ABC). Rejected: one flat class (the prefix would carry no information), one class per rule (~35, sprawl).

Row → class mapping:

| Class | Checker rows |
|---|---|
| `TimeAxisError` | `check_dimension_temporal_axis` (was `NotImplementedError`), `check_windowed_time_dimension`, `check_time_transforms_resolved`, `check_windowed_time_axis_attributable` ×2, `check_time_dimension_date_range` |
| ↳ `TimeDimensionColumnError` | `check_time_dimension_column` ×2 |
| `WindowDurationError` | `parse_window_duration` (every arm, incl. the non-string guard moved in from the checker) |
| `PartitionKeyError` | `check_partition_key_resolves` ×2, `check_transform_partition_keys_in_operand_grain`, `check_partition_key_attributable`, `check_cross_model_partition_keys_attributable`, `check_reaggregation_partition_key_is_query_dim` |
| `UnsafeJoinInputError` | `check_local_producer_inputs_safe` ×3, `check_cross_model_inputs_safe` ×2, `check_cross_model_source_resolves` |
| `UnanalyzableDependencyError` | `check_input_dependencies_analyzable` ×2, `check_filter_dependencies_analyzable` ×2 |
| `TransformInputError` | `check_transform_inputs` ×2 |
| `ComputedDimensionError` | `check_computed_dimension` ×3 (its raw-rows arm stays `DistinctDimensionValuesError`) |
| `AssociationError` | `check_association_windowed_ranked`, `check_association_root_unique_key` |
| `ParameterGrainError` | `check_parameter_determined` |
| `ReaggregationError` | `check_reaggregation_no_window`, `check_reaggregation_dims_attributable` |
| `NameCollisionError` | `check_computed_dim_name_collision` ×2, `check_stage_flatten_collision` (inline `flatten_collision_message`), `check_measure_dedupe_collision` ×2, `check_reserved_regroup_prefix` |
| ↳ `MeasureNameCollidesWithColumnError`, `CanonicalAliasShadowsColumnError`, `DuplicateMeasureNameError` | their rows; structured ctors + attributes kept, but moved onto the D3 schema (Codex 2) |
| `DimensionTypeError` | `check_opaque_grouping_dim` |
| `ModelFilterError` | `validate_model_filter` ×2 |
| `PositionTypingError` | `type_position_conjunct` ×2, `check_order_target_has_slot` |
| `DistinctDimensionValuesError` | 6 checker rows + 2 in `core/query.py` |

`PositionTypingError`, `DistinctDimensionValuesError`, `TimeDimensionColumnError` move from a positional message to the base's keyword ctor.

**D3 — Message schema (Q3 = a, Q6 = i).** `location` = the subject, spelled `measure '<alias>'` / `filter '<text>'` / `dimension '<name>'` / `order item '<text>'` / `transform '<op>'` / `time dimension '<name>'` / `model filter '<text>'`, omitted when none; `summary` = the defect with the subject removed — the only rewording allowed is dropping the subject and fixing grammar; `suggestion` = the remedy sentence(s), unchanged. Every changed message is an individually blessed golden delta. Rejected: prefix-only (satisfies P4 in the letter only; no structured fields).

**D4 — Window durations (Q4 = a).** `parse_window_duration` raises `WindowDurationError` for every arm and gains the non-string guard (Codex 3); `check_window_duration` just calls it, so there is one raise path. The ledger scans `core/window_duration.py`. Rejected: checker catch-and-rethrow (leaves the parser raising bare `ValueError`), leaving it to DEV-1969 (two shapes for one mistake).

**D5 — Ratchet.** Parity test: `_collapsed` also reads the `summary` / `location` / `scope` / `suggestion` keywords in that fixed order, rendered as `_format_error_message` lines (`summary`, `\n  at …`, `\n  scope: …`, `\n  suggestion: …`) with interpolations still `…`, so a ledger message is the rendered format minus the class prefix; a keyword passed as a variable collapses to `…`. Scanned modules accept slayer-root-relative paths. New assertion: every `category == "checker"` row names a concrete strict subclass of `QueryTypeError` exported by `slayer.core.errors`. Guard ratchet: drop the dead `^A time-ordered transform …` expressiveness entry; `guards.baseline` stays 1.

**D6 — Surfaces.** REST needs no code: `except ValueError` now catches the time-axis error (400). Flight: a test pins invalid-argument for a checker type error; explicit mapping code is added only if that test shows pyarrow's implicit `ValueError` → `Invalid` path does not hold end-to-end (Codex 1). MCP `_format_resolution_error` prints `Error: {exc}` when `str(exc)` already starts with `f"{type(exc).__name__}: "`.

**D7 — arc42 (exact text approved; applied at implementation, when it becomes true).**

core P4 becomes:

```
4. **Errors are typed, with a stable format**: intentional failures are
   `SlayerError` subclasses rendered via `_format_error_message`
   [target: DEV-1969]; the checker's `QueryTypeError` family already is.
   [enforced: test:tests/test_dev1871_raise_parity.py]
```

engine P9's last clause becomes `every user-facing algebra type error raises in the checker, as a `QueryTypeError`.` Rejected (Codex 6): rewording P4 further — the `[target:]` tag already marks the not-yet-true clause.

## Risks / Trade-offs

- [~110 golden raise entries move] → each listed in its module's `ALLOWED_DELTAS` with the reason, re-blessed via `SLAYER_UPDATE_GOLDEN=1`, manifest emptied; dev1900's 7 `query_engine.py:535` keys must NOT move (not the checker).
- [Message-pinning tests break] → only mechanical, assertion-preserving retargets (same fact via `.summary` / `.location` / first line); anything else stops for consent.
- [Clients catching `NotImplementedError` for the time-axis case] → none in-repo; the error is still a `ValueError`, which REST/CLI/inspect already handle.
- [A `match=` fragment spanning subject + defect] → retargeted to `.location` / `.summary`.
