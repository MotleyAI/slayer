# DEV-1892 design — one parameter rule, one pick path

## Context

See proposal.md — Why. Substrate facts that shape the design:

- One kernel (`AssociationProducerKernel`: `entity_keys`, `null_safe`), one renderer
  (`_render_association_producer_body`) with two branches for sourcing the picked value
  `_v`: re-aggregation renders the carrier composite via `render_value_key`; plain association
  goes through `_resolve_agg_inputs_via_scope` + `_build_agg_render_spec_from_planned`
  (derived `Column.sql` expansion, `column_type` cast, owner-anchored `Column.filter`).
  Level 2 references `_v` plus `spec.agg_kwargs`, whose SQL names columns `_base` lacks.
- Input safety (`_assert_cross_model_inputs_safe`) runs before the association gate and
  already walks source, args, kwargs and definition-default fragments — a surviving column
  parameter is attributable from the root, i.e. constant per entity.
- Re-aggregation constituents come only from `operand_aggregates(root.source)`; the
  combined-consumer partition-key exemption (`reaggregation_operand_keys`) walks only
  `k.source`; `_AggregateArgValue` (aliased by `_AggregateKwargValue`) excludes
  `AggregateKey`, and `_bind_agg_arg` rejects an `AggCall`.
- Outer-dimension determination (`_reaggregation_determined` / `_grain_seeds_chain`) seeds a
  to-one chain only from empty-path grain members or the host PK.
- The raise ledger byte-pins checker messages; the guard ratchet classifies only
  `NotImplementedError` raises (`guards.baseline: 1` untouched here).

## Goals / Non-Goals

**Goals:** every parameter shape the one rule types as legal executes, through one pick
path; the residue raises one message; no origin-dependent branch survives in the kernel's
renderer; dimension and parameter determination share one helper.

**Non-Goals:** the attached-parameter-on-row-level-source mechanism (DEV-1859); folding the
compiler's two synthesis entry points (they define the dataset, not inspect construction);
changing key identity/aliases for definition defaults; `percentile`'s literal-only `p`;
ranked/windowed association (unchanged typed errors); stray positional values on
aggregations that declare no parameters (pre-existing behaviour).

## Decisions

1. **One determination helper, `grain_determines(key, grain, host_model, models_by_name)`**
   (`join_safety.py`): true iff `key ∈ grain`, or `key` is an aggregate whose partition keys
   ⊆ grain, or `key` is a column at path `p` seeded by a grain member at a prefix `q ⊑ p`
   (the model at `q`'s unique key, or the host-side join columns of hop `q→q+1`, lie among
   the grain members at `q`) with every hop `q…p` provably to-one. `_reaggregation_determined`
   and `_grain_expression_determined` delegate to it — a deliberate expansion for outer
   dimensions (nested-path entity keys now seed), spec'd in `queries/semantics`. Alternative
   — generalised helper for parameters only, old rule for dimensions — rejected: two
   determination notions is the same origin-flavoured fork one level down.
2. **Level-1 grain per kernel:** association = requested grain ∪ entity keys (host
   coordinates); re-aggregation = the union grain of the *source's* constituents. Parameters
   never widen the grain — a parameter grained outside it is the residue.
3. **Parameter resolution is one compiler helper**, run after input safety: for the key and
   its owner model's `Aggregation`, yield every explicit non-scalar arg/kwarg and every
   non-overridden definition default. A bare-identifier default → `ColumnKey` in the owner's
   coordinates; an expression default → a Mode-A fragment carrying the owner path
   (`PickedParam.anchor_path`), determined iff every referenced column is. Defaults stay off
   the key (folding them in would change identity and result-column names).
4. **Re-aggregation sequencing** (order matters): resolve parameters → check each against the
   union grain → append aggregate-valued parameters to `constituents` (dedup) → build the
   placeholder map and carrier → `substitute_value_keys` over the *whole* outer key (kwargs
   included) → `picked_params`. `reaggregation_operand_keys` walks a root's args/kwargs so the
   bind-stage partition-key exemption covers them.
5. **The kernel carries `picked_params: List[PickedParam]`** (`name`, `key | None`,
   `sql | None`, `anchor_path`); `null_safe` stays (a dataset property: whether NULL entity
   keys form cells).
6. **One pick path in the renderer.** A single helper renders any picked value — the source
   and each parameter — branching on the *key's type*, never on origin: `ColumnSqlKey` →
   the existing derived expansion; placeholder / column / composite → `render_value_key`
   (scope carries `attached_columns=regroup_env`); Mode-A fragment → the owner-anchored
   Mode-A entry. The declared `column_type` cast and the owner-anchored measure-local filter
   (`_expand_column_filter_sql`) wrap the source pick exactly as today. Level 2 gets
   `agg_kwargs[name] = ResolvedAggKwarg(kind="expr", value=_base._p<i>)`, so the formula and
   percentile renderers substitute the picked column and the default path is never taken.
   `_assert_association_no_column_default_params` is deleted.
7. **Two checker rules replace three gates**: `check_parameter_determined` (fired from both
   syntheses with the compiler-resolved fact) and `check_attached_param_requires_attached_source`
   (fired from `bind_query_inputs` on pre-lowering roots, next to `check_time_shift_input`:
   an `AggregateKey`-valued arg/kwarg on a root whose source is row-level). Both are ref-free
   type rules with remedies; the DEV-1859 pointer lives in the docstring only. Ledger: two rows
   out, two in.
8. **Positional parameters** bind through the same `_bind_agg_arg` and fold onto declared
   names after binding, so widening the shared `_AggregateArgValue` union covers both
   spellings; the resolver walks args and kwargs.

## Risks / Trade-offs

- [Generalised seeding flips a nested-path-seeded outer dimension from broadcast to exact]
  → spec'd and pinned with paired tests (nested-path unique-key seed / non-key seed); the
  change is strictly more correct and loses only a warning.
- [Renderer fold silently changes derived-source qualification or casts] → the pick helper
  reuses the existing expansion/cast/filter contracts; DEV-1841 exec + golden suites and a
  fold-equivalence matrix (expression/derived/cast/`*:count`/filter shapes) pin it.
- [An expression default anchored on the wrong model] → owner path carried on `PickedParam`;
  tests with host and target sharing the default's bare column name.
- [Association goldens move under the fold] → `ALLOWED_DELTAS` per key with the reason,
  re-blessed, manifest emptied.
- [NULL parameter values] → SQL `SUM` semantics inside `weighted_avg`; one scenario pins it.

## Migration Plan

Pure feature lift: previously erroring queries execute; no stored-artifact migration. One
existing behaviour changes (nested-path-seeded outer dimensions become attributable) and is
enumerated in the PR. Rollback = revert the PR.
