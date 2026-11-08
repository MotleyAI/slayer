## Context

See proposal.md — Why. The source-constituent position was generalised to transforms by
DEV-1832 through one classifier (`operand_constituents` / `attached_inputs` in
`slayer/core/keys.py`), but the argument positions still admit `AggregateKey` only at
every hand-kept site: the binder (`_bind_agg_arg`), the key union
(`_AggregateArgValue`), the alias fragment (`refs.agg_kwarg_canonical_str`), the two
keys.py rewrites (`normalize_transform_constituents`, `lower_collapsing_constituents`,
both looping over `operand_constituents(rebuilt.source)`), the time-axis check
(`elaborate_env._temporal_axis_transforms`), the parameter resolver
(`reference_closure.resolve_aggregation_params`), the lenient attached set
(`keys.attached_operand_keys`, aggregates only), and the two determination predicates
(`stages._home_determines_grain_member`, `stages._param_is_determined` →
`join_safety.grain_determines`), which have column and aggregate arms only.

Throwaway probe (2026-09-21, reverted): with bind, the union, the alias fragment and the
resolver lifted, the pinned ranked-transform cells pass (67.5 broadcast; 59.375 / 97.5 /
40 by tier every mode), as do positional, filter, ORDER BY, local-root, determined-cumsum
and windowed-outer shapes; the remaining failures are exactly the sites above — no
determination arm (broadcast SQL fails at execution; error mode hits the engine-level
dimension refusal), the axis check skipped, the ungrained inner unnormalised (empty
projection by `status`), `last` not lowered (wrong grain), association returning NULL on
SQLite / a binder error on DuckDB, and the re-aggregation outer leaking scope.

## Goals / Non-Goals

**Goals:**
- One attached-input law: every rewrite and check that classifies an aggregation's
  attached values covers source, args and kwargs through the one classifier.
- One attached-parameter grain consumed by both determination checks.
- Every scenario in the delta executes or fails closed with the existing typed error;
  no new error family, no new kernel.

**Non-Goals:**
- Expression-valued parameters (`weight=qty * 2`) — still refused at bind.
- A transform as a ranked `first`/`last` ranking key — still refused ("ranks by a column").
- The `TimeTruncKey` `str()` rendering inside aggregate-parameter alias fragments.
- Widening the home through attached parameters (DEV-1919 D2b's rejected alternative).

## Decisions

- **D1 Bind.** `_bind_agg_arg` binds a `TransformCall` through `_bind_transform` with
  `dim_alias_map` (no `alias_map` / `measure_ctx`: a measure is not legal inside an
  aggregation). `_AggregateArgValue` admits `TransformKey`; the refusal tail stays for
  every other kind, reworded to list grained transforms. `_fold_positional_agg_args` is
  unchanged: a positional transform folds onto the declared name (DEV-1919 D8), so after
  bind an attached parameter is always a kwarg. Alternative rejected: binding transforms
  through `_bind` generically — it would admit composites the argument union refuses.
- **D2 One attached-input rewrite.** `keys.map_attached_inputs(key, fn)` applies `fn` to
  each top-level attached constituent of an `AggregateKey` (source, args, kwargs — the
  same enumeration as `attached_inputs`) and substitutes per position with
  `substitute_value_keys`; partition keys are never rewritten. `normalize_transform_constituents`
  and `lower_collapsing_constituents` use it. Both stay post-order over `map_children`:
  a node rewrites only its own top-level attached inputs; a transform nested deeper was
  rewritten when its owning aggregate was visited, and the enclosing node then sees
  that aggregate as one opaque constituent (Axiom 2.5). Consequences: an ungrained,
  non-windowed, local inner of a transform parameter is grained at the query grain
  (11.1); a collapsing `first`/`last` parameter becomes `max(t, partition_by=<grain −
  axis>)` (11.3b) so every downstream check sees the collapsed grain.
- **D3 Axis check.** `_temporal_axis_transforms` (measure / filter / order roots)
  iterates `attached_inputs(k)`; a time-ordered transform parameter whose grain lacks its
  axis fails with the existing time-axis error in every position and mode.
- **D4 Parameter resolver and kernels.** `resolve_aggregation_params` admits a
  `TransformKey` value as an attached `ParamSpec`. The association arm skips the
  column-typing `check_parameter_determined` for attached kinds (`AggregateKey`,
  `TransformKey`; both are judged by D5) and lifts the transform as a `PickedParam`
  rerooted into the home; the trailing-window kernel picks it per interval row; the
  re-aggregation outer appends it to the carrier's constituents without the ungrained
  normalisation an aggregate parameter gets (its grain is explicit after D2).
- **D5 One attached-parameter grain.** `keys.attached_parameter_grain(key, *,
  projected_dim_keys, projected_td_keys, active_bucket) -> Optional[Grain]`:
  `AggregateKey` → its `partition_keys` (`None` = ungrained, typed at the enclosing
  grain and determined by construction — DEV-1859 decision 12, unchanged; a lowered
  collapsing transform always carries explicit keys); `TransformKey` →
  `constituent_grain(...)`. Both `_check_attached_params_determined` (DEV-1919 D2b, the
  cross-model producer's shared tail, every mode; members judged by
  `_home_determines_grain_member`) and `_param_is_determined` (re-aggregation outer vs
  the union grain; members by `grain_determines`) resolve the parameter to that grain
  first; `grain_determines` / `_home_determines_grain_member` gain no transform arm. The
  projected keys and `prebound.main_time_key` are threaded from
  `_ProducerSynthesisContext` and the re-aggregation synthesis. Ruled consequences
  (Egor, 2026-09-21): `weight=rank(sum(amount))` by `status` is refused while
  `weight=sum(amount)` by `status` stays allowed; a windowed inner's bucket joins the
  grain. Alternative rejected: extending the decision-12 exemption to a transform over
  ungrained inners — contradicts 11.1 and the spec sentence "a transform is determined
  iff its result grain is".
- **D6 Lenient attached set.** `attached_operand_keys` collects aggregates AND
  transforms nested in an attach-owning root's inputs, so a transform parameter's own
  rank-family `partition_by=` (and, by the same change, a source constituent's) is exempt
  from the combined-consumer partition-key rule (Codex finding, 2026-09-21).
- **D7 Alias fragment.** `agg_kwarg_canonical_str` gains a `TransformKey` arm:
  `<op>` + `_<kw>_<val>` per scalar kwarg (sorted) + `_` + input fragment
  (`agg_kwarg_canonical_str` for a column / aggregate / transform input, else the
  sanitised `legacy_key_str`) + `_by_<sorted partition-key displays>` when the transform
  declares partition keys + `_<time-key column display>_<granularity>` when `time_key`
  is set (identity includes the time key). Identity only; render reads the placeholder
  or picked column.
- **D8 Citations.** The two `xfail(strict=True, …DEV-1903)` markers in
  `tests/test_dev1919_home_rooted_attached_inputs.py` are removed (no other edit to that
  file); the three "(Target behaviour, deferred to …)" placeholders in the requirement are
  gone in the delta. DEV-1903 carries a comment (2026-09-21) marking its section 5
  superseded.
- **D9 arc42 (edit approved by Egor 2026-09-21 — apply exactly this).**
  `architecture/semantics.arc42.md` Axiom 11.4: "as an aggregation-source constituent it
  is an opaque dataset at its result grain (2.3)" → "as an aggregation-source
  constituent or aggregation parameter it is an opaque dataset at its result grain
  (2.3)"; append `[enforced: test:tests/test_dev1946_transform_parameter.py]` after the
  existing Axiom 11 `enforced:` tags.

## Risks / Trade-offs

- [The lenient attached set now includes transforms; a rank-family `partition_by=` that
  names a non-operand-grain member could slip past the query-dimension rule] → the
  transform producer's own grain check still requires a rank partition key to be an
  operand-grain member (Axiom 11.2); scenario "Transform parameter with its own
  partition_by outside the query dimensions" pins the legal shape, and the full unit
  suite guards the source-constituent twin.
- [A missed substitution leaves the transform's alias fragment as parameter text in the
  rendered SQL] → plan-shape assertions: the association and trailing-window kernels
  carry a `PickedParam` named `weight`, the re-aggregation carrier lists the transform
  constituent, `assert_scope_closed` + no `__regroup__` leak, and every scenario pins
  literal expected constants beside its oracle.
- [Goldens] → two new dev1859 cases blessed with `SLAYER_UPDATE_GOLDEN=1`; no existing key
  is expected to move (a moved key means an unplanned alias change — stop and report).
- [NULL-valued cells under `rank`] → both backends rank a NULL cell last under DESC (the
  Void region in the sales graph); oracles model it as rank = 1 + the number of non-NULL
  cells, per semantics arc42 §4 (database-level ordering accepted, not closed).
- [A scenario trips an unrelated gap in spec-tests] → stop and ask (never narrow, never
  xfail, never file an issue unprompted).
