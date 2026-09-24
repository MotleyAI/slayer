# Design — DEV-1903 structural cleanup after DEV-1859

## Context

See proposal.md — Why. Planned against the post-DEV-1958 tree (its D6 own-grain
rule for ranked/windowed constituents, its D9 checker rename, its `compile/shift.py`
shifted producer as the sixth `compile_synthesized` site, its `carried_attaches`
keyword). Load-bearing current state: `_plan_regroups` (`slayer/engine/compile/stages.py`)
runs `combined_consumer_aggregates` / `dimension_regroup_roots` /
`dimension_partitioned_aggregates` (`slayer/ir/bound.py`), `_bare_combined_roots`, the
`_local_broadcasts` scan and `_discover_roots`, then reconciles them (`producer_bound`,
`mixed_inline_inner`, subtractions); `compile_prebound` takes
`disable_host_rooted_isolation` + `enable_producer_regroups`, the latter hand-computed at
every producer site; `check_transform_row_leaf` + `check_time_shift_input` share
`_first_row_leaf`; `syntax.py::_convert_call` routes `first`/`last` by AST shape
(`_contains_agg_or_transform`, `_is_mixed_agg_source`).

Probe data (throwaway edits, reverted): forcing nested discovery on in every producer
passed every golden and 20311/20313 tests — the two failures are one shape
(`avg(sum(amount, partition_by=[city, region]), partition_by=product)` by
`[region, product]`, broadcast mode), where the re-aggregation outer producer's answer
keeps `partition_by=[product]` while the producer grain drops the broadcast dim, so the
sub-plan nests its own answer. Making attached inputs opaque in the discovery closure
passed every golden and the full suite.

## Goals / Non-Goals

**Goals:** one discovery walk, one producer flag with one nesting rule, one
transform-input checker, one parse node for `first`/`last` with type-based dispatch;
goldens byte-identical except the one enumerated class; net-negative planner code.

**Non-Goals:** the importers' `parse_formula` text rewrite
(`slayer/core/formula.py::_rewrite_funcstyle_aggregations`, `_AMBIGUOUS_AGG_TRANSFORMS`)
— an importer-only helper producing `FieldSpec` for dbt / Cube / OSI, never on the engine
parse path, and it expands saved measures before its shape check; any change to
DEV-1958's shifted-producer synthesis beyond where its candidates come from; any spec
behaviour change beyond the saved-measure `first`/`last` operand.

## Decisions

**D1 — One discovery walk, in `engine/compile`.** New `slayer/engine/compile/discovery.py`
with `discover_roots(prebound, *, filter_typings, scope, bundle, …) ->
List[RootDisposition]`. `RootDisposition` (frozen Pydantic): `root: ValueKey`,
`phase: Literal["row", "combined"]`, `routing: Literal["inline", "local_producer",
"target_rooted", "reaggregation", "reaggregation_constituent", "shifted"]`,
`consumer_public_names: Tuple[str, ...]` (ordered, every consuming public measure —
replaces `public_alias_by_agg`, `root_public_names`, `reagg_constituent_consumers`),
`declared_type: Optional[DataType]`, `series: Optional[bool]` (shift candidates only).
Dispositions are per consumer occurrence, first-seen order; one root may carry several
(phase, routing) pairs (dual role: inside a computed dimension AND in a measure; a
standalone re-aggregation root AND a constituent of an inline row-attach root, in either
phase); grouping dedups by the (root, phase, routing) triple and merges consumer names.
Phase is the consumer position: dimension → row; measure / order / filter → combined;
attached inputs of an inline row-attach root → row. Three golden-pinned exclusions are
explicit consumer-context rules: order-by-name and field-typed-filter references to a
computed dimension's own partitioned aggregate are row-scope; a measure-typed filter
subtree equal to a dimension key is the grouped value and is not walked; measure use and a
measure-typed filter keep a dual-role aggregate combined. Routing from the key's own path
and shape: local plain → `inline`; local partitioned, bare windowed / ranked, crossing-input
local → `local_producer`; cross-model, and a local aggregate with an unattributable grain
member (today's `_local_broadcasts`) → `target_rooted`; pure re-aggregation root →
`reaggregation`; a re-aggregation key that is an attached input of an inline row-attach
root → `reaggregation_constituent` (row); a `time_shift` occurrence → `shifted` with
`series` judged by DEV-1958's one `_series_mode` on the original key (the walk runs
pre-substitution). A row-attach root is `inline` with its attached inputs yielded, or
producer-bound with its inputs left to its sub-plan. Routing lives in `engine/compile` because it needs `bundle` / `scope` (crossing closure, attributability) — planning logic ir P1 keeps out of `ir`; `ir/bound.py` keeps data shapes only. The position half is shared: the positioned consumer traversal is `core/keys.py::walk_consumer_positions` (`walk_consumer_keys` its projection, mirrored by `substitute_consumer_keys`); the classification over it — `consumer_roots`, `PositionClasses` (row-role aggregates incl. re-aggregations, row transform roots, `combined_admits`), `position_classes`, `combined_partitioned_consumers` and `position_typing_context` (moved from `ir/prebound.py`) — lives in `elaborate_env`, consumed by the walk, by `bind_inputs`' partition-key leniency (filters untyped at bind) and by position typing, over existing arrows only. `_plan_regroups` is split into
discovery → grouping → synthesis; its `NOSONAR(S3776)` goes. Alternatives rejected: keep
the walk in `ir/bound.py` with injected predicates (classification stays split in two
places); a `(root, phase, routing)` set rather than an occurrence list (loses same-phase
dual routings and consumer names).

**D2 — Universal attached-input opacity.** The walk judges each node by its own closure;
`aggregate_input_closure` loses `descend_aggregates` (opaque always) and
`local_crossing_input_paths` goes. A local aggregate whose attached input's inner crosses a
join is inline, the input row-attached through its own producer (target-rooted or local),
instead of today's host-rooted "crossing local root" producer — Axiom 2.3 as written. The
one intended non-identical plan class; value-identical; the probes show no golden moves
(any that does is recorded in `divergences.md` here). Coverage: a crossing attached source,
a crossing attached kwarg, a crossing attached transform, a `Column.filter` dependency
riding `ColumnSqlKey`, and the DEV-1910 `locus="host"` wrap (no second producer, its
safety check still runs). Alternative rejected: reproduce today's descent inside the walk
(keeps the category error as a special case).

**D3 — No producer flag: two typed entry points over one core; one nesting rule.**
`disable_host_rooted_isolation` and `enable_producer_regroups` are deleted from every
signature, and no boolean or nullable discriminator replaces them. "Top level" means one
user-authored query stage: `plan_stages` sends every authored DAG stage (non-root stages
included) through `plan_query`; only compiler-synthesized producers use
`compile_synthesized`. Entry points and environment types: `elaborate_query` returns
`ElaboratedStage` (`query: SlayerQuery`); new `elaborate_synthesized(prebound, …)` returns
a distinct `ElaboratedProducer` (`query: StrictQueryCarrier`) and never splits filters;
`compile_query` accepts only `ElaboratedStage`, so a producer environment is rejected
statically and at Pydantic validation; `compile_synthesized` owns producer elaboration and
never exposes an `ElaboratedProducer`. The private core in `stages.py` (`compile_prebound`
retired as a public name): two routing wrappers, `_route_top_level(…)` and
`_route_producer(…, context: ProducerContext)`, share one discovery → grouping →
substitution implementation. `ProducerContext` (frozen Pydantic) carries
`enclosing_grain: Grain` (required; derived by `compile_synthesized` from the producer's
projected grain, never supplied by a caller), `population: InheritedPopulation |
NoInheritedPopulation` (the latter carrying a reason, e.g. target-rooted: the cross-model
producer re-roots and disposes the inherited filters itself), `carried_attaches` and
`reserved_placeholders`. `compile_synthesized` requires `population` as a keyword with no
default; each of the six sites states it explicitly (wrap, shifted, local regroup,
re-aggregation outer and carrier inherit the parent's disposition; the cross-model producer
passes `NoInheritedPopulation`). `_emit_planned` does projection and final plan assembly; it
may invoke `compile_synthesized` for the existing late wrap and shifted-producer paths once
their projection / slot facts exist, and never re-runs a top-only step. The once-per-stage
steps live only in the top-level path: the host-rooted filter split (in `elaborate_query`),
population-filter disposal and the redundant query-grain `partition_by` strip (before
routing), and `_assert_total_routing` (after routing). The six caller-side predicates and
`_answers_need_nested_regroups` are deleted; nested discovery always runs. The one nesting decision, in `_route_producer`: a row-phase root always nests (a row attach is never an inline aggregate); a combined root at exactly `context.enclosing_grain` compiles inline, except DEV-1958 D6 (a ranked / windowed strict constituent of a composite answer nests at own grain) and the windowed-transform-input clause; any other combined root nests — a strict subset broadcasts back (carrier constituents included), and a constituent with a grain member outside the producer grain nests at its own grain (its complete-grain join-back fails closed while the producer lacks that member) — except the producer's own answer, whose outside members are the synthesizer's disposition (broadcast-dropped or functionally pruned) and which compiles inline; `_route_top_level` nests every discovered root. This closes the probe hole without rewriting any key (the outer answer's `[product]` was broadcast-dropped → inline, today's SQL) and never aggregates a constituent at a grain other than its own. Alternatives rejected: one
`in_producer: bool` (a mode flag any caller can set wrongly); `enclosing_grain:
Optional[Grain]` (the same flag as nullable data); one shared environment type (lets a
producer environment reach `compile_query`); bare `population_filters=None` (hides why
there is no population); normalising every producer answer's `partition_keys` to the
producer grain (Codex) — erases carrier constituent grains and risks alias / type / slot
lookups keyed by the original key.

**D4 — One transform-input checker, per node.** `check_transform_inputs(*, roots,
projected_grain_keys)` in `elaborate_env` replaces `check_transform_row_leaf` and
`check_time_shift_input` / `_check_shift_family_key`: one pre-order walk over
`walk_value_keys(root)`; a per-op table `_TRANSFORM_INPUT_RULES` naming the ops that
reject a boolean-shaped input (`change`, `change_pct`); the row-leaf rule total over every
op. `_first_row_leaf` treats a nested transform as opaque — that node is judged on its own
visit — so the innermost consuming transform is named and the `first` / `last` exemption
disappears as a rule (a `first` / `last` transform's input is attached-typed by D5).
Observable consequence: `first(cumsum(weight))`, `rank(cumsum(weight))` and
`time_shift(cumsum(weight), -1)` all name `cumsum`. Messages byte-identical to the
post-1958 text; both ledger rows re-anchored; `bind_inputs` makes one call.

**D5 — `first` / `last`: one parse node, bind-time dispatch by type.** `_convert_call`
always emits `AggCall` for `first(x)` / `last(x)` (engine P2 becomes literally true);
`_FIRST_LAST`, `_is_mixed_agg_source` deleted; `_contains_agg_or_transform` kept only for
the unknown-name rung. New `core/keys.py::is_attached_source(source)` (constituents
present, no row leaf), reused by `is_reaggregation_key`. In binding, a ranked-aggregation
`AggCall` takes a dedicated operand path: the operand binds with the same `alias_map` /
`measure_ctx` / `dim_alias_map` every transform input gets (`_bind_transform` split into an
input-binding half and a parameter-binding half); attached bound operand →
`TransformKey(op, input=…)` with args / kwargs through the transform parameter binder;
row-grain bound operand → the ranked `AggregateKey` through the ordinary aggregation
source validation (eligibility gate, positional fold, expression-source rule) — a bound
operand carrying a measure reference is attached by construction, so no measure ever
enters an aggregation source. A transform operand is attached by construction too
(`operand_constituents` treats a `TransformKey` as opaque, Axiom 2.3), so dispatch never
pre-empts transform-input validation: `first(cumsum(weight))` reaches the checker and
fails naming `cumsum`, never the expression-source error. The oracle for every operand
shape is parity with `cumsum(<same operand>)`: `first(X)` binds to the transform exactly
when `cumsum(X)` binds, and raises the same error when `cumsum(X)` raises — including a
dotted cross-model saved measure, a saved formula that is itself a transform or a
composite, a recursive saved measure, a selected-measure alias in filter / order position,
and an ineligible unselected saved measure there (still rejected; no broadened
eligibility). Consequence: `first(rev)` over a saved measure is the
transform (spec delta). Shapes checked unchanged: `last(balance, updated_at)`,
`first(sum(x))`, `first(sum(x) + 1)`, `first(quantity * avg(...))`, `first(cumsum(weight))`,
`first(revenue:sum > 100)`, `first(sum(x), partition_by=…)` (transform kwarg error),
`first(1)` (expression-source error).

**D6 — DEV-1958 alignment.** `plan_shifted_producers` consumes the walk's non-series shift
candidates instead of walking; synthesis (after `_plan_regroups`, carried attaches, slot
ids, staging's per-slot `series`) stays as landed. The shifted producer is the sixth
`compile_synthesized` site.

**D7 — Architecture.** Node principles applied: engine P1, P2, P3, P6, P9, P10; ir P1, P2;
core P3; sql P10; semantics Axioms 2.3, 9, 11.3b, 11.4. Two arc42 edits, each presented as
an exact diff for explicit approval before landing: `architecture/ir.arc42.md` §1 drops
"and their discovery walks"; `architecture/engine.arc42.md` P2 gains
`[enforced: test:tests/test_dev1903_first_last_dispatch.py]`. `compile/discovery.py` sits inside the declared `compile` child and the shared position classification in `elaborate_env`; no `index.yaml` or `.c4` change.

## Risks / Trade-offs

- [Always-on nested discovery changes a producer's plan] → probe A (every golden) + the
  D3 nesting rule + the probe-hole test; any divergence follows the ledger protocol.
- [Opacity re-routes an uncovered shape] → D2's coverage matrix; `divergences.md` if a
  golden moves.
- [Bound-level dispatch admits contexts the aggregation path forbids] → the row-grain arm
  re-runs the ordinary aggregation source validation; a measure reference is attached by
  construction.
- [Nested-error attribution changes] → consented re-pins (tasks 1.9–1.10); spec text
  ("names the transform") stays true.
- [DEV-1958 merge conflicts in `stages.py` / `elaborate_env.py`] → merge origin/main first
  (task 1.0 / 2.1), never rebase; 1958's tests are the regression gate.

## Migration Plan

Pure planner / checker / binder change behind the existing query surface; rollback =
revert the PR. Internal entry-point split only; no storage or API migration.
