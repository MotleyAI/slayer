## Context

See proposal.md — Why. The windowed producer CTE (`_render_window_measure_cte_from_planned`)
is `_base LEFT JOIN _src ON <null-safe grain> AND _src._w_time ∈ [bucket_end − window,
bucket_end)` followed by `GROUP BY grain` — a range self-join, not an `OVER (RANGE …)`
frame. Its aggregate node comes from the registry's `window_class` (sum/avg only) and the
checker refuses every other aggregation before planning. Kernel selection picks the
trailing-window kernel whenever `window=` is present, so a windowed `first`/`last`
reaches this CTE, not the ranked one. The association kernel already lifts
reference-bearing parameters as `PickedParam`s (`resolve_aggregation_params` on the
source owner; `MAX(<param>) AS _p<i>` per level-1 cell; `kind="expr"` kwargs into
`_build_agg`) and remaps them through the sub-plan's substitutions after compile.

## Goals / Non-Goals

**Goals:**
- One render path for a windowed aggregate: the same `_build_agg` every plain aggregate
  uses, over the `_src` value column (sql P5); the checker keeps only type rules (engine
  P9); the renderer branches on typed kernel fields, never on the aggregation name
  (engine P6).
- Windowed `first`/`last` through the existing ranked helpers, so the ranked-collapse
  contract (one NULL row for an empty grain) holds by construction.

**Non-Goals:**
- Free column references inside a custom formula body outside `{value}` and its params:
  not a supported contract today (the plain path resolves them only by accident of the
  base FROM), unchanged.
- Windowing by association (DEV-1914); window on a re-aggregation's outer aggregation.
- The plain `first`/`last` implicit-ranking-key gap (DEV-1945).
- ClickHouse goldens: not in the harness's dialect list.

## Decisions

- **D1 — Structural (option B), not a wider allowlist.** Two hand-synced lists are the
  bug class; extending both to five names leaves the next aggregation to another PR and
  keeps `median`/stats/custom refused for no semantic reason. The windowed CTE builds its
  aggregate via `_build_agg(AggRenderSpec(model_name="_src", name="_w_value", …))`; the
  registry's `window_class` / `windowable` / `window_agg_class` and the checker's agg
  allowlist are deleted (`check_windowed_key_supported` → `check_window_duration`, two
  duration checks, ledger rows re-anchored). Alternative rejected: option C (B without
  parameter lifting) leaves an allowlist-shaped residue for `corr`/`weighted_avg`/custom.
- **D2 — Column count passes `sql="_w_value"`, star count projects a constant.**
  `_build_agg` switches `count` with `sql=None` to `COUNT(*)`, which would count the
  unmatched grain row of an empty interval as 1. Column count therefore builds the spec
  with `sql="_w_value"` (the association kernel's precedent); `*:count` projects a literal
  `1 AS _w_value` in `_src` (the star never enters `ScopeFrame.resolve`, which has no
  star arm) and the outer is `COUNT(_src._w_value)` → 0 on an empty interval.
- **D3 — `first`/`last` under `window=` = ranked pick within the interval (option D).**
  The kernel gains `ranking_time_key: Optional[ValueKey]`, set by the planner through
  `resolve_ranking_time_key` (the `_ranked_kernel` call). `_src` projects the ranking
  time as `_w_rank` (`_ranked_scope_expr(..., cast_derived=False)`); the outer wraps the
  range-joined rows in `build_rank_column(partition_by=[_base grain cols],
  ranking_time=ranked_ordered(_src._w_rank, agg, dialect native nulls))`, then
  `build_ranked_pick` (`MAX(CASE WHEN rn = 1 THEN _w_value END)`) `GROUP BY` grain, cast
  via `_ranked_value_cast_type`. The LEFT JOIN's NULL row for an empty interval ranks 1
  and yields NULL, matching `sum`. NULL ranking keys order per the dialect's native
  ordering — the same deliberate choice as plain `first`/`last` (an emulated NULLS term
  inside the frame would change which row ranks first). Alternative rejected: a typed
  residue error, which would be a new deferral site against the only-ever-lowered
  `guards.baseline` with no semantic grounding.
- **D4 — Parameters via `PickedParam`, built at the kernel site.** The kernel gains
  `picked_params: List[PickedParam]`. `_trailing_window_kernel` receives the root model
  and bundle (as `_ranked_kernel` does); the owner is the source anchor path walked from
  the producer root; `resolve_aggregation_params` yields exactly the reference-bearing
  parameters (column, attached aggregate, column-naming definition default) — literals
  are omitted by construction, so `percentile`'s `p` never lifts. After the producer
  sub-plan compiles, each picked key is remapped through the sub-plan's regroup
  substitutions exactly as the association path does, so an attached-aggregate parameter
  reads the producer's row-attach column. `_src` projects `_w_p<i>` per param via
  `_render_picked_param_value` (join paths register in the `_src` scope); the spec gets
  `agg_kwargs = {literal kwargs as kind="str"} ∪ {picked → kind="expr" Column(_src._w_p<i>)}`
  with `window`/`partition_by` stripped. The aggregation definition (custom formula,
  defaults) is resolved on the source owner before crossing the `_src` boundary, since
  `_src` carries no model metadata.
- **D5 — Dialect gaps stay render-time errors.** `_build_agg`'s dialect hooks raise
  `NotImplementedError` for the plain form already; the windowed form inherits them
  unchanged. No new checker error, no new deferral site.
- **D6 — New spec capability `aggregations/trailing-window`.** No existing requirement
  states the window contract (only its compositions), so the contract gets its own home
  under the existing cross-cutting `aggregations` spec; no `index.yaml` change.

## Risks / Trade-offs

- [The `_src` projection boundary drops model metadata] → resolve the aggregation
  definition and the parameter set on the source owner before building the `_src` spec;
  golden + executed cases for a custom aggregation defined on a joined target model with
  a definition-default column parameter.
- [An attached-aggregate parameter inside the windowed producer needs the substitution
  remap] → reuse the association path's remap verbatim; executed case
  `weighted_avg(weight=qty:sum(partition_by=region), window='90d')` with a hand oracle.
- [`COUNT(*)` regression on an empty interval] → executed `window='1d'` cases for column
  count, star count, sum and last.
- [Windowed `first`/`last` reaching the plain-render guard] → kernel selection already
  prefers `window=`; golden module asserts every `first`/`last` case records SQL.
- [Stage-backed time axis resolution] → executed two-stage `count`/`first` equivalence
  against a model-backed dataset (DEV-1471 fixtures).
