## Context

See proposal.md — Why. `resolve_ranking_time_key` (`slayer/engine/ranked_planner.py`)
runs inside `_ranked_kernel` / `_trailing_window_kernel` after `compile_synthesized`,
over `ordered_row_keys(producer_plan.row_slots, projection)` — public grain first, then
hidden row slots, which is how the documented "first time dimension in filters"
candidate reaches it (probe J: a filter-only `created_at` ranks a producer with no
default). The pre-compile `_assert_local_producer_inputs_safe` judges the explicit
ranking argument by `key_host_path` (raw path, so a path-less derived argument falls
through to the gated closure's hop-only message), then the gated closure, then the
source; `_assert_cross_model_inputs_safe` judges explicit arguments by closure in host
coordinates before the closure's hop-only message. Positional parameters fold onto
declared names for every aggregation except `first`/`last`, so `AggregateKey.args`
holds only a ranking column. Probes (2026-09-21, SQLite): the host-rooted default (A),
the target-rooted default (D), a fanning temporal grain dimension (H, every mode) and a
fanning time dimension all rank silently over the fanned join; their explicit spellings
are refused.

## Goals / Non-Goals

**Goals:**
- Exactly one predicate answers "does this ranking key cross an unsafe hop", closure-
  based (engine P10), applied to every candidate the resolver can pick (Axiom 2.4).
- Same messages as today (host-rooted "ranks/reads by … crosses an unproven join hop",
  target-rooted "ranks/reads by … not attributable from …", unanalysable "names derived
  column …"), same aggregate alias at both sites, no new raise site.

**Non-Goals:**
- Changing which candidate wins (the precedence list and its hidden-slot candidate are
  documented behaviour); making a derived (`ColumnSqlKey`) temporal dimension a
  candidate (`_temporal_row_dimension_key` matches `ColumnKey` only, unchanged).
- The grain disposition of a ranked producer over a fanning dimension ("ranks within
  each match", pinned by the DEV-1748 matrix) — only the ordering key is refused.
- Association support for ranked producers (DEV-1914).

## Decisions

- **D1 — One predicate, two sites (option B).** `_ranking_key_crossing(key, root_model,
  bundle, alias)` in `compile/stages.py`: `key_closure` anchored at the root; `None` →
  `check_input_dependencies_analyzable(alias, column=<the derived column>)`; the first
  closure path not `safe_reachable` from the root → `(column_leaf(key), path[-1])`. It
  replaces the explicit-argument loop in `_assert_local_producer_inputs_safe` (applied to
  `explicit_ranking_time_arg(agg)`, so the argument-before-source precedence and the
  argument-naming message are kept and now cover derived arguments), and it judges the
  resolved key post-compile in `_checked_ranking_time_key`. Alternatives rejected:
  kernel-site check only (two predicates that must agree by hand; derived explicit args
  keep the hop-only message); judging every input after compile in one function
  (reverses checker-before-planner); making the implicit key an explicit argument before
  planning (candidate order depends on the compiled producer's row slots; changes aliases
  and producer identity).
- **D2 — Judge whichever candidate won.** The resolved key is judged unconditionally:
  explicit argument, temporal row dimension, time dimension's raw column, model default.
  A fanning temporal dimension or time dimension used implicitly now fails like its
  explicit spelling. Alternative rejected: exempting grain-member candidates (keeps the
  explicit/implicit inconsistency and a silent tie-ranking over fanned rows; adds the
  hand-synced special case D1 removes).
- **D3 — Typed default.** The model default is built with `column_default_key(path=(),
  leaf=default, base=root_model)` (reference_closure.py): `ColumnSqlKey` when the column
  needs expansion, else `ColumnKey`. Rendering moves to `_derived_column_expr` with
  `cast_derived=False` on both ranked paths (plain ranked CTE and windowed `_src`), so
  no CAST is added — pinned by SQL-shape assertions (D7).
- **D4 — Dead target-rooted branch deleted.** `target_path`, `_resolves_on`,
  `_ranking_key_name` and the "not resolvable" raise go; `_synthesize_cross_model_producer`
  already re-roots the aggregate (one rooting law, Axiom 2.5) before the kernel is
  built, so both kernel builders resolve host-style on the root. ranked_planner.py is
  outside the DEV-1871 ledger; the one unit test exercising the branch
  (`test_invalid_leaf_past_first_hop_is_rejected_at_plan_time`) is deleted with it
  (consented 2026-09-21). The docstring keeps only the precedence list.
- **D5 — Existing checkers only.** `_checked_ranking_time_key(producer_plan, agg_key,
  root_model, bundle, alias, target_rooted)` resolves, then judges: host-rooted →
  `check_local_producer_inputs_safe(alias, host=root_model.name, ranked_crossings=[..])`;
  target-rooted → `check_cross_model_inputs_safe(alias, root_name=root_model.name,
  unattributable_arg_leaves=[(leaf, key_broadcast_reason(key=resolved, target_path=(),
  root_model=root_model, host_model=root_model, host_name=root_model.name, …))])`. No
  lexical `raise` is added to compile/stages.py; the ledger, guards baseline and
  legacy-arrow baseline are untouched.
- **D6 — Canonical alias at both sites (Codex).** The kernel builders receive
  `canonical_aggregate_alias(<host-coordinate agg>, profile="stage_formula")` — the alias
  the pre-compile checks already use — at all four call sites (two in
  `_synthesize_cross_model_producer`, two in the local regroup path), never the public
  alias, so explicit and implicit violations name the aggregate identically.
- **D7 — SQL-shape assertions for a derived default (Codex).** The executed
  proven-hop cases assert the emitted SQL: the `customers` join is present in the ranked
  CTE / windowed `_src`, and no `CAST` wraps the ranking expression — one plain and one
  windowed case. No golden module: the refusals are dialect-independent.
- **D8 — Windowed precedence is covered (Codex).** `ordered_row_keys` lists public
  dimensions before the time dimension, so a fanning temporal ROW dimension precedes the
  bucket's raw column even under `window=`; `check_windowed_time_axis_attributable`
  validates only the bucket. The windowed twin of the fanning-dimension scenario is a
  test row and is refused under D2; a windowed producer with only a safe bucket (its
  default crossing) keeps executing.

## Risks / Trade-offs

- [D2 flips two silently-wrong shapes to errors] → both are pinned as refusals with
  the same message their explicit spelling produces; the DEV-1748 non-temporal fan-out
  grain case is a regression guard.
- [Typed default changes the render path of a derived default] → D7 SQL-shape
  assertions; existing goldens (dev1748, dev1915) use plain defaults and are expected
  unchanged — a diff there is a finding, not a re-bless.
- [`key_broadcast_reason` on a re-rooted key] → verified by Codex: with `target_path=()`
  and `host_model=root_model` the closure is root-relative and the reason names the
  first unsafe hop (`shipments`); pinned by the target-rooted scenario.
- [A second judgement of an explicit argument post-compile] → idempotent: the resolver
  returns the explicit argument unchanged, so the pre-compile check has already raised
  or passed on the same key.
