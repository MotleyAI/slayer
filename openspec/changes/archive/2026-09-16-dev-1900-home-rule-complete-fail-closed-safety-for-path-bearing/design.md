# DEV-1900 design — one dependency closure

## Context

See proposal.md — Why. Substrate facts that shape the design:

- Crossed-path discovery for aggregate inputs lives in `slayer/engine/aggregate_input_paths.py`
  as three collectors (`_collect_ref_paths`, `_collect_host_locus_source_paths`,
  `_collect_default_fragment_paths`), each deciding on its own whether to look inside a
  derived column's `Column.sql`; `_collect_default_fragment_paths` reads `AggregateKey.locus`
  to find the definition owner.
- Three partial derived-aware walkers exist besides: `filter_reachability.compute_key_join_paths`
  (re-scans expanded SQL), `_ref_effective_paths` / `_ref_sql_dependency_paths` in the compiler
  (one level, hand-patches the host into the bundle), and the anchor-local expansion in
  `column_filter_paths.compute_column_filter_join_paths`.
- The bare-path predicates: `grain_determines`, `attributable_from_root`,
  `grain_member_attributable`, `_first_unattributable_arg_leaf`, `_first_unattributable_attached_leaf`,
  the `_local_broadcasts` predicate in `_plan_regroups`, `_home_path`.
- `slayer/sql/column_expansion.expand_derived_refs_sync` already expands derived references
  recursively (cycle-guarded, `owner_path`-aware) and collects every crossed prefix into a
  `crossed_paths` sink — the sink adds prefixes only for references with a non-empty
  owner-relative path.
- `bundle_builder._collect_referenced_models` returns the source model first, so every
  hand-built `{m.name: m for m in bundle.referenced_models}` map (about twenty sites) carries
  the host in production; hand-built test bundles omit it.
- Regroup discovery pre-substitutes re-aggregation roots with placeholders, but a DEV-1859
  row-attach root reaches `crossing_local_root_predicate` unsubstituted, so its attached
  input's crossings decide today whether it becomes a host-rooted producer.
- Host-rooted producers inherit base row filters through `_conjunct_disposition` (EXISTS
  pushdown or drop-and-warn); the inline base query has no such disposition.
- Executed probe on the DEV-1840 graph + `regions → region_events` (1:N) +
  `regions.bad_pop = pop + region_events.value`: every gap in the issue renders the
  multiplying join; derived dimensions double count within a cell (North 200 vs 100);
  host-population fanning filters double count for structural references too (520 vs 420).

## Goals / Non-Goals

**Goals:** one closure every classification consumes; one aggregate-input resolver (DEV-1892
decision 3 completed); a derived reference behaves like a structural one in every position;
no new error vocabulary beyond two checker rules; goldens byte-identical.

**Non-Goals:** reverse-hop cancellation for definition defaults (DEV-1908); association
semantics for population filters (DEV-1909, which retires the interim guard); population
inference's pre-binding string walk (`population.py`, downstream guards catch fan-out);
unifying discovery's descent into attached inputs (DEV-1903).

## Decisions

1. **One closure, reusing the expansion sink.** `fragment_closure(sql, model, owner_path,
   anchor_relation, bundle)` runs `expand_derived_refs_sync` over the planner dialect chain
   with the `crossed_paths` sink and `owner_path`, returning root-relative prefixes.
   `key_closure(key, anchor_model, anchor_relation, bundle, cache)` recurses over the key
   tree with the fail-closed `_child_keys` dispatch (moved from `filter_reachability`):
   `ColumnKey` / `ColumnSqlKey` → own path prefixes plus, when the terminal column is derived
   and non-trivial, the fragment closure of its definition at the terminal; `TimeTruncKey`
   delegates to its column; `StarKey` → prefixes only; `SqlExprKey` → its stamped paths;
   `column_filter_key` → `source.path` + owner-relative stamped paths; `str` → fragment
   closure at the anchor; scalars nothing; unknown kinds raise. Alternative — a paths-only
   twin of `_process_reference_site` sharing `reference_sites` — rejected: it would have to
   agree with the expansion by hand.
2. **Closure is tri-state.** `None` when no dialect parses the definition or the expansion
   fails; consumers treat `None` as unsafe: input safety and the population guard raise
   `check_input_dependencies_analyzable` (names the aggregate/filter and the column);
   grain determination reads it as not determined; attributability as unattributable with
   its own broadcast reason; conjunct disposition as unsafe. Parseable-but-opaque
   qualifiers keep today's tolerance (the strict render door leaves them untouched);
   expression defaults keep DEV-1892's fail-closed `None` reference. Alternative — keep the
   empty fallback — rejected: `()` is indistinguishable from a proven-local fragment
   (sql.arc42 §3.9).
3. **Owner-local references need no extra prefix.** A derived column's own path prefixes
   come from the structural arm of `key_closure`; a definition default's owner-local
   references ride the source role (exempt exactly when the source is exempt, as today), so
   a default contributes only its beyond-owner crossings, prefixed with the owner path.
4. **One input resolver.** `resolve_aggregate_inputs(agg, anchor_model, anchor_relation,
   bundle, include_source)` yields source, args, kwargs, and non-overridden definition
   defaults resolved on the owner = terminal of the source path from the anchor, always;
   `ParamSpec`, `default_param_value_key`, `column_default_key`, `expr_default_ref_keys` move
   out of `compile/stages.py`. `locus` is no longer consulted for discovery; the association
   synthesis drops its `locus="host"` safety copy and the literal-1 substitution.
5. **Aggregate opacity is a resolver mode, not a universal rule.** Safety predicates (input
   safety, home path, grain determination, attributability) treat an `AggregateKey`-valued
   input as opaque — its inputs belong to its own producer (DEV-1859 decision 13).
   Discovery (`crossing_local_root_predicate`) keeps descending into attached inputs as
   today, so DEV-1859 routing is byte-identical; unifying that is noted on DEV-1903.
6. **Predicates consume closures.** `grain_determines` gains `bundle`; exact grain
   membership still wins first, else every closure path must be pinned by the same grain
   with today's re-seeding (`_column_grain_determined` per path). New
   `key_attributable_from_root(key, …, bundle)` = every closure path attributable; it
   backs `grain_member_attributable`, `_first_unattributable_arg_leaf`,
   `_first_unattributable_attached_leaf`, `_conjunct_disposition` (replacing
   `_ref_effective_paths`), and `_local_broadcasts`. `_home_path` draws candidates from
   every resolved input (ranked positional args and attached inputs excluded), paths
   verbatim. The re-aggregation arm needs no new call: a fanning derived parameter now
   fails `check_parameter_determined` (root reachability would wrongly reject a legal
   `orders.amount` weight over a grain pinned by `orders.id`).
7. **Interim population-filter guard** runs after regroup discovery on the main plan only
   (never inside producer sub-plans): when at least one host-rooted aggregate term survives
   inline in the main consumer, each original ROW-typed bound-filter conjunct whose closure
   (anchored at the host) has a path not `safe_reachable` from the host raises
   `check_population_filter_no_fanout(filter_text, hop, host)` from the checker. Windowed,
   ranked, partitioned and crossing-local aggregates compile in host-rooted producers that
   already push such conjuncts as EXISTS, so they do not trigger it. Alternative — an
   interim warning — rejected: a wrong number with a warning is still a wrong number.
8. **One model map.** `ResolvedSourceBundle.models_by_name` (source first, then referenced,
   dedup by name) replaces the exact idiom everywhere; walkers keep their `setdefault` for a
   caller-supplied root so precedence is unchanged; `models_with_host` and the hand-patched
   bundle in `_ref_sql_dependency_paths` go. Sites that filter the list on purpose stay.
9. **Ledger accounting.** Two rows in (`check_input_dependencies_analyzable`,
   `check_population_filter_no_fanout`, both `ValueError`, checker-owned); no guard-ratchet
   change (neither is a `NotImplementedError`); existing messages byte-identical.
10. **Normative edit (approved).** `architecture/engine.arc42.md` §3 principle 10 —
    "Dependencies are closures" — with the ownership-boundary exemption and the
    "unanalyzable is unsafe" clause; enforced by `tests/test_dev1900_closure.py`. The exact
    diff is shown again before it is applied.

## Risks / Trade-offs

- [DEV-1859 / DEV-1892 goldens shift under the closure] → closure is a superset of today's
  paths only where a derived column crosses a join; fixture derived columns
  (`cust_tier`, `last_status`, `derived_pop`, `north_spend`) already route identically;
  discovery descent kept (decision 5). Any shifting existing test stops the work for Egor's
  ruling.
- [Structural host-population filters that "worked" now error] → intended (the values were
  wrong); DEV-1909 lands the association semantics; the error names the remedy.
- [A legal derived parameter rejected because a dependency path is re-seeded differently] →
  exact-membership-first and per-path re-seeding with the same grain (decision 6), pinned
  by positive tests (exact grain member; fanning path pinned by the target's unique key).
- [Planner dialect chain narrower than the render dialect] → tri-state closure (decision 2)
  makes the gap fail closed instead of open.
- [Model-map sweep changes a site that relied on a host-free map] → production maps already
  carry the host (bundle builder); walkers keep `setdefault`; per-stage and overlay bundles
  tested.

## Migration Plan

Pure planner change; no stored-artifact migration. Behaviour changes are enumerated in the
spec deltas and the PR. Rollback = revert the PR.
