## Context

See proposal.md — Why. Today `AggregateKey.kwargs` holds only explicit arguments; a
non-overridden definition default is re-read from `Aggregation.params` by nine
production sites, and an explicit string argument naming a placeholder is re-parsed as
Mode-A SQL by the planner (`fragment_closure`, fail-open on unparseable text) and the
renderer (`_register_fragment_kwarg_joins`). Sites disagree on root frame (query host /
home / render root), parse dialect (dialect chain / render dialect) and failure policy
(planner fails closed, `_default_frag_entry` fails open); the HAVING/WHERE seam
(`_filter_agg_builder`) never resolves defaults at all.

Governing principles: system P6, P9, P10, P13 (reworded, below); engine P1, P3, P4,
P10; sql P1, P4, P5, P9; core P1, P3, P4; semantics Axioms 2.4, 2.6, 2.7, 2.8.

## Goals / Non-Goals

Goals: one resolution of every parameter, at bind; the renderer and planner consume
typed keys only; a default and its explicit twin are one key.

Non-goals: DEV-1974 (host-grain aggregate expands its derived source / `Column.filter`
outside the Mode-A door) — a separate render-scope issue. No save-time parse of default
text (decided: one analysis site). No change to explicit column kwargs' resolution
frame (root-relative, no cancellation).

## Decisions

**D1 — Binding completes the key.** In `_bind_agg`, after explicit kwargs bind, the
owner is resolved as a typed result: the host (`bundle.source_model`) walked along
`source_anchor_path(source)` (a fully-attached source anchors at the host). With no
owner (a stage source, an unresolvable hop) no definition is consulted: a custom
aggregation name fails with today's unknown-aggregation error, and a built-in gets no
defaults. For every parameter the rendered formula reads (or a formula-less built-in's
own parameters) that is not explicit, one binder module (an engine-node module, e.g.
`slayer/engine/param_binding.py`) parses the default ONCE in `bundle.dialect` and binds:
- a single column reference → `ColumnKey` / `ColumnSqlKey` (`column_default_key`) at
  its canonical absolute path: owner-first with reverse-hop cancellation, a qualifier
  the owner cannot reach anchored at the query host — the DEV-1908/1931/1954 rules,
  moved (with `resolve_default_qualifier_path` / `resolve_default_reference_paths`)
  out of `sql/column_expansion.py` into the binder module;
- a numeric/boolean literal → the same normalized scalar the explicit twin binds
  (`normalize_scalar`), emitted spelling preserved as for the explicit twin;
- anything else → a `SqlFragmentKey` (D2).
A placeholder left unbound raises today's "requires parameter" error, now at bind.
Alternatives: a sidecar of bound defaults on the elaborated term (rejected: keeps two
routes that must agree by hand and cannot dedupe twins).

**D2 — `SqlFragmentKey(template, refs)`, a new ValueKey kind (E1).** `template` is the
canonical Mode-A text (re-emitted from the parsed AST, so formatting variants intern)
with each column reference replaced by a placeholder `{r0}`, `{r1}`, …; `refs` is a
tuple of `ColumnKey | ColumnSqlKey` at canonical absolute paths. `children()` = refs;
`map_children` rebuilds refs; phase ROW. Registered in `KIND_POLICY` (not slottable,
not a composite slot), sampled by the traversal totality test, admitted to the
aggregate arg/kwarg union. Explicit arms: `render_value_key` (parse the template via
`SqlTemplate`, bind each `{rN}` to `render_value_key(ref)` in the scope — the refs are
typed keys, so the template is not free SQL with unresolved references and join
discovery stays a side effect of resolving them), `_child_keys`, the canonical kwarg
encoding (D5), `_LEGACY_KEY_SPELLINGS`. At bind the parsed text is rejected when it
contains an aggregate, window function, subquery or any non-expression node.
Alternative E2 — `ScalarCallKey("__slayer_sql", (template, *refs))` — rejected after an
independent review: it falsifies `ScalarCallKey`'s allowlist contract, forks the scalar
namespace (P10), and fails open (a probe rendered `__SLAYER_SQL('{r0} + {r1}', …)`
unguarded), whereas the new kind raised at every reachable kind-dispatch site. P13 is
reworded (approved) to: "a construct with an existing kind's shape and meaning reuses
it; anything else is a new kind, never a reserved name bending an existing kind's
contract. Every kind dispatch fails closed on a kind it does not handle."

**D3 — Explicit string fragments.** A `str` kwarg naming a formula placeholder (or any
`str` param of a formula-less built-in — today's `_is_fragment` rule) binds exactly as
D1, but its references resolve in the QUERY frame (root-relative, no cancellation), as
its unquoted twin does. Every other `str` kwarg is a marker (`window='90d'`) and is
never rendered as SQL. After binding, no string anywhere in a key tree is SQL: the
`str` branch of `_leaf_closure` is deleted.

**D4 — One home-seeding rule (α).** `home_path_for`'s `safe_reachable` carve-out for
defaults is deleted; a default is a kwarg and seeds like one. Justified by Axiom 2.4
(uniform over parameters and defaults) and 2.7 (home depends only on input paths):
probes show the explicit twin and the expression-source spelling already home at the
fanned dataset and agree; the default was the lone outlier.

**D5 — One parameter render path; names.** Every `AggregateKey` kwarg renders through
one parameter API: a scalar → a SQL literal, a `ValueKey` → `render_value_key` in the
current scope. Deleted: `ResolvedAggKwarg`'s `"str"` route, `agg_kwarg_canonical_str` as
a render path, `_resolve_kwargs` / `_resolve_agg_kwargs_for_key` column-only filters,
`_register_fragment_kwarg_joins`, `_default_frag_entry`, `_fragment_placeholders`,
`_is_fragment`, `_build_formula_agg`'s `param_defaults`, `_resolve_agg_param`'s default
branch. Every `AggRenderSpec` builder migrates: plain, expression-source, HAVING/filter,
windowed, picked-parameter, association, re-aggregation. `AggRenderSpec` carries the
formula only, never `Aggregation.params`. Naming: `canonical_aggregate_alias` encodes a
`SqlFragmentKey` as a short stable digest of its canonical template plus its refs'
canonical names, and booleans explicitly; the PUBLIC auto-name derives from the
user-written kwargs only (the binder names before filling defaults), so result columns
do not change; internal CTE / hidden-slot names derive from the full key.

**D6 — Consumers walk parameter leaves.** Every planner consumer that filtered kwargs
by `isinstance(v, (ColumnKey, ColumnSqlKey))` — `home_path_for`,
`_first_unattributable_arg_leaf` (keeps the outer parameter name for its error),
parameter typing (`_param_is_determined`), the kernels' picked parameters, the
cross-model kwarg gate, join-path registration — walks each parameter value's row
leaves generically. Deleted: `ParamSpec.expr_sql` / `expr_refs`, the default half of
`resolve_aggregation_params` and `_default_param_spec(s)`, `_default_params_closure`,
`_canonical_default_sql`, `requalify_expr_to_paths`, `_reroot_picked_expr`,
`PickedParam.sql`, `home._default_home_candidate_paths` / `_default_param_keys`,
`requalify_default_references`, `agg_registry.merge_agg_params` (dead). Kernels reroot
bound defaults exactly as explicit kwargs (fixes the trailing-window kernel's
producer-root fallback divergence).

**D7 — Bind-time fail-closed.** Unparseable text, or an aggregate / window / subquery
node → new `UnanalyzableAggregationParameterError(SlayerError)` naming model,
aggregation, parameter and text (no issue reference). Unreachable from both frames,
ambiguous or partially resolvable chain → the existing errors. A reference that
revisits a dataset without cancelling (through an edge name) → `CircularJoinPathError`.
A missing column → the explicit twin's unknown-column error.

**D8 — `window` reserved.** `check_aggregation_definition` rejects a placeholder or
declared parameter named `window` at save (mirroring `value`); the binder raises the
same typed error for a stored model. Reason: a default `window` on the key collides
with the trailing-window marker (`window_kwarg_of`).

**D9 — Law test.** `tests/test_law_param_text_bound.py` AST-scans `slayer/engine`,
`slayer/sql`, `slayer/ir` and fails on any access to `Aggregation.params` /
`AggregationParam.sql` outside the binder module, allowlisting save-time validation
(`sql_template.check_aggregation_definition`) and the binder's name lookup; it also
asserts no `str` kwarg is rendered as SQL. Tagged on engine P1.

## Risks / Trade-offs

- [A column-only kwarg filter survives somewhere and skips a `SqlFragmentKey`] →
  Axiom 2.7 spelling-invariance law test: for home, input safety, closure, join
  registration and the cross-model gate, `w=<col>` and `w=<fragment over col>` give
  the same verdict; executed E2 coverage through every render builder.
- [Internal alias churn] → goldens re-blessed per the divergence protocol; public
  names pinned by a test.
- [Widening the ValueKey union adds basedpyright errors] → fixed at the root, never
  absorbed into the baseline.
- [Behaviour changes (α, `window` reserved, bind-time errors)] → spec deltas + docs;
  no stored model in the repo, docs or demo uses `{window}`.

## Migration Plan

No storage migration. A stored model declaring a `window` parameter fails when used,
with a typed error naming the remedy (rename the placeholder).
