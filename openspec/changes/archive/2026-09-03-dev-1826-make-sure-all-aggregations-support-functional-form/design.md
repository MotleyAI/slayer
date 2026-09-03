# Design — functional form for all aggregations

## Context

(Anchors verified after merging `egor/dev-1835…` and then `origin/main`
(2026-09-02), which brought DEV-1740/1743/1824/1825/1829/1835/1836/1837/1838/1839
into this branch.)

Two funcstyle→colon regex rewriters exist: the slack rule `FUNC_STYLE_AGG`
(`slayer/engine/normalization.py:148`, warning-emitting, covers query
measures/filters and model measures at save) and a legacy twin
(`slayer/core/formula.py:246`, used by order coercion at `core/query.py:536`/`:553`
and the cube/dbt/OSI importers). The parser `parse_expr`
(`slayer/engine/syntax.py:367`) has no functional-aggregation branch — an
un-normalized `sum(revenue)` dies as `UnknownFunctionError`
(`syntax.py:1094-1106`), per the rationale in
`docs/architecture/slack-normalization.md` — which this change reverses.
Positions that miss both rewriters (`ModelExtension` measures via
`source_bundle.py:116`, hand-authored YAML measures re-parsed at
`measure_expansion.py:190`) are the current exceptions.

Post-merge landscape this design builds on:
- **Computed dimensions** (DEV-1740/1824/1825): `dimensions` entries may be
  expressions — `ComputedDimension` (`core/query.py:501`), parsed by the
  same `parse_expr` at `query.py:658` (construction) and
  `stage_planner.py:3418` (binding); inner aggregates must carry
  `partition_by=` (guard `stage_planner.py:3358`). This is a **new
  aggregation position** the parity contract must cover.
- **Row-level expression machinery exists**: `ArithmeticKey` (`keys.py:664`),
  `ScalarCallKey` (`keys.py:692`, `iif` included — no ConditionalKey), rendered
  by `sql/render/value_expr.py` (`render_arithmetic:320`,
  `render_scalar_call:355`, iif→CASE `:596`). `CASE WHEN` is a pre-parse text
  rewrite `_rewrite_case_when` (`syntax.py:218-309`) feeding
  `_preprocess_colons` (`syntax.py:761`).
- **Computed-dimension auto-naming** exists: `_auto_name_from_expression`
  (`core/query.py:489`).
- `DOT_PATH_IN_SQL` is already retired (DEV-1743, name-only stub);
  `reject_user_dunder` is gone from `refs.py`.

See proposal.md for motivation. Interview + Codex plan review + post-merge
re-verification resolved all decisions below (no open questions).

## Goals / Non-Goals

**Goals:** parser-level equivalence (functional and colon collapse to one
`AggCall` at parse time); zero position exceptions by construction — including
computed dimensions and mixed-grain arithmetic; same-model expression
aggregation via binder desugar reusing the DEV-1740 row-level machinery.

**Non-Goals:** cross-model expression aggregation and dotted paths inside
expressions (follow-up DEV-1832); functional-primary docs flip (DEV-1830);
legacy `core/formula.py` pipeline consolidation (DEV-1831); SQL
`DISTINCT`-keyword syntax; entity-ref support for expression text; Mode-A
surfaces (unchanged).

## Decisions

1. **Parser-native, not wider normalization.** New dispatch in `_convert_call`
   (`syntax.py:1029-1106`): (1) colon placeholder → `AggCall` (`:1056`,
   unchanged); (2) NEW — `normalize_aggregation_name(func_name)` ∈
   `BUILTIN_AGGREGATIONS` and ≥1 positional arg whose first is
   ColumnRef/star-placeholder/aggregation-free scalar expr →
   `AggCall(source, agg, args[1:], kwargs)`; (3) transforms (`:1063`);
   (4) scalars (`:1078`); (5) NEW — unknown name with aggregatable first arg →
   `AggCall` candidate (binder validates — parity with `x:whatever`), else
   `UnknownFunctionError` (message updated). Alternative (extend regex rewrites
   per surface) rejected: enumeration is how today's gaps arose.
2. **`first`/`last` arbitration** by first-arg shape: contains an
   `AggCall`/`TransformCall` → transform; otherwise → aggregation. Generalizes
   the regex rule (`normalization.py:192`).
3. **Star pre-pass is token-aware, not regex** (Codex F7): alongside
   `_preprocess_colons` (`syntax.py:761`), replace `*` / `path.*` occurring as
   a call's first argument with a collision-proof placeholder before
   `ast.parse`, string-literal-safe, leaving multiplication untouched. Ordering:
   after `_rewrite_case_when` (`:218`), composing with the existing pre-parse
   chain at `parse_expr:400`.
4. **Binder validates the aggregation name globally before per-column gates**
   (Codex F3): `_validate_agg_eligibility` (`binding.py:959-1043`)
   early-returns when no gate owner resolves (`binding.py:993-995`) BEFORE the
   unknown-name gate (`:1000-1003`), so `*:bogus` escapes to SQL-gen. Split:
   always heal + validate the name first; apply PK/type/`allowed_aggregations`
   gates only when a column owns them. Unknown-agg error text also hints the
   scalar allowlist (typo UX).
5. **Retire `FUNC_STYLE_AGG` — and the quiet calls become dead code.** Delete
   the rule, helpers, warning emission, and `func_style_agg_to_colon`
   (`normalization.py:75-259`); delete all three quiet call sites — they are
   no-ops once the parser is native: `stage_planner.py:519`
   (`_parse_order_formula`), `schema_drift.py:501`, and
   `memories/resolver.py:598-602` (the latter replaced by the entity-ref
   helper, decision 8). No schema-drift rewiring needed. `core/formula.py`
   untouched — importers keep their internal rewriter (consolidation:
   DEV-1831). `MISPLACED_MEASURE` unchanged; `DOT_PATH_IN_SQL` already retired.
   Save no longer rewrites spelling (author's text preserved).
6. **Order coercion resolves functional forms at enrichment** (P4):
   `_order_formula_candidate` / `_coerce_order_column` (`core/query.py:536`/`:553`)
   stop importing the legacy rewriter; call-style text → existing
   `_FUNCSTYLE_PENDING` placeholder + `raw_formula` (original spelling), bound
   later via `_parse_order_formula` (`stage_planner.py:517`). Avoids a
   core→engine import. Construction-time filter validation
   (`_validate_dsl_user_input`, `query.py:789`) is unaffected: unknown
   functional names parse to `AggCall` candidates.
7. **Custom aggregation names may not shadow scalar functions** (Codex F1):
   extend `Aggregation._reject_transform_names` (`core/models.py:351-364`) to
   also reject `SCALAR_FUNCTIONS` members (which now include `iif`). Makes the
   exception class impossible instead of warned (alternative binder-time
   resolution rejected: context-dependent meaning).
8. **Entity refs** (`memories/resolver.py:265`, `:445-460`;
   `query_engine.py:1730` `split_agg_suffix` in the recommend path): one shared
   helper parses the ref via `parse_expr` and accepts the result only when it
   is an `AggCall` over a pure column/star source, mapping to the same
   (prefix, agg, suffix) as `split_agg_suffix`; expressions rejected. One
   interpretation everywhere (Codex F9).
9. **Expression aggregation reuses the DEV-1740 row-level machinery** (Codex
   F4/F5, simplified post-merge): the binder resolves every ref in the
   expression against the current scope (host model, or `StageSchema` outputs
   in stages) into the existing row-level `ValueKey` composites
   (`ArithmeticKey`/`ScalarCallKey`/`ColumnKey`/`LiteralKey`); extend
   `_AggregateSource` (`keys.py:471`, currently
   `Union[ColumnKey, ColumnSqlKey, StarKey]`) with that expression variant.
   Hash/equality/serialization come from the canonical bound tree, so
   formatting variants intern to one key. SQL gen renders
   `AGG(<row-level expr>)` through `render_value_key`
   (`value_expr.py:471`) — percentile/dialect hooks/custom-agg `{value}`
   templates receive the rendered expression. Boundaries: refs must resolve
   in-scope (no join hops); operands carrying `Column.filter` rejected in v1;
   nested aggs/transforms rejected.
10. **Expression-agg naming reuses `_auto_name_from_expression`**
    (`core/query.py:489`) — the computed-dimension sanitizer becomes a
    shared helper; leaf = `<sanitized expr>_<agg>` (e.g. `sum(amount - cost)` →
    `orders.amount_cost_sum`), then the existing `agg_signature_suffix` /
    `partition_by_suffix` machinery appends unchanged. Distinct expressions
    that sanitize to the same key raise a loud duplicate-key error advising a
    rename. (Replaces the earlier operator-words proposal — one auto-naming
    convention product-wide.)
11. **Expression gates/types** (D8 + Codex F6): per-column gates skipped for
    multi-token expressions; best-effort type inference on the bound tree —
    numeric-only aggs rejected on confidently non-numeric expressions, display
    classification from inferred class, default plain numeric.
12. **Computed dimensions are a parity position.** Functional spelling and
    expression aggregation work inside `ComputedDimension.expression` by
    construction (same `parse_expr`); the dimension guards
    (`stage_planner.py:3363-3417` — bare aggregate without `partition_by=`
    rejected, etc.) must fire identically for both spellings, and mixed-grain
    arithmetic (DEV-1839 grain-union broadcasting) gets functional twins.
    Covered by tests, not new machinery.

## Risks / Trade-offs

- [Scalar-typo UX: `rond(price)` now errors as unknown aggregation at binding]
  → error text lists near-miss scalars alongside available aggregations.
- [Deleting FUNC_STYLE_AGG breaks consumers asserting that warning] → release
  note; warning payload type itself remains (MISPLACED_MEASURE still uses it).
- [Scalar-name rejection can fail loading a stored model that already has such
  a custom agg] → judged near-zero occurrence; validation error is explicit.
- [Expression desugar touches `AggregateKey` shape → serialization
  compatibility] → new variant is additive; colon-form keys serialize
  byte-identically to today.
- [Fail-open visitors (the DEV-1827 lesson): sites traversing
  `AggregateKey.source` by isinstance may silently treat the new expression
  variant as a leaf] → audit `walk_value_keys` / `reroot_value_key` /
  `substitute_value_keys` / alias + serialization paths for the new variant;
  add a traversal test that fails if a source-kind is unhandled.
- [Shared auto-naming can collide distinct expressions (`sum(a+b)` vs
  `sum(a-b)`)] → loud duplicate-key error with a rename hint; never silent.
- [Order resolution shifting to enrichment could surprise construction-time
  introspection] → behavior-preserving for user-visible aliases; placeholder
  path already exists for custom aggs. Audit `_canonical_alias_for_formula`'s
  text fallback (`stage_planner.py:3607`) for functional text.

## Migration Plan

Pure code + docs change, no storage migration: stored colon-form models parse
unchanged; functional text that previously round-tripped through save as colon
now stays as written (both spellings legal on load). Rollback = revert the PR.

## Follow-up issues (created at plan time)

1. DEV-1830 — docs flip to functional-primary syntax.
2. DEV-1831 — legacy `core/formula.py` pipeline consolidation onto the native
   parser.
3. DEV-1832 — cross-model expression aggregation (dotted paths inside
   expressions, filtered-operand semantics).
