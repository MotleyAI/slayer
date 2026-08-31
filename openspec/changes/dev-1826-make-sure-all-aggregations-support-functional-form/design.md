# Design — functional form for all aggregations

## Context

Two funcstyle→colon regex rewriters exist today: the slack rule `FUNC_STYLE_AGG`
(`slayer/engine/normalization.py:152`, warning-emitting, covers query
measures/filters and model measures at save) and a legacy twin
(`slayer/core/formula.py:246`, used by order coercion at `core/query.py:750` and
the cube/dbt/OSI importers + schema drift). The parser `parse_expr`
(`slayer/engine/syntax.py`) deliberately rejects functional aggregations
(`syntax.py:978`), per the rationale in `docs/architecture/slack-normalization.md:127`
— which this change reverses. Positions that miss both rewriters
(`ModelExtension` measures via `source_bundle.py:116`, hand-authored YAML measures
re-parsed at `measure_expansion.py:190`) are the current exceptions. See
proposal.md for motivation. Branch = DEV-1826; interview + Codex plan review
resolved all decisions below (no open questions).

## Goals / Non-Goals

**Goals:** parser-level equivalence (functional and colon collapse to one
`AggCall` at parse time); zero position exceptions by construction; same-model
expression aggregation via binder desugar.

**Non-Goals:** cross-model expression aggregation and dotted paths inside
expressions (follow-up issue); functional-primary docs flip (follow-up issue);
legacy `core/formula.py` pipeline consolidation (follow-up issue); SQL
`DISTINCT`-keyword syntax; entity-ref support for expression text; Mode-A
surfaces (unchanged).

## Decisions

1. **Parser-native, not wider normalization.** New dispatch in `_convert_call`
   (`syntax.py:913`): (1) colon placeholder → `AggCall` (unchanged); (2) name
   healing via `normalize_aggregation_name` ∈ `BUILTIN_AGGREGATIONS` and ≥1
   positional arg whose first is ColumnRef/star-placeholder/aggregation-free
   scalar expr → `AggCall(source, agg, args[1:], kwargs)`; (3) transforms;
   (4) scalars; (5) unknown name with aggregatable first arg → `AggCall`
   candidate (binder validates — parity with `x:whatever`), else
   `UnknownFunctionError`. Alternative (extend regex rewrites per surface)
   rejected: enumeration is how today's gaps arose.
2. **`first`/`last` arbitration** by first-arg shape: contains an
   `AggCall`/`TransformCall` → transform; otherwise → aggregation. Generalizes
   the current regex rule (`normalization.py:196`).
3. **Star pre-pass is token-aware, not regex** (Codex F7): extend the existing
   colon preprocessing (`syntax.py:652`) to replace `*` / `path.*` occurring as a
   call's first argument with a collision-proof placeholder before `ast.parse`,
   string/comment-safe, leaving multiplication untouched.
4. **Binder validates the aggregation name globally before per-column gates**
   (Codex F3): `_validate_agg_eligibility` (`binding.py:972`) currently
   early-returns when no gate owner resolves (`binding.py:1007`), so `*:bogus`
   escapes to SQL-gen. Split: always heal + validate the name first; apply
   PK/type/`allowed_aggregations` gates only when a column owns them. Unknown-agg
   error text also hints the scalar allowlist (typo UX).
5. **Retire `FUNC_STYLE_AGG` user-facing only** (Codex F2 resolution): delete the
   rule, its helpers, `func_style_agg_to_colon`, and the warning from
   `normalization.py`; drop the quiet call at `stage_planner.py:667` (parser
   covers it); switch `schema_drift.py:602` to
   `core.formula._rewrite_funcstyle_aggregations` (the legacy pipeline it
   belongs to); `memories/resolver.py:599` moves to the entity-ref helper (D8).
   `core/formula.py` itself is untouched — importers keep their internal
   rewriter. `MISPLACED_MEASURE` / `DOT_PATH_IN_SQL` unchanged. Save no longer
   rewrites spelling (author's text preserved).
6. **Order coercion resolves functional forms at enrichment** (P4):
   `_order_formula_candidate` / `_coerce_order_column` (`core/query.py:739`)
   stop importing the legacy rewriter; call-style text → existing
   `_FUNCSTYLE_PENDING` placeholder + `raw_formula` (original spelling), bound
   later via `parse_expr` (machinery already exists for custom aggs). Avoids a
   core→engine import. Construction-time filter parse (`core/query.py:1079`)
   works unchanged: unknown functional names parse to `AggCall` candidates.
7. **Custom aggregation names may not shadow scalar functions** (Codex F1):
   extend the transform-name rejection at `core/models.py:333` to
   `SCALAR_FUNCTIONS`. Makes the exception class impossible instead of warned
   (alternative binder-time resolution rejected: context-dependent meaning).
8. **Entity refs** (`memories/resolver.py`, `query_engine.py:2100`): one shared
   helper parses the ref via `parse_expr` and accepts the result only when it is
   an `AggCall` over a pure column/star source, mapping to the same
   (prefix, agg, suffix) as `split_agg_suffix`; expressions rejected. One
   interpretation everywhere (Codex F9).
9. **Expression aggregation = bound row-phase tree, not text** (Codex F4/F5):
   the binder resolves every ref in the expression against the current scope
   (host model, or `StageSchema` outputs in stages), producing a bound scalar
   tree stored on `AggregateKey` as a new expression-source variant
   (`keys.py:459` union extended); hashing/equality/serialization derive from
   the canonical bound form, so formatting variants intern to one key. SQL gen
   renders `AGG(<row-level expr>)` through the existing row-phase value
   renderer; percentile/dialect hooks/custom-agg `{value}` templates receive the
   rendered expression. Boundaries: refs must resolve in-scope (no join hops);
   operands carrying `Column.filter` are rejected in v1; nested
   aggs/transforms rejected. Naming (P1): operator→word map
   (plus/minus/times/div/mod), other punctuation → `_`, collapse repeats,
   digit-lead prefix `_`; segment > 40 chars → first 32 + `_` + 8-hex BLAKE2 of
   canonical text; existing `agg_signature_suffix` / `partition_by_suffix`
   append unchanged. Stage naming uses the stage namespace (`flat_name`
   machinery).
10. **Expression gates/types** (D8 + Codex F6): per-column gates skipped for
    multi-token expressions; best-effort type inference on the bound tree —
    numeric-only aggs rejected on confidently non-numeric expressions,
    display classification from inferred class, default plain numeric.

## Risks / Trade-offs

- [Scalar-typo UX: `rond(price)` now errors as unknown aggregation at binding]
  → error text lists near-miss scalars alongside available aggregations.
- [Deleting FUNC_STYLE_AGG breaks consumers asserting that warning] → release
  note; warning payload type itself remains (other rules).
- [Scalar-name rejection can fail loading a stored model that already has such
  a custom agg] → judged near-zero occurrence; validation error is explicit;
  DECISIONS.md records it.
- [Expression desugar touches `AggregateKey` shape → serialization
  compatibility] → new variant is additive; colon-form keys serialize
  byte-identically to today.
- [Order resolution shifting to enrichment could surprise construction-time
  introspection] → behavior-preserving for user-visible aliases; placeholder
  path already exists for custom aggs.

## Migration Plan

Pure code + docs change, no storage migration: stored colon-form models parse
unchanged; functional text that previously round-tripped through save as colon
now stays as written (both spellings legal on load). Rollback = revert the PR.

## Follow-up issues (created at plan time)

1. Docs flip to functional-primary syntax.
2. Legacy `core/formula.py` pipeline consolidation onto the native parser.
3. Cross-model expression aggregation (dotted paths inside expressions,
   filtered-operand semantics).
