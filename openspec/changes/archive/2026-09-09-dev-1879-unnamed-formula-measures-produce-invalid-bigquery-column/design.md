# Design

## Context

`_canonical_alias_for_formula` (`slayer/engine/stage_planner.py:3907`) derives
the auto-name for unnamed query measures. `AggregateKey`-rooted formulas route
through `canonical_aggregate_alias(profile="stage_formula")`; everything else
falls to a text fallback whose `":" in text and "(" not in text` branch
mis-fires on arithmetic and whose `.replace()` chain misses `/`, `-`, `'`. The
result feeds `public_name` → the quoted SQL alias (`generator.py:1733`) → the
result key (`response_meta.py:121`). BigQuery's `DottedAliasManglingMixin` only
rewrites pure-`\w` dotted aliases, so the junk reaches BigQuery, which rejects
it. See proposal.md — Why.

## Goals / Non-Goals

**Goals:** valid, deterministic, dialect-uniform derived names for unnamed
non-aggregate formula measures, reusing the one existing sanitizer.

**Non-Goals:** auditing or guaranteeing validity of projection aliases from any
other source (named measures, dimensions, hidden slots); changing the error
taxonomy of the collision guard; touching dialects, `slayer/sql/naming.py`, or
the MCP surface.

## Decisions

1. **Sanitizer = `auto_name_from_expression` (`slayer/core/refs.py:46`) over
   the canonical formula text.** The Linear issue suggested the sanitizer in
   `slayer/sql/naming.py`, but that regex is CTE-only (no lowercase/hash-fold);
   `auto_name_from_expression` is the DEV-1826 product-wide convention already
   used for computed dimensions and expression-aggregation leaves — one
   convention, formatting-insensitive, hash-folded. (Approved.)
2. **Operator-dropping accepted over operator-aware encoding** (`a:sum /
   b:sum` and `a:sum * b:sum` collide): consistent with the documented DEV-1826
   convention; the existing unnamed-collision guard
   (`stage_planner.py:3822-3830`, a `ValueError` with the "set 'name'" remedy)
   turns collisions into a clear error and is kept as-is per Codex review —
   changing it to a typed error would expand scope for no user benefit.
3. **Result keys change uniformly on every dialect** rather than sanitizing
   only the emitted alias: per-dialect keys would split the naming authority
   and break the one-key contract. Deliberate **BREAKING** change, approved.
4. **Tighten the plain-`col:agg` text branch to an `AGG_REF_RE` fullmatch**
   (not substring heuristics) so it can never again capture an arithmetic
   composite; audit callers passing `bound=None`/`parsed=None`.
5. **Alias-validity scope narrowed** (Codex): the dialect battery and the spec
   requirement cover aliases derived from unnamed formula measures only, not
   every emitted alias.

## Risks / Trade-offs

- [Existing clients keyed on old malformed result keys] → breaking change is
  called out in proposal; old keys failed outright on BigQuery and contained
  `:`/`/`/spaces elsewhere.
- [Fixed battery exposes a pre-existing alias bug in another shape] → stop and
  surface before widening scope (agreed rule).
- [Existing tests asserting old derived keys] → listed and updated only with
  explicit consent in the spec-tests stage.
