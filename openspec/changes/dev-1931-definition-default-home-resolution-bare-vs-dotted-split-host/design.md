## Context

See proposal.md — Why. Two resolution frames exist today for a custom aggregation's
definition defaults, and they disagree. `home.py::_default_home_candidate_paths`
resolves every default from the query root (`owner_path=()`, `owner_model=host_model`)
and drops `()` candidates; the input-safety closure
(`reference_closure.py::_default_param_specs`) looks the definition up on the *home*
model with `owner_path=()`. The parameter-typing caller (`compile/stages.py:1930-1940`)
already does it right — `owner_model = walk(host, source_path)`, `owner_path=source_path`
— and is the template the other two paths should follow. A custom aggregation's
declaring model is always its source anchor (`binding.py::_resolve_agg_owner`,
`home.py:55`), so "owning model" = "source anchor".

## Goals / Non-Goals

**Goals:**
- Definition defaults resolve from the owning model, root fallback for
  owner-unreachable qualifiers; genuine host-local `()` retained.
- Home computation and input-safety use the *same* definition-owner resolution.
- Flip `[target: DEV-1931]` on Axiom 2.4; keep every DEV-1900/1832 test green.

**Non-Goals:**
- No full unification of all default-resolution callers into one helper (typing and
  reaggregation keep their current behavior; the new frame is opt-in).
- No leaf-existence validation (a default naming a valid path but a missing column
  keeps failing at SQL generation, as today).

## Decisions

- **Owner-first, root fallback (Option B) over strict bare→owner/dotted→root
  (Option A).** On the current model graph A and B are behaviorally identical (bare
  defaults never move the home; only ancestor/root-side dotted defaults widen, via the
  root under both). B is chosen because it *is* Axiom 2.4's "from the owning model," so
  flipping the tag is honest, and it carries the true owner-relative key (robust to a
  hypothetical name that is both a root child and an owner descendant).
- **Fallback only on a clean owner first-hop miss.** A qualifier whose first hop is not
  a join of the owner falls back to the root. A qualifier whose first hop resolves but a
  later hop is missing, or whose hop is ambiguous, fails closed — never silently
  re-anchors at the root. (Mirrors `column_expansion._raise_if_broken_join_walk` /
  `_resolve_qualifiers`.)
- **Per-column-reference resolution inside expression defaults.** Each column reference
  in an expression default is resolved independently (owner-first, root fallback), so
  `spend + orders.amount` resolves `spend` owner-local and `orders.amount` root-local.
  Implemented by resolving the reference set in the owner frame and, for references the
  owner cannot reach, in the root frame — the qualifier tokens must be preserved for the
  root retry (today `compute_expr_reference_columns` collapses an unresolved reference to
  `(None, leaf)`).
- **Retain genuine `()`.** Drop the `if p` guard in `_default_home_candidate_paths`; keep
  the `walk_key_path(...) is not None` path-validity check. Safe because bare defaults
  now resolve owner-local instead of producing a bogus root `()`.
- **Fix the safety path, don't leave it.** `_default_param_specs` must locate the
  definition owner via `source_anchor_path` of the re-rooted key (as `stages.py:1930-1940`
  does), not use the home model with `owner_path=()`. This closes a pre-existing hole: a
  fanning *derived* default on a cross-model aggregate whose home widens away from the
  declaring model slipped safety (home checks a default's path, not its closure; safety
  then looked up the definition on the wrong model and omitted it). The fix only differs
  from current behavior when `home ≠ anchor`, so existing safety/golden tests are
  unaffected.
- **Opt-in frame parameters.** New owner+root frame arguments on
  `default_param_value_key` / `expr_default_ref_keys` default to current behavior, so the
  typing and reaggregation callers are unchanged unless they pass the new frame.

## Risks / Trade-offs

- [Retaining `()` widens the home wrongly if any resolver still yields a bogus `()`] →
  bare defaults resolve owner-local first; add regression tests asserting a bare default
  is never resurrected as `()`.
- [Changing a shared resolver leaks into typing/reaggregation] → new frame is opt-in;
  run the full non-integration suite and confirm those tests stay green.
- [`walk_key_path` conflates missing/ambiguous/cyclic as `None`, so a naive fallback
  could re-anchor a malformed reference] → gate the fallback on a clean first-hop miss;
  fail closed on ambiguity/partial resolution.

## Migration Plan

Behavior-only change to home resolution and input safety; no data or config migration.
Rollback is a straight revert. arc42 edits (normative harness) are applied with
per-change approval.
