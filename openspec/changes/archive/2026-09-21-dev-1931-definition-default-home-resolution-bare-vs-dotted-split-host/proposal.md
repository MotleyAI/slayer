## Why

A custom aggregation's non-overridden parameter defaults are resolved into home
candidates from the **query root** today (`home.py::_default_home_candidate_paths`),
and any host-local `()` candidate is dropped. So a default that names the root
model's own column fails to widen the home to the root the way its explicit
`weight=<that column>` twin does, and the guard cannot simply be lifted because a
bare default resolved-from-root produces a bogus `()` key. Axiom 2.4 already states
the target — "defaults resolved as references from the owning model" — but carries
`[target: DEV-1931]` because the code has not flipped. This change flips it.

## What Changes

- Resolve each non-overridden definition default **from the owning model** (the
  source anchor); a qualifier the owner cannot reach forward falls back to the
  **query root** (a leading root-model name self-strips to root-local). Bare is the
  degenerate owner-local case. Applies per column reference, including inside an
  expression default.
- **Retain** a genuine host-local `()` candidate (drop the `if p` guard, keep the
  path-validity check) — now safe because bare defaults no longer produce a bogus `()`.
- Fix the input-safety path so a definition default is resolved on the **declaring
  model** even when the home widens away from it, closing a pre-existing hole where a
  fanning derived default on a widened cross-model aggregate slipped safety (silently
  multiplied result instead of failing closed).
- Flip `[target: DEV-1931]` on Axiom 2.4 and reconcile Axioms 2.4 ↔ 2.6 in
  `architecture/semantics.arc42.md` (normative harness — edited with per-change approval).

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `queries/semantics`: the *Home dataset of a row-level aggregation source*
  requirement gains the definition-default resolution frame (owning model, root
  fallback), genuine host-local `()` retention, and the safety consistency that a
  fanning definition default fails closed even when the home widens away from the
  declaring model.

## Impact

- `slayer/engine/home.py` — `_default_home_candidate_paths` / `_default_param_keys`.
- `slayer/engine/reference_closure.py` — `default_param_value_key` /
  `expr_default_ref_keys` gain optional owner+root frames (default = current
  behavior; other callers unaffected); `_default_param_specs` locates the definition
  owner via the re-rooted source anchor.
- `architecture/semantics.arc42.md` — Axiom 2.4 tag flip + 2.4/2.6 wording; add the
  DEV-1931 test to Axiom 2's enforced tags.
- New tests `tests/test_dev1931_*.py`; existing DEV-1900 / DEV-1832 home, input-safety
  and golden tests stay green (the safety fix only differs when `home ≠ anchor`).
