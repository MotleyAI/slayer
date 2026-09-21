# Tasks

## 1. Failing tests (spec-tests stage, TDD-first)
- [x] 1.1 Add `tests/test_dev1931_default_home.py` with a genuine root-local default via
  the DIRECT form: a `customers`-declared aggregation `weight="orders.amount"`, source
  `customers.spend`, rooted at `orders` → home `()`, value equal to explicit
  `weight=orders.amount`. Fails on current tree (home = `customers`).
- [x] 1.2 Add the EXPRESSION form of the same case (`weight="orders.amount * 1"`),
  exercising `expr_default_ref_keys` → home `()`, equal value.
- [x] 1.3 Add a mixed-frame expression default `weight="spend + orders.amount"`
  (owner-local `spend` + root-local `orders.amount`) → home `()`; asserts per-reference
  resolution, not whole-expression-from-root.
- [x] 1.4 Add an owner-reachable dotted default (`customers`-declared `weight="regions.pop"`)
  → resolves owner-relative `customers.regions.pop`, homes as spelling it explicitly.
- [x] 1.5 Add a bare-owner-local regression: a bare default resolves to the owner's column
  and is NOT resurrected as a bogus root-local `()` after retention (home = source's home).
- [x] 1.6 Add a multi-leaf expression-source test pinning the home when a bare default sits
  at the anchor (documents the narrowed "bare never widens" claim).
- [x] 1.7 Add fail-closed tests: a qualifier unreachable from both owner and root, and an
  ambiguous owner hop → input-safety/analyzability error, never silent `()` or anchor fallback.
- [x] 1.8 Add the F1 safety test: a `regions`-declared aggregation over `customers.regions.pop`
  with one home-widening default and one fanning derived default → fails closed naming the
  fanning `region_events` hop even though the home widened to `customers`. Fails on current tree.
- [x] 1.9 Confirm the new tests fail on the current tree (or xfail(strict)); run
  `poetry run pytest tests/test_dev1931_default_home.py -x`.

## 2. Implementation (spec-implement stage)
- [x] 2.1 `reference_closure.py`: give `default_param_value_key` / `expr_default_ref_keys`
  optional owner+root frames (default = current behavior); resolve each reference
  owner-first with root fallback ONLY on a clean owner first-hop miss, failing closed on
  ambiguous/partial resolution. Preserve qualifier tokens for the expression root retry.
  Verify: unit-level resolution over the DEV-1832/1900 fixtures.
- [x] 2.2 `home.py::_default_home_candidate_paths` / `_default_param_keys`: pass both the
  owner (anchor model + anchor path) and root frame; retain genuine `()` (drop `if p`, keep
  the path-validity check). Verify: tasks 1.1–1.6 pass.
- [x] 2.3 `reference_closure.py::_default_param_specs`: locate the definition owner via
  `source_anchor_path` of the re-rooted key (mirroring `stages.py:1930-1940`) and use
  owner-first-root-fallback. Verify: task 1.8 passes; DEV-1900 input-safety + golden green.
- [x] 2.4 Run the full non-integration suite (`poetry run pytest -m "not integration"`) and
  fix any regressions; confirm DEV-1832 home, DEV-1900 home/safety/golden, and the
  typing/reaggregation tests stay green.
- [x] 2.5 Shared per-reference resolver (option 2): `sql/column_expansion.py` gains
  `resolve_default_reference_paths` (owner-first / root-fallback per reference, forward-only
  via the revisit-guard check) + `requalify_default_references`. `reference_closure`
  consumes it (→ keys); `generator._default_frag_entry` uses it to requalify a MIXED-frame
  default and enter it at the root (single-frame keeps `_default_frag_owner_path`,
  byte-identical). Reading A supersedes DEV-1892 `wbad` → renamed `wroot` /
  `test_root_named_expr_default_widens_to_root`, asserting widening (F1 keeps the
  fanning-fail-closed pin). Deeper "resolve defaults once at bind" → follow-up.

## 3. Architecture (normative harness — per-change approval before each edit)
- [x] 3.1 `architecture/semantics.arc42.md`: Axiom 2.4 → "…from the owning model, a
  qualifier the owner cannot reach forward anchored at the query root instead." (remove
  `[target: DEV-1931]`); 2.6 append "…a qualifier the anchor cannot reach forward anchored
  at the query root."; add `[enforced: test:tests/test_dev1931_default_home.py]` to Axiom 2's
  enforced tags. Verify: present the exact diff, get approval, then `poetry run python
  tools/arch_check.py` passes.

## 4. Gates
- [x] 4.1 `poetry run ruff check slayer/ tests/` clean.
- [x] 4.2 `poetry run basedpyright` — no new errors vs baseline.
- [x] 4.3 `poetry run python tools/arch_check.py` green.
- [x] 4.4 Codex full-diff review clean (last local gate before push).
