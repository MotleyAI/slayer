# Tasks

## 1. Failing tests (spec-tests stage, TDD-first)

- [ ] 1.1 `tests/_dev1908_fixtures.py`: DEV-1900 graph + `regions → countries` (to-one,
  PK-covered; `regions.country_id`), two country rows (North → gdp 1000, South → gdp
  2000) seeded into SQLite AND DuckDB by reusing the DEV-1900 seed then adding the table
  and column. Aggregations: on `countries` `wsum_region_pop` (`regions.pop`),
  `wsum_region_pop_expr` (`regions.pop * 1`), `wsum_cust_spend2` (`regions.customers.spend`),
  `wsum_cust_spend2_expr` (`regions.customers.spend * 1`), `wsum_plan_fee`
  (`regions.customers.plans.fee`), `wsum_fan` (`regions.region_events.value`), `wsum_two`
  (`regions.pop` + `regions.customers.plans.fee`); on `region_events` `wsum_rp`
  (`regions.pop`); on `regions` `wsum_cust_plan_fee` (`customers.plans.fee`); on `orders`
  `wself` (`orders.cost`-style self-qualified, dotted and expression), `wavg_pop_expr`
  (`customers.regions.pop * 1`, second-order outer) and `wnowhere` (`nowhere.col`).
  Hand-derived oracles with a smoke re-derivation from the raw rows (500000 / 1500000
  fan, 670000, 100000, 10000, 17000000, 16000 / 48000 fan; the windowed and associate
  cells derived in the file). Verify: the smoke test passes.
- [ ] 1.2 `tests/test_dev1908_default_cancellation.py`: home paths via `Aggregate.home_path`
  and executed values (SQLite + DuckDB) equal to the explicit-kwarg twin AND the oracle
  for every scenario of the `queries/semantics` delta: single cancel, double cancel,
  cancel-then-forward (both depths), two-frame defaults, events-homed default plus its
  explicit twin and the plain expression (16000), the `ModelExtension` named-join case
  (`ship_region`, edge-name spelling survives), owner-at-root self-name (dotted and
  expression), the per-kernel expression defaults (associate by `status`; `window='1y'`
  over `customers.signup_at` months with the double-cancel expression default; second-order
  `wavg_pop_expr(sum(amount, partition_by=customers.regions.id))`), the stage-query pin.
  Fail-closed: cancel-then-fanning names `region_events`; second-order `wnowhere` raises
  the unresolvable-join error; query-typed revisit raises the circular-join error; a
  model-SQL derived column that revisits is refused (probe the exact error; if it is not
  a typed refusal STOP and raise it as an unrelated gap). Verify: fails on the current
  tree for every cancellation case; the fail-closed pins that already hold are marked so.
- [ ] 1.3 `tests/test_dev1908_walker.py`: the cancelling walk — cancel at the root, at a
  middle model, to self; an owner path containing an edge-name token
  (`orders.billing_customer.regions`) cancelling to `customers` and to `orders` with the
  original spelling returned; an edge-name token never cancels (a hop through one onto a
  visited model → `None`); precedence edge name → path model → hop; ambiguity raises; a
  miss returns `None`; plain `walk` still refuses revisits. Verify: fails (import error)
  on the current tree.
- [ ] 1.4 `tests/test_dev1908_route.py`: `attributable_from_root`, `reroot_from_root` and
  `broadcast_reason` on prefix-sharing pairs — a to-one reverse suffix is attributable
  and the key reroots to the suffix path; a fanning suffix is not attributable and the
  reason names the hop; a depth-2 shared prefix; a named edge in the target suffix;
  non-sharing pairs identical to the round trip (DEV-1910 shapes). Verify: the sharing
  cases fail on the current tree.
- [ ] 1.5 `tests/test_dev1908_golden_sql.py` + `tests/golden/dev1908_sql_baseline.json`
  via `bind_golden_tests` across the seven Tier-1 dialects: the executed shapes above
  plus the fail-closed ones recorded as raises. Verify: blessed per the divergence
  protocol; DEV-1892 / 1900 / 1931 goldens untouched.
- [ ] 1.6 `tests/test_dev1900_home_path.py::TestReverseHopDefaultFailsClosed`: replace
  the docstring sentence "reverse-hop cancellation is DEV-1908, so it must fail closed"
  with "customers is not on the path, so nothing cancels and there is no forward home"
  (assertion untouched; consent recorded 2026-09-21). Verify: test still passes.
- [ ] 1.7 Run `poetry run pytest tests/test_dev1908_default_cancellation.py
  tests/test_dev1908_walker.py tests/test_dev1908_route.py -x` and confirm the expected
  failures.

## 2. Implementation (spec-implement stage)

- [ ] 2.1 `slayer/core/join_walker.py`: add the cancelling walk (D1, D2, D4) — resolve
  the prefix into a model stack, then per token: incident edge name → hop; model name on
  the stack → truncate (keep original tokens); else model-name hop; visited-through-edge →
  `None`; ambiguity raises. Verify: 1.3 passes; DEV-1853 mirror-parity suite green.
- [ ] 2.2 `slayer/sql/column_expansion.py`: `resolve_default_qualifier_path` takes the
  root model and frame path, returns an absolute path via the cancelling walk, keeps the
  D2 split-alias raise and the partial-chain raise (D3, "resolves" includes "cancels");
  `resolve_default_reference_paths` drops the owner-model parameter and the forward
  guard; delete `_forward_valid`. Verify: unit resolution over the 1.1 graph.
- [ ] 2.3 `slayer/engine/reference_closure.py`: `_frame_dotted_key` consumes the new
  door; delete `_forward_valid`, `_dotted_key_legacy`, the legacy expression branch,
  `compute_expr_reference_columns` and its private parse helper if dead (D5, D6); root
  frame required on `default_param_value_key`, `expr_default_ref_keys`,
  `resolve_aggregation_params`, `_default_param_spec`; the parameter spec carries the
  expression default requalified into query-root coordinates (D8). Verify: 1.2 home
  paths pass; DEV-1900 closure + input-safety suites green.
- [ ] 2.4 `slayer/engine/home.py`: `_default_param_keys` / `_default_home_candidate_paths`
  follow the signatures; docstrings state cancellation; keep the path-validity filter.
  Verify: DEV-1931 and DEV-1900 home-path suites green.
- [ ] 2.5 `slayer/engine/join_safety.py`: the route primitive (D9) and its use in the
  via-host branch of `attributable_from_root`, `_reroot_leaf_via_host`,
  `broadcast_reason`. Verify: 1.4 passes; DEV-1910 back-path suite green.
- [ ] 2.6 `slayer/engine/compile/stages.py` + `slayer/ir/planned.py`: the re-aggregation
  caller passes the host as root frame (D6); `_trailing_window_kernel`, `_association_arm`
  and the second-order picked parameters reroot the expression's reference keys into the
  producer root and store the canonical fragment with no anchor path (D8); delete
  `PickedParam.anchor_path` if unread. Verify: the per-kernel cases in 1.2 pass.
- [ ] 2.7 `slayer/sql/generator.py`: `_default_frag_entry` per D7 (owner-forward raw at
  the owner, else requalified at the root); delete `_default_frag_owner_path`;
  `_render_picked_param_value` enters canonical SQL at the root; update the comments in
  `_resolve_fragment_kwargs` / `_register_fragment_kwarg_joins`. Verify: 1.2 executed
  values pass; DEV-1892 / 1900 / 1931 goldens byte-identical (a moved golden is a finding).
- [ ] 2.8 Bless `tests/golden/dev1908_sql_baseline.json` per the divergence protocol
  (1.5). Verify: golden test green on all seven dialects.
- [ ] 2.9 Run the full non-integration suite (`poetry run pytest -m "not integration"`) and
  fix regressions; confirm DEV-1847 / 1859 / 1928 / 1942 re-aggregation, DEV-1853
  mirror-parity, DEV-1909 / 1910 / 1919 / 1935 and the raise ledger stay green; any
  existing test pinning the old "unreachable" reason for a prefix-side dimension → STOP
  and ask before re-pointing.

## 3. Architecture (normative harness — exact diffs approved 2026-09-21)

- [ ] 3.1 `architecture/semantics.arc42.md`: Axiom 2.4 → "…defaults resolved as
  references from the owning model, a qualifier naming a dataset already on the owner's
  path (the query root included) cancels the path back to that dataset and reads its row
  there, any other qualifier the owner cannot reach forward anchored at the query root
  instead — only a definition default cancels; a query-typed path that revisits a
  dataset stays refused, and a model-SQL derived column whose own definition revisits a
  dataset is refused likewise `[target: DEV-1952]`."; Axiom 2.6 → "…is resolved there, a qualifier naming a dataset
  on the anchor's path cancelling back to it, any other qualifier the anchor cannot reach
  forward anchored at the query root instead."; add
  `[enforced: test:tests/test_dev1908_default_cancellation.py]` to Axiom 2's tags. Axiom 1
  unchanged. Verify: `poetry run python tools/arch_check.py` green.

## 4. Docs

- [ ] 4.1 `docs/concepts/models.md` (Custom aggregations): one sentence — parameter
  defaults resolve from the declaring model; a qualifier naming a model already on the
  query's path to it reads that row (`customers.spend` in a `regions` aggregation queried
  as `customers.regions.pop:…` weights by that customer's spend), any other qualifier the
  model cannot reach resolves from the query root. Verify: page renders; nav unchanged.

## 5. Gates

- [ ] 5.1 `poetry run ruff check slayer/ tests/` clean.
- [ ] 5.2 `poetry run basedpyright` — no new errors vs baseline; shrink in touched files.
- [ ] 5.3 `poetry run python tools/arch_check.py` green.
- [ ] 5.4 Codex full-diff review clean (last local gate before push).
