Fixture (`tests/_dev2101_fixtures.py`): orders 1 EU c1 paid 10 Jan · 2 EU c1 paid 20 Jan · 3 US c2 Pending 30 Feb · 4 US c2 paid 40 Feb · 5 NULL-region c3 void NULL Mar; customers 1 A gold · 2 B silver · 3 C gold. Expected values below are for this fixture.

## 1. Tests (pr-tests stage — all fail before the fix unless marked "stays green")

- [x] 1.1 Add the custom aggregations `rows` (`COUNT(*)`) and `per_order` (`SUM({value}) / COUNT(DISTINCT id)`) and the saved measure `paid_rows` (`rows(amount) FILTER (WHERE status = 'paid')`) to `orders_model()` in `tests/_dev2101_fixtures.py`; verify the whole existing `tests/test_dev2101_*` suite stays green.
- [x] 1.2 Create `tests/test_dev2186_custom_filter.py` (SQLite + DuckDB via `make_exec_engine`, unit). Cover the value scenarios below and verify each fails on the current code where it reads a formula-internal row:
  - `rows` by region → EU 2, US 1, NULL-region NULL; ungrouped 3.
  - `per_order` by region → EU 15, US 40, NULL-region NULL; ungrouped 70/3.
  - `wsum(price) FILTER (WHERE paid)` = 18 and equals the restricted-query value (stays green).
  - Ungrouped `rows(amount) FILTER (WHERE status = 'none')` → 0.
- [x] 1.3 Composition tests in the same file:
  - `rows(amount, partition_by=region) FILTER (WHERE paid)` by region, city → EU rows 2, US rows 1, Rome NULL.
  - Query filter `amount > 15` plus `FILTER (WHERE paid)` → ungrouped 2 (both restrict).
  - Joined condition `customers.tier = 'gold'` → ungrouped 3; by region EU 2, US NULL, NULL-region 1.
  - Sibling `rows(amount)` and `count(*)` by region read EU 2, US 2, NULL 1, with exactly 3 result rows, equal to the query without the filtered measure.
- [x] 1.4 Window tests:
  - By month, `rows(amount, window='2m') FILTER (WHERE paid)` → March present and reading its trailing interval's paid count (1), not NULL.
  - `FILTER (WHERE status = 'void')` → January and February NULL (empty frame), March 1.

  Calibrate the window's interval semantics against the unfiltered `rows(amount, window='2m')` first.
- [x] 1.5 Position tests:
  - Saved measure `paid_rows` equals the inline form, grouped and ungrouped.
  - `rows(amount) FILTER (WHERE paid) / count(*)` by region → EU 1, US 0.5, NULL-region NULL.
  - Measure-typed filter `rows(amount) FILTER (WHERE paid) > 1` by region → only EU.
  - Order by the filtered formula descending → EU, US, then NULL-region.
- [x] 1.6 Producer isolation tests:
  - `rows` and `per_order` with the same condition, both correct.
  - `rows … FILTER (WHERE paid)` and `rows … FILTER (WHERE status = 'Pending')` beside unfiltered `rows(amount)` by region, each correct (EU 2/NULL/2, US 1/1/2, NULL-region NULL/NULL/1).
  - Assert via the planned query that the differently-conditioned aggregates do not share a producer.
- [x] 1.7 Cross-model and stage tests:
  - Rooted at `customers` by `name`, `rows(orders.amount) FILTER (WHERE orders.status = 'paid')` → A 2, B 1, C NULL.
  - A non-final stage `s` on `orders` by region with `rows(amount) FILTER (WHERE status = 'paid')` named `pr`, consumed by a final stage `sum(pr)` → 3 (today 5).
- [ ] 1.8 Warnings, typing and emission tests:
  - The joined-condition and cross-model cases return no response warnings.
  - Typing parity: every condition the built-in path rejects with `AggregationFilterError` (aggregate inside `c`, a condition the source anchor does not determine) is rejected identically for `rows(...)`.
  - `TIER1` emission of the three custom formulas FILTERed by region succeeds on every dialect.
- [x] 1.9 Renderer fail-closed unit test: feeding an `AggregateKey` with `filter` set and `builtin=False` straight to aggregate rendering raises the internal error instead of emitting SQL.
- [x] 1.10 Create `tests/test_dev2186_postgres.py` (pytest-postgresql, unit, modelled on `tests/test_dev2177_postgres.py`) asserting `rows` and `per_order` grouped (incl. the NULL-region cell) and ungrouped, and the saved `paid_rows`; verify it runs locally (TCP-only Postgres options).

## 2. Implementation (pr-implement stage)

- [ ] 2.1 `slayer/core/keys.py`: add `restricts_own_rows(key)` (`key.filter is not None and not key.builtin`) and generalise the `AggregateKey.filter` comment; verify via 1.2.
- [ ] 2.2 `slayer/engine/binding.py` `_bind_filtered_agg`: a non-built-in key runs the existing anchor check on the would-be masked candidate, then carries `c` unmasked on `filter` (no `_carrying_filter` window refusal); built-ins unchanged; verify 1.8 typing parity and that `tests/test_dev2101_agg_filter.py` stays green.
- [ ] 2.3 `slayer/engine/compile/discovery.py`: route a `restricts_own_rows` root like a bare windowed / ranked root (local → `local_producer`, cross-model → `target_rooted`, association mode → the association producer); teach every guard keyed on `is_kernel_requiring` that must also hold for it; verify 1.2–1.5.
- [ ] 2.4 `slayer/engine/compile/stages.py`: the producer aggregates the filter-stripped key, and `c` is appended after the inherited filters as its own row filter (never counted in `n_date_range`; rerooted onto the target for a target-rooted producer). Map the original key to the producer slot. Key `_local_regroup_groups` by (grain, window identity, condition). Verify 1.6, 1.7, 1.8 warnings.
- [ ] 2.5 Windowed producers: `c` restricts only `_src`; the local windowed producer's buckets come from the population (an endpoints plan like `_window_endpoints` where missing); verify 1.4.
- [ ] 2.6 `slayer/sql/generator.py`: fail-closed guard on a filtered non-built-in key reaching aggregate rendering; verify 1.9.
- [ ] 2.7 `docs/concepts/formulas.md` › Filtered aggregation: add the one sentence "On a custom aggregation the clause restricts the rows its formula reads (`COUNT(*)` included), and a grouped cell with no passing rows reads NULL."; verify `zensical.toml` nav needs no change (existing page).

## 3. Verification

- [ ] 3.1 Full unit suite `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), and `poetry run la-arch-check` all green.
- [ ] 3.2 Integration suite with the CI invocation from `.github/workflows/ci.yml` green.
- [ ] 3.3 `openspec validate dev-2186-filter-where-c-on-a-custom-aggregation-restricts-every-row --strict` green.
