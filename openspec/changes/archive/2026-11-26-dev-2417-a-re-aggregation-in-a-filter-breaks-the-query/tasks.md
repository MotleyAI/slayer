## 1. Failing tests (pr-tests)

- [x] 1.1 Add `tests/_reagg_filter_fixtures.py`: the `tests/_dev1739_fixtures.py` datasets (`orders → customers → regions`) plus an `items(id, order_id, qty)` child of `orders` (relationship `items → orders`), seeded identically into SQLite and DuckDB, with hand-computed values in the module docstring; verify both seeds load.
- [x] 1.2 Add `tests/test_reaggregation_filter_beside_joined_metric.py` (unit, `params=["sqlite", "duckdb"]`). Oracle per design Decision 8: rows == the `count(orders.*)`-only control's rows; the sibling's values == the same query without the measure-typed predicate under test (every other filter kept); plus hand values. Cases (probe values on the dev1739 data, population `orders` unless stated):
  - (1) dims `orders.region`, `sum(customers.spend)`, filter `max(sum(orders.amount, partition_by=[orders.region, orders.city])) > 45` → South 50, NULL-region 100;
  - (2) the same filter beside the child sibling `sum(items.qty)`;
  - (3) dims region + city, filter `max(sum(orders.amount)) > 35` → (North, CityB) 200, (South, CityC) 50, (NULL, CityD) 100;
  - (4) metrics `max(sum(orders.amount, partition_by=[orders.region, orders.city]))` as `m0` and `sum(customers.spend)` as `m1`, filter `m0 > 45` → South (50, 50), NULL (60, 100);
  - (5) case 1 with no explicit `population` field → same rows and values;
  - (6) dims region + city, filter `max(sum(orders.amount, partition_by=[orders.region, orders.city]), partition_by=orders.region) > 45` → (South, CityC) 50, (NULL, CityD) 100, asserting the checker types the conjunct METRIC stratum 1;
  - (6b) dims region + city + computed dimension `sum(orders.amount, partition_by=orders.region)` as `rt`, filters `sum(orders.amount, partition_by=orders.region) > 55` and `max(sum(orders.amount, partition_by=[orders.region, orders.city])) > 35` beside `sum(customers.spend)` → (North, CityB) 200, (NULL, CityD) 100, AND assert the checker types the first conjunct FIELD with stratum > 0 (design Decision 9);
  - (7) filter `max(sum(customers.spend, partition_by=[orders.region, orders.city])) > 150` beside `sum(customers.spend)` → North 300;
  - (8) month time dimension `orders.ordered_at` with filter `max(sum(orders.amount, partition_by=[orders.region, orders.city])) > 25` beside `sum(customers.spend)` and, separately, beside `time_shift(sum(customers.spend), -1)`;
  - (9) case 1 with threshold `> 35` plus the stratum-0 row filter `orders.customer_id <> 1` → North 200 (300 without the row filter), South 50, asserting the checker types it FIELD stratum 0 and that the cross-dataset sibling sees it.
  Verify every case except the controls fails on `main` (the `orders.__regroup__…` UnknownReferenceError) and the controls pass.

## 2. Implementation (pr-implement)

- [x] 2.1 Introduce the typed population-filter carrier (Pydantic: original filter index, `BoundFilter`, text, date-range flag; date-range entries first) and ONE selector building it from `prebound` + `filter_typings` by index (FIELD, stratum 0, not a row restriction) — never from a rewritten filter's phase (design Decisions 2–3); verify the 1.2 cases pass.
- [x] 2.2 Route `_regroup_inherited_filters`, `dispose_population_filters`, `_desugar_regroups`' cross-dataset base filters and `_ProducerSynthesisContext` through the selector (one list replaces `base_filters_with_text` + `population_filters` / `n_population_date`); retire the redundant `phase == ROW` checks in `_home_filters`, the `product_anchors` argument and `_producer_row_conjuncts`; leave `_frame_free_filters` unchanged; verify `la-affected-tests --base main` scope passes.
- [x] 2.3 Run the enforcement bundle (`poetry run la-arch-check`, LikeC4 validate, `la-typecheck`) and `poetry run ruff check slayer/ tests/`; verify all green with no new type-check errors.
