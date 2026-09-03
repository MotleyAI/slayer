# DEV-1840 — approved value / error / SQL divergences

Every behavior flip this change ships, enumerated for approval per the
"Existing cross-model behavior is preserved where already safe" requirement.
Provably-safe shapes stay byte-identical (existing golden corpus green).

## Value flips (lenient mode): dropped filter → semi-join pushdown

A ROW filter reachable from a target-rooted producer's root only across an
unproven/unsafe hop no longer drops (metric broadcast its unfiltered value);
it now restricts the producer population via correlated EXISTS.

- `tests/test_dev1739_execution.py::test_host_local_filter_semi_joins_into_target_total`
  — scalar `customers.spend:sum(partition_by=[])` under `region == 'North'`:
  350.0 (unfiltered) → 300.0 (customers with ≥1 North order).
- `tests/test_dev1746_empty_base_plan.py::test_host_filter_gates_the_whole_result`
  — scalar `customers.spend:sum` under `status == 'paid'`: 1325.0 → 1000.0.
- `tests/integration/test_integration_duckdb.py::TestF1F4SemanticValues`
  — F4 pins revised: `paid` 700.0 → 100.0; `unpaid` 700.0 → 200.0
  (no-matching-rows still returns zero rows).
- The three consented DEV-1836 pins (filter_inheritance ×2,
  broadcast_strict strict-drop) updated in the test stage.

## Warning-surface flips

Pushed filters emit no `unreachable_filter_dropped` warning/metadata and no
Python warning; `strict=True` stops erroring on them. Genuinely excluded
conjuncts (unreachable, ambiguous reverse path, out-of-scope OR/NOT mixing,
multi-branch) keep the warning and the strict error.

- `tests/test_dev1769_routed_filter_path_validation.py` — intermediate-hop and
  intermediate-owned derived filters: dropped+warned → pushed silently.
- `tests/test_dev1745_reachability.py::TestProducerInheritanceRouting` —
  sibling-branch / host-local / mixed-locality conjuncts: dropped → pushed
  (contract now three-way).

## SQL-shape flips (goldens re-blessed)

- `tests/golden/dev1840_sql_baseline.json` — every `exists/…` key re-blessed
  at implementation time (ALLOWED_DELTAS emptied again); `inline/` and
  `excluded/` keys byte-identical to the pre-implementation bless.
- `tests/test_carrier_scope_matrix.py` F4 SQL-shape pin and the two
  `tests/test_sql_generator.py` cm-CTE pins: the host/off-branch filter now
  appears inside the CTE as EXISTS (never as a cardinality-changing join).
- `tests/test_dev1747_golden_sql.py` — unchanged: the dev1747 fixture gained a
  second `orders → customers` join, keeping its drop-path corpus dropping via
  a genuinely ambiguous reverse hop (dev1745_warning_contract likewise).

## Error-surface flips

- ClickHouse < 25.4 / undeterminable version: a semi-join query now fails
  closed with a clear error naming the filter and the version requirement
  (previously executed with the filter dropped). On ≥ 25.4,
  `allow_experimental_correlated_subqueries=1` is attached automatically.

## Checker contract

- `slayer/sql/scope_check.py` is correlation-aware: a qualifier bound in an
  ancestor scope across expression-subquery boundaries is legal (planner
  semi-joins and the RLS rewrite pass without the legacy allowlist flag);
  Mode-A correlated subqueries consequently execute instead of being rejected
  (`tests/test_dev1752_subquery_scope.py`).
