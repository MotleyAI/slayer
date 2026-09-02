# Divergence ledger — DEV-1846

How the lift changed observable behaviour, measured on SQLite against
`tests/_dev1846_fixtures.py` (and the DEV-1750 fixture for the flipped golden).

## 1. Errors → values (lifted shapes)

Every row previously fail-closed (a `NotImplementedError` naming DEV-1450 stage
7b.11); it now renders and executes. Values are hand-computed in
`tests/test_dev1846_composite_transforms.py`.

| Shape | Pre-lift | Post-lift value (SQLite) |
| --- | --- | --- |
| `time_shift(revenue:sum / qty:sum, -1)` by store | 7b.11 raise | prev ratio per store, NULL in each store's first month (A: –,6,8; B: –,6,10) |
| `change_pct(revenue:sum / *:count)` by store | 7b.11 raise | per-store MoM ratio growth, NULL in month 1 |
| `change(revenue:sum / qty:sum)` by store | 7b.11 raise | ratio − prior ratio (A: –,2,2; B: –,4,−5) |
| `time_shift(coalesce(revenue:sum, 0), -1)` | 7b.11 raise | NULL, 60, 100 (missing bucket stays NULL under the wrapper) |
| `time_shift(revenue:wrevenue_sum + hi_rev:sum, -1)` | 7b.11 raise | NULL, 200, 360 (each leaf re-aggregates with its own params/filter) |
| `time_shift(revenue:wrevenue_sum * 2, -1)` | 7b.11 raise | NULL, 300, 520 (crossing-fragment join re-registered in the shifted CTE) |
| `consecutive_periods(revenue:sum - cost:sum)` | 7b.11 raise | streak 1, 2, 3 (non-NULL/non-zero truthiness) |
| `consecutive_periods(cumsum(revenue:sum))` | 7b.11 raise | streak 1, 2, 3 |
| `consecutive_periods(revenue:sum > 90 or cost:sum > 40)` | 7b.11 raise | streak 0, 1, 0 |
| `consecutive_periods(not (revenue:sum > 90))` | 7b.11 raise | streak 1, 0, 1 |
| `consecutive_periods(store in ('A','B'))` | 7b.11 raise | streak 1, 2, 3 per store |
| `consecutive_periods(store not in ('B','C'))` | 7b.11 raise | A: 1,2,3; B: 0,0,0 |
| `consecutive_periods(iif(revenue:sum > 0, 1, 0))` | 7b.11 raise | streak 1, 2, 3 (iif value truthiness) |
| `consecutive_periods(store in ('A','C') and revenue:sum > 0)` | 7b.11 raise | A: 1,2,3; B: 0,0,0 (nested IN column materialises) |
| `consecutive_periods(hi_rev:sum > 0 or cost:sum > 1000)` | 7b.11 raise | B: 1,2,0 (NULL predicate group breaks the run) |

NULL / non-numeric semantics: a missing shifted bucket is NULL even under a
NULL-absorbing wrapper (`coalesce`), and a NULL-valued `consecutive_periods`
predicate is treated as false.

## 2. Class (b) — single-leaf SQL byte-movement

**None.** Unifying the bare-aggregate `time_shift` onto the composite render
door left single-leaf SQL byte-identical, and bare regroup-placeholder leaves
(crossing-fragment / target-grain / first-last) keep their DEV-1750
read-and-rebucket path unchanged. The DEV-1750 golden baseline diff is limited
to the one intentional flip below; every other entry is untouched.

Composite `time_shift` re-aggregates each leaf directly in the shifted CTE from
source rows, so it reads no `_cm_*` value and omits those regroup attaches
entirely (a bare read-and-rebucket still keeps them, to resolve its placeholder
column). A crossing-fragment leaf's own join (e.g. `regions`) is re-registered
per leaf and stays.

## 3. Golden re-blessings

| Golden | Case | Change |
| --- | --- | --- |
| `dev1750_sql_baseline.json` | `composite/still_7b11` → `composite/lifted` | was a 7b.11 raise; now renders a shifted-CTE re-aggregation (all 5 dialects) |
| `dev1846_sql_baseline.json` | ts/* and cp/* lifts | recorded as SQL (first-time baseline) |
| `dev1846_sql_baseline.json` | reject/* | recorded as the new uniform `ValueError` (below) |

## 4. Surviving fail-closed errors

Still rejected, now with one uniform `ValueError` per shape on every render path
(plain, combined-attaches, kernel body) — no internal stage markers, naming the
transform, the shape, and the multi-stage `source_queries` remedy.

| Shape | Error names |
| --- | --- |
| `time_shift(cumsum(x), -1)` (nested transform) | time_shift · transform · source_queries |
| `time_shift(revenue:sum * weight, -1)` (mixed) | time_shift · row · source_queries |
| `time_shift(weight * qty, -1)` (pure row) | time_shift · row · source_queries |
| `time_shift(revenue:sum + regions.factor:sum, -1)` (cross-model leaf) | time_shift · cross-model · source_queries |
| `consecutive_periods((revenue:sum>0)+(cost:sum>0))` | consecutive_periods · boolean · numeric/arithmetic |
| `consecutive_periods(coalesce(revenue:sum>0, 0))` | consecutive_periods · boolean |
| `consecutive_periods(lower(sku:max))` | consecutive_periods · string · truthiness |

A `time_shift` composite raises the same message whether or not a cross-model
measure sits elsewhere in the query (the gate is hoisted above every render
path). BETWEEN is not DSL-reachable, so it is covered structurally
(`_iter_slot_deps`), not end-to-end.
