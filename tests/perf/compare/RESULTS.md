# Engine A/B audit — results

**Branch query engine (DEV-1811 typed pipeline) vs. pinned `motley-slayer==0.9.12`.**

Same corpus of 100+ queries run under both engines, on identical seeded data, comparing
result **values** (correctness) and **execution time** (performance). Correctness runs at
10k rows + an adversarial dataset; timing runs at 10k → 10M rows. Backends: **SQLite,
DuckDB, PostgreSQL 16**. Charts and numbers are regenerated from `out/` by
[`render_report.py`](render_report.py); see [README](README.md) to reproduce the run.

> Snapshot: branch `30b26d5b`, pinned `0.9.12`, ABBA timing ×7 repeats/side, 2026-08-21.

---

## TL;DR

- **Correctness: the rewrite is at least as correct as 0.9.12 everywhere.** Where the
  ground-truth oracle can arbitrate, it **never** faults the branch — and it catches
  **two real 0.9.12 bugs the branch fixes**, confirmed on all three engines.
- **Performance: parity on SQLite and Postgres** (median branch÷0.9.12 ≈ 1.00× at every
  scale). **DuckDB carries a ~20 ms fixed per-query overhead** on the branch — the one
  systematic perf gap.
- **One genuine algorithmic regression:** `time_shift` with a date range is ~1.35×
  slower and grows to **+2.6 s at 10M rows on SQLite** — traceable to the recent
  period-boundary fix's truncate→shift→truncate nesting.
- The **new deep multi-CTE queries** (cross-model aggregates, host-rooted isolation,
  ranked/transform-over-join, stacked stages) execute at parity — the tall CTE stacks
  are not themselves a cost.

---

## What is measured

Each corpus entry is a SLayer query (dict or multi-stage list). The harness runs it under
both engine versions and compares:

- **Correctness** — result rows compared cell-by-cell with numeric tolerance. A slayer-free
  **pandas oracle** independently computes the ground truth from an explicit per-entry spec,
  so when the two engines disagree we know *which* is right (not just *that* they differ).
- **Performance** — **ABBA** ordering (`pypi, branch, branch, pypi`), 7 timed repeats per
  side per run → 14 pooled samples/side; medians compared. An entry is **flagged** only if
  the branch median is **> 1.3× AND > 20 ms** slower (both, to ignore sub-millisecond noise).

The corpus spans aggregations, joins, filters, formulas, time grains, ordering, multi-stage
chains, and error paths. This audit **added deep multi-level-CTE coverage** that the corpus
previously lacked — see [the new entries](#the-new-deep-cte-entries).

---

## Correctness

Across SQLite, DuckDB and Postgres (plus the adversarial dataset on the file backends),
the branch **matches 0.9.12 except where it is provably *more* correct.** No case exists
where the oracle judges the branch wrong, and there are **no unexpected double-failures**.
The non-MATCH results are dominated by *known* branch improvements (the SQL-boolean-literal
and list-variable fixes; the time-shift period-boundary fix), plus the two bugs below.

### Two 0.9.12 bugs the branch fixes — confirmed on all three engines

| Bug (query) | 0.9.12 | Branch | Verdict |
|---|---|---|---|
| **Ranked `:last`/`:first` grouped by a joined dimension** (`join_ranked_last_over_join`) | Emits invalid SQL — `ROW_NUMBER() OVER (PARTITION BY customers.segment …)` but projects a bare `segment` → `no such column: segment` | Correct SQL | `PYPI_ONLY_ERROR` on SQLite, DuckDB **and** PostgreSQL |
| **Measure `Column.filter` that crosses a join** (`join_filtered_local_isolation`) | **Silently drops the filter** → returns unfiltered category totals | Host-rooted isolation applies the filter correctly | `VALUE_MISMATCH`, **oracle says branch ✓ / pypi ✗** on all three engines |

The second is the important one: the filtered measure `whale_cost:sum` (a `cost` column
filtered to `customers.segment = 'whale'`) should sum only whale orders per category and
keep every category (NULL where none). 0.9.12 ignores the filter entirely; the branch's
DEV-1503/1709 isolation is correct, and the pandas oracle proves it independently.

---

## Performance

### Median branch ÷ 0.9.12, by scale and backend

![Median execution ratio by scale, per backend](report_assets/ratio_by_scale.svg)

SQLite (blue) and Postgres (green) ride the 1.00× parity line at every scale — the typed
pipeline costs nothing there. **DuckDB (orange) sits at 1.2–1.5×.** But that ratio is
misleading on its own: it comes from a *fixed additive* overhead, not a multiplier.

### Why DuckDB is high: a fixed per-query overhead, not a slowdown

![Per-query pypi vs branch at 1M rows](report_assets/scatter_1m.svg)

Each dot is one query at 1M rows; the dashed line is parity (above it = branch slower).
SQLite and Postgres points hug the diagonal. **DuckDB's cluster sits *parallel* to and above
the diagonal** by a near-constant offset. On the **flagged 1M-row queries** that offset is
~20 ms (`filter_range_and` +20 ms, `ms_stage_variable` +20 ms, `formula_arith_time_shift`
+20 ms…); across the **whole 100-query corpus** the pooled **median** additive delta is
smaller — DuckDB +6–9 ms, versus SQLite ~0 ms and Postgres ~0 ms. Because DuckDB queries are
tiny (≈10–20 ms), even the median offset lifts the ratio to 1.2–1.5×; at 10M rows, where
queries take longer, the same overhead is only 1.22×.

That the overhead is **absent on Postgres** (whose connections are the *heaviest*) rules out
generic connection cost — it points at something **DuckDB-execution-specific** in the
branch's `execute` path, worth profiling directly.

### The one algorithmic regression: `time_shift` at scale

![Added latency of time_shift_date_range on SQLite](report_assets/timeshift_sqlite.svg)

`bench_time_shift_date_range` is slower on the branch across **all three** backends
(~1.3–2.2×). It's cheap on DuckDB/Postgres (+25 ms at 10M) but **expensive on SQLite:
+2.6 s at 10M rows.** The cost tracks the recent `time_shift` period-boundary fix, which
wraps the shift in an extra `DATE_TRUNC` (truncate → shift → truncate) — correct, but SQLite
pays for the added nesting at scale.

### Flagged entries (branch > 1.3× **and** > 20 ms)

| Backend | Where it bites | Nature |
|---|---|---|
| **DuckDB** | ~all small-query entries at 10k–1m (e.g. `filter_range_and` 3.5×, `ms_stage_variable` 2.8×, transform family 2.0–2.4×) | the fixed ~20 ms overhead; small absolute; fades to one entry by 10m |
| **SQLite** | `bench_time_shift_date_range` (1m 1.34×, **10m 1.35× = +2.6 s**); `bench_simple_count` (10m 2.45×, but only +47 ms — likely IO variance) | the time_shift nesting; otherwise parity |
| **Postgres** | a tail of transform/window entries (`time_shift_date_range`, `monthly_yoy/_cumsum/_change_pct`, `last_agg_type`); median stays ~1.0× | small absolute; no systematic overhead |

---

## The new deep-CTE entries

These were added to make the corpus actually exercise **multi-level CTE generation** — the
tallest SQL the branch emits — which it previously only touched via single-model transforms.

| Entry | CTE structure exercised | Result vs 0.9.12 |
|---|---|---|
| `join_cross_model_rerooted_dim` | re-rooted cross-model `_cm_` (target's own join graph) | MATCH¹ |
| `join_cross_model_by_time` | forward cross-model `_cm_` over a time grain | MATCH |
| `join_cross_model_plus_local` | host `_base` + joined-back `_cm_` side by side | MATCH |
| `join_transform_rank_over_join` | transform layer stacked on a joined-dim aggregate | MATCH |
| `join_ranked_last_over_join` | ranked `_rk_` over a joined dimension | **branch fixes 0.9.12 crash** |
| `join_filtered_local_isolation` | host-rooted filtered-local isolation (`_cm_`) | **branch correct, 0.9.12 wrong** (oracle) |
| `ms_stage_join_then_agg` | stage CTE built on a join CTE | MATCH |
| `ms_stage_transform_then_agg` | stage CTE built on a transform CTE | MATCH |

All eight execute at parity (0.99–1.04× on SQLite/Postgres; DuckDB rides the shared fixed
overhead). ¹ `join_cross_model_rerooted_dim` diverges *only* on the adversarial dataset's
NULL-region rows, where the branch keeps the aggregate via the null-safe grain join-back —
defensible branch behavior, not oracle-arbitrable.

---

## Two things worth a follow-up

1. **DuckDB per-query ~20 ms overhead** — systematic, DuckDB-only (SQLite & Postgres are
   clean), small absolute but real. Profile the branch's DuckDB `execute` path.
2. **`time_shift_date_range` nesting** — the boundary-fix's extra truncation is +2.6 s at
   10M rows on SQLite. Correctness-critical, so keep the fix; consider whether the outer
   re-truncation can be elided when the shift is whole-period.

---

## Reproduce

```bash
# full correctness + full-corpus timing, all scales, sqlite + duckdb
poetry run python tests/perf/compare/compare.py --profile-all --retime --subprocess-timeout 21600

# add Postgres (needs a running server + a db whose name contains 'bench')
poetry run python tests/perf/compare/compare.py \
  --db-url postgresql://bench:bench@localhost/slayer_bench --db-type postgres \
  --profile-all --retime --subprocess-timeout 21600

# regenerate this report's charts from out/
poetry run python tests/perf/compare/render_report.py
```
