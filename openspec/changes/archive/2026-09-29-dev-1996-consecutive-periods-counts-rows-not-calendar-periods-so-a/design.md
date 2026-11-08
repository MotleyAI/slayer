## Context

`consecutive_periods` is emitted by one method, the per-slot
`consecutive_periods` emitter in `slayer/sql/generator.py`, as two window CTEs:
a reset-group running sum over NOT-`p`, then a running count of `p` within
(partition, reset group). Adjacency is row adjacency. `time_shift` (and
`change` / `change_pct`, which desugar onto it) finds the calendar neighbour by
joining back on `offset(bucket, n, granularity)` — re-truncated to the bucket
granularity when the shift does not preserve bucket starts — inlined in the
`time_shift` emitter. The partition is the transform auto-grain (every
projected ROW slot except the time bucket), so each (partition, bucket) is one
row.

## Goals / Non-Goals

**Goals:** one definition of calendar adjacency shared by every transform that
reads a calendar neighbour; `consecutive_periods` gap-aware on every dialect.

**Non-Goals:** densifying series (a date spine would add rows, violating the
population axiom); streak lookback beyond the query's date range (unlike the
`time_shift` producer regime); changing `lag` / `lead` (row-based by design);
a `period=` granularity override (rejected instead).

## Decisions

1. **LAG + calendar predecessor.** A new first CTE carries every column plus
   `LAG(bucket) OVER (PARTITION BY <auto-grain> ORDER BY bucket)`. The reset
   flag becomes `CASE WHEN p AND prev = predecessor(bucket) THEN 0 ELSE 1 END`;
   the value CTE is unchanged. `prev` NULL (first row, or after a NULL bucket)
   and a NULL bucket both fail the equality, so each starts a group — which
   yields the "NULL bucket adjacent to nothing" rule with no special case.
   *Alternative rejected:* period ordinals (`period_index − ROW_NUMBER()`) need
   a new per-dialect epoch-period function for eight granularities and a second
   definition of adjacency.
2. **One calendar-offset helper.** A generator helper returns the bucket `n`
   periods of granularity `g` away, re-truncated to the bucket granularity
   when the shift does not preserve bucket starts. The `time_shift` join-back
   lookup and the `consecutive_periods` predecessor (`n = -1`, `g` = bucket
   granularity) both call it, so the two cannot disagree on adjacency. It
   composes existing dialect hooks (offset, truncation); no new dialect hook.
3. **SQLite sub-day offsets use `DATETIME(...)`.** `DATE(col, '-1 hours')`
   drops the time; `DATETIME` output (`YYYY-MM-DD HH:MM:SS`) matches the
   STRFTIME hour/minute/second bucket formats. Day-and-coarser units keep
   `DATE(...)`, matching the `%Y-%m-%d` bucket formats and leaving the
   `week_sunday` truncation (which composes this hook with day offsets)
   unchanged. The quirk stays in `dialects/sqlite.py` (sql P2).
4. **`period=` rejected** by dropping it from the transform keyword allowlist,
   so the existing unsupported-keyword error fires (sql P9 fail closed).
5. **Axiom 11.3** gains the approved calendar-adjacency sentence, enforced by
   `tests/test_consecutive_periods_calendar.py`.

## Risks / Trade-offs

- [Bucket-type mismatch in `prev = predecessor(bucket)` on server dialects —
  DATE vs TIMESTAMP truncation results, implicit coercion] → the same
  comparison shape already backs the `time_shift` join-back; executed
  integration cases on PostgreSQL, MySQL, ClickHouse and SQL Server over DATE-
  and TIMESTAMP-typed columns at month, quarter and week_sunday; BigQuery is
  golden-only (no execution harness).
- [One extra window pass per `consecutive_periods` slot] → accepted; one
  `LAG` over rows already sorted for the running sums.
- [Existing tests whose data has gaps assert row-based values] → any such
  failure stops the flow for a per-test user decision; goldens are re-blessed.
