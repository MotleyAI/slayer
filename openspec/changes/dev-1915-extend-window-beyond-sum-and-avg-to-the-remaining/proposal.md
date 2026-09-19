## Why

`window=` is refused on every aggregation but `sum` and `avg` by a checker allowlist that a
second, hand-synced allowlist in the render registry mirrors. Neither encodes a semantic
rule: the windowed producer is a range self-join over the home rows in the trailing
interval (Axiom 2.4), so any aggregation — rolling `count`, `min`, `max`, `count_distinct`,
the statistics family, a custom formula, and a `first`/`last` pick within the interval —
has one obvious reading. Under Axiom 9 a well-typed term refused for an implementation
reason is a closure violation, and two lists that must agree by hand are the bug class.

## What Changes

- **`window=` on every aggregation.** Every built-in and every custom model-level
  aggregation accepts `window=` in every position a windowed `sum` is accepted today
  (measure, filter, order, computed dimension, composite, transform input, with
  `partition_by=`, cross-model, expression source). The value per cell is the aggregation
  over the home rows whose window time lies in `[bucket_end − window, bucket_end)`; an
  empty interval follows SQL's empty-set semantics (`count` family 0, everything else
  NULL). `*:count(window=)` counts interval rows, never the unmatched grain row.
- **`first`/`last` under `window=` gain a defined reading**: the value of the earliest or
  latest interval row by the ranking time column, resolved exactly as plain `first`/`last`
  (explicit argument, else the temporal row dimension, else the time dimension's raw
  column, else the model default); an empty interval yields NULL; NULL ranking keys and
  ties follow the dialect's native order, as for plain `first`/`last`.
- **Parameters evaluate per interval row.** Reference-bearing parameters — a column,
  an attached aggregate, or a definition default that names columns (`other=`, `weight=`,
  custom params) — are read on each interval row; literal parameters (`p=0.9`, `lo=10`)
  pass through unchanged, so `percentile`'s literal-only `p` rule is untouched.
- **No aggregation allowlist survives.** The checker keeps only the duration checks
  (compact duration string, non-empty, positive, well-formed); the render registry loses
  its windowability field. Dialect gaps are unchanged: an aggregation a dialect cannot
  render plain raises the same error windowed (median/percentile on MySQL, T-SQL,
  BigQuery; corr/covar on MySQL). `count_distinct` needs no framed DISTINCT — the emission
  is a range join — so no new per-dialect error exists.
- Unchanged: time-dimension resolution and attributability, the association refusal for
  `window=`/`first`/`last` (DEV-1914), the re-aggregation outer-window refusal, the
  reserved `window` kwarg name on custom aggregations.
- A separate pre-existing gap found in review — a **plain** `first`/`last` whose implicit
  ranking key is a derived `default_time_dimension` across an unproven hop escapes the
  input-safety check — is DEV-1945; a windowed `first`/`last` cannot reach it (its grain
  always contains the bucket, so the implicit key is the checked window axis).

## Capabilities

### New Capabilities

- `aggregations/trailing-window`: the trailing `window=` contract for every aggregation —
  the interval, empty-interval semantics, star count, `first`/`last` within the interval,
  per-row parameters, custom aggregations, dialect gaps, and the absence of an
  aggregation allowlist.

### Modified Capabilities

None — every existing windowed scenario (partitioned, cross-model, computed dimension,
transform, stage time axis, association refusal, re-aggregation refusal) stays true.

## Impact

- `slayer/sql/generator.py` (the trailing-window producer CTE renders its aggregate
  through the one aggregate builder over the `_src` value column; `first`/`last` branch
  onto the ranked helpers; picked parameters and the ranking time projected into `_src`),
  `slayer/sql/render/aggregates.py` (`window_class` / `windowable` / `window_agg_class`
  deleted), `slayer/ir/planned.py` (`TrailingWindowProducerKernel` gains
  `ranking_time_key` and `picked_params`), `slayer/engine/compile/stages.py` (the kernel
  is built with the root model and bundle; picked parameters remapped through the
  sub-plan's substitutions), `slayer/engine/elaborate_env.py`
  (`check_windowed_key_supported` → `check_window_duration`).
- Tests: new `tests/test_dev1915_golden_sql.py` + `tests/golden/dev1915_sql_baseline.json`,
  new `tests/test_dev1915_windowed_exec.py` + `tests/_dev1915_fixtures.py`; consented
  edits to `tests/test_dev1835_guards.py`, `tests/test_dev1744_value_expr.py`,
  `tests/_dev1871_raise_ledger.py`, `tests/test_sql_generator.py`,
  `tests/test_dev1733_order_only_transform_composite.py`.
- Docs: `docs/concepts/formulas.md`, `docs/concepts/queries.md`,
  `docs/examples/07_aggregations/aggregations.md`, the MCP help text in
  `slayer/mcp/server.py` (one factual line). No notebook states the old rule.
- No arc42, LikeC4 or `index.yaml` change (`aggregations` is already a cross-cutting
  spec); `guards.baseline` unchanged (no new deferral site; the removed guard is a
  `ValueError`).
