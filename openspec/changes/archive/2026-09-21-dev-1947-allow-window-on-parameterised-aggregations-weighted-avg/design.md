## Context

See proposal.md — Why. On main the trailing-window producer reads its parameters per
interval row: `_trailing_window_kernel` resolves the reference-bearing parameters on the
source owner and remaps each key through the producer sub-plan's regroup substitutions,
so an attached-aggregate parameter reads its row-attach column (DEV-1915 D4). A
spec-plan probe over the DEV-1840 dataset confirmed both the built-in and the custom
(`wsum`) windowed-parameter forms execute identically under `broadcast`, `associate` and
`error` on SQLite and DuckDB with no warning, in measure, filter and ORDER BY positions,
and generate SQL on all seven golden dialects with no placeholder leak. Planning the
built-in form yields one outer attach with a `trailing-window` kernel whose single picked
parameter (`weight`) is the `ColumnKey` placeholder that the nested, `orders`-rooted
producer's one substitution maps the `sum` aggregate to.

## Goals / Non-Goals

**Goals:**
- The corpus states the windowed attached parameter as current behaviour, including the
  custom form and the filter / ORDER BY positions.
- Pins that fail if the per-interval-row parameter path regresses: executed values, and
  the plan-level fact that the kernel reads the row-attach column.

**Non-Goals:**
- Any change under `slayer/`; the structural fix (allowlists deleted, engine P9 / Axiom 9)
  is DEV-1915's.
- Windowing by association when the bucket is unattributable (DEV-1914) and `window=` on
  a re-aggregation's outer aggregation stay their existing typed refusals.
- A ranked aggregation's ordering key as an attached value: the ordering key keeps its
  own rules (DEV-1945).

## Decisions

- **D1 — Close-out, not a re-implementation.** The issue's implementation notes are
  superseded by DEV-1915 (allowlist → `check_window_duration`; `picked_params` on the
  kernel, remapped through the sub-plan's substitutions). Alternative rejected: cancelling
  the issue as subsumed leaves the stale spec note and the missing golden without a home.
- **D2 — One MODIFIED requirement.** The scenario title "Windowed aggregation with an
  attached parameter" is kept (a MODIFIED delta cannot rename a scenario); only the stale
  parenthetical goes and the THEN gains the positions clause. The new normative sentence
  is qualified to the enclosing *non-ranked* aggregation so it never specifies an attached
  value in a ranked aggregation's ordering-key role. The custom scenario lives beside the
  built-in one rather than in `aggregations/trailing-window`, which already has the
  same-model attached-weight and cross-model custom-default cases.
- **D3 — A separately named raw-row oracle.** `windowed_custom_param_by_month()` is
  composed from the DEV-1919 fixtures' raw-row helpers (`_region_weight`, `_months`,
  `_trailing_year`, `_sum_product`) with its own docstring, not an alias of
  `windowed_constituent_by_month`, so the expected values are visibly derived from the
  dataset for the custom formula even though they coincide with the constituent's.
- **D4 — Exact-shape plan pin.** The pin asserts exactly one outer attach, kernel kind
  `trailing-window`, picked names `["weight"]`, exactly one nested attach with exactly one
  substitution whose original key is the `sum` aggregate, and placeholder equality with
  the picked key — so a second nested aggregate cannot be mistaken for the weight.
- **D5 — Goldens in the DEV-1859 module.** The issue names `tests/test_dev1859_golden_sql.py`
  and its `test_param_cases_generate` guard is the point; the custom twin
  `param/windowed_custom` rides the module's existing `orders_wsum` model set. New keys
  fold into the baseline on regeneration without an `ALLOWED_DELTAS` entry.

## Risks / Trade-offs

- [The executed and plan pins are green on the current tree, so the TDD stage has no red
  to drive] → by design of a close-out; tasks.md says so explicitly, and the only red is
  the golden module's missing keys until spec-implement blesses them after review.
- [DuckDB returns rows in a different order than SQLite] → every assertion is on a
  month-keyed dict or on the non-NULL order list, as the existing pins do.
- [The golden harness rejects unknown keys] → expected; `SLAYER_UPDATE_GOLDEN=1` on the
  one module, every pre-existing key checked byte-identical.
