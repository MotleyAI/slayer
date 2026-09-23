## Why

`time_shift` (and `change` / `change_pct`, which desugar onto it) over a composite that contains a `partition_by=` leaf re-aggregates that leaf at the shifted query grain, silently dropping its partition — the launch post's flagship shape (a mixed-grain expression, shifted, re-aggregated) returns plausible wrong numbers. The cause is structural: the shifted CTE is a hand-rendered second evaluator of the base aggregation that never consults the planner's structural decisions, so the same defect class also breaks a bare partitioned or ranked leaf under a `date_range` (NULL at the first visible bucket) and a windowed leaf inside a shifted composite (window silently dropped).

## What Changes

- The re-aggregation-regime shifted relation becomes a planner-synthesized producer (one composition primitive, sql P10): every leaf of the shifted input is evaluated at its own grain — a leaf whose grain contains the time axis, and any ranked or windowed leaf, is re-evaluated over frame-free rows; a leaf whose grain does not contain the axis is carried at its in-frame value. Frame bounds (`date_range` and relational bounds with a temporal literal on a query time dimension's raw column) are stripped from the shifted evaluation exactly as today; every other predicate and every model-level filter applies unchanged.
- Both regimes share one join-back: the base row reads the shifted relation at the bucket containing `bucket + offset`. Value-identical for every bucket-aligned shift; a shift not aligned to the bucket now reads the previous bucket (the series regime's existing convention) instead of relabelling source buckets forward.
- **BREAKING**: the shift family loses its bare row-column exemption. `time_shift(weight, -1)`, `change(weight)`, `change_pct(weight)` over an unprojected row or derived column are rejected at plan time by the one grain-refining-row-leaf rule every other transform obeys (the former behaviour multiplied result rows). A projected dimension operand stays legal.
- A ranked leaf inside a shifted composite (`time_shift(amount:last / 2, -1)`) executes instead of raising an internal error; a windowed leaf inside a shifted composite fails loudly with the existing windowed-composite guard instead of returning a silent wrong value.
- The generator's hand-rendered shifted-CTE body (leaf re-aggregation, shifted scope, frame-bound stripping, relabel arithmetic, read-and-rebucket arm) is deleted; the emitter renders the producer through the existing producer door and emits the join-back.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/transforms`: "Composite-input time_shift" — each leaf keeps its own grain when shifted, the frame rule, the shared join-back; "time_shift row-level-leaf rejection stays fail-closed" — bare row / derived column inputs are no longer exempt; "Non-shift transforms reject grain-refining row-level leaves" is REMOVED and replaced by "Transforms reject grain-refining row-level leaves" covering every transform op except the aggregation-dispatched `first` / `last`.

## Impact

- `slayer/ir/planned.py` (shifted attach phase), `slayer/engine/compile/` (new shifted-producer synthesis, own-grain nesting rule, population-filter threading), `slayer/engine/elaborate_env.py` (one transform row-leaf checker), `slayer/engine/query_engine.py` (identity-deduplicated nested-plan traversal), `slayer/sql/generator.py` (time_shift emitter reduced to a join-back), `slayer/core/keys.py` (shift-offset parsing helper).
- Every `time_shift` golden across the five golden dialects moves (class b: SQL shape, values unchanged) and is re-blessed per the divergence protocol; shape tests pinning the relabel form of the shifted CTE are re-pinned to the lookup form with per-file consent; the three pinned bare-row-column result multisets become rejection tests.
- `docs/concepts/formulas.md`: one sentence.
