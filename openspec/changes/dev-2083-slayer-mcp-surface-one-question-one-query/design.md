## Context

See proposal.md (Why) and the delta spec. DEV-2066 / DEV-2075 constraints hold: tool descriptions and `instructions` ≤ 2048 chars (Claude Code's cut), input schemas passed whole, the `query` schema within `QUERY_SCHEMA_BUDGET` (28,000), each concept in one place, no benchmark task text. Measured on `98ca207`: instructions 1,223, `query` description 1,494, `query` schema 26,559 (expected ≈ 27,500 after).

Plan-time probe (in-process DuckDB idiom fixture, `tests/_mcp_idiom_fixtures.py`, monthly buckets on `order_date`):

| Value | time filter `order_date >= '2025-02'`, `order_date <= '2025-03'` (Feb, Mar) | unfiltered (Feb, Mar) |
|---|---|---|
| `cumsum(sum(amount))` | 70, 100 | 295, 325 |
| `lag(sum(amount), 1)` | NULL, 70 | 150, 70 |
| `lead(sum(amount), 1)` | 30, NULL | 30, 60 |
| `consecutive_periods(sum(amount) > 0)` | 1, 2 | 5, 6 |
| `rank(sum(amount), direction='desc')` | 1, 2 | 3, 6 |
| `ntile(sum(amount), n=2)` | 2, 1 | 2, 1 |
| `sum(amount, partition_by=[])` | 100, 100 | 490, 490 |
| `change(sum(amount))` | -80, -40 | same |
| `time_shift(sum(amount), -1)` / `(…, 1)` | 150, 70 / 30, 60 | same |
| `sum(amount, window='3m')` | 265, 250 | same |

The equivalent `date_range` gives the same values. Dimensions `customers.regions.name`, `customers.name`, filter `customers.name == 'Alice'`: rank 1 (2 overall), dense_rank 1 (2), `ntile(…, n=3)` 1 (2), `sum(amount, partition_by=[])` 170 (490), share of region 1.0 (170/300), `avg(sum(amount, partition_by=[customers.name]), partition_by=[])` 170 (490/3). Two stages — inner `rank` over every customer, outer filter `customers__name == 'Alice'` — give 2. A value filter in one stage runs after the transform: `cumsum(sum(amount))` filtered `> 300` keeps 2025-03 325, 2025-04 385, 2025-06 490. A non-time filter restricts look-back input: `status == 'paid'` gives `sum(amount, window='3m')` 195 for 2025-01 (215 unfiltered). A filter on a transform's partition key (per-customer `cumsum`, one customer kept) leaves it unchanged.

Architecture: `slayer.mcp` and `slayer.memories` are buckets without a node arc42 file; `slayer.core.query` is `core`. Principles applied: system P9 (examples use dotted-canonical paths, `customers.regions.name`), P12 (Pydantic). No arc42 or model edits; no import changes; spec attaches to the existing `mcp` capability.

## Goals / Non-Goals

**Goals:** the one-query rule is the first thing an agent reads in the instructions and the `query` description; the stage rule says exactly when a stage is needed, on the field where stages are written, once, and nothing on the surface contradicts it; examples model readable grouping.

**Non-Goals:** engine behaviour; `docs/` (human docs, not the agent surface — `docs/concepts/queries.md` keeps its `region_id` examples); running the evals (driven from slayer-evals).

## Decisions

**D1 — Stage scope.** The issue's "Stages ARE needed only to show part of a running total" is too narrow: the probe shows a row filter or `date_range` applies before every cross-row value except the calendar look-backs (`change`, `change_pct`, `time_shift`, `window=`), which read past a filter on their own time axis only. A filter on a transform's value applies after it. So a stage is needed exactly to drop rows by a dimension or time condition that a cross-row value must still see. Alternative — the issue's wording — rejected: false for rank, lag/lead, ntile, consecutive_periods and `partition_by` totals.

**D2 — Exact wording.**
- `SERVER_INSTRUCTIONS` second line (plain capitals, no markdown): "ONE QUESTION = ONE QUERY. Build the whole answer in a single `query` call. Do NOT split it into several queries, do NOT combine or recompute query results yourself (by hand or in Python), and do NOT add stages for what one stage can do. One SLayer query computes all of these together: totals at any coarser grain and shares of them, ranks and top N per group, period-over-period changes, running totals, trailing windows, aggregates of aggregates, and aggregates over several joined models. Each aggregate runs over its own model's rows, so one query NEVER fans out or double counts. Explore the data first if you need to; then write the one query."
- `query` docstring: first paragraph and "One query can usually produce the whole answer. Prefer that to …" become "ONE QUESTION = ONE QUERY. Build the whole answer in a single `query` call. Describe the result you want (measures, dimensions, filters, order) and SLayer writes the SQL, including the joins. A single query can:"; the third bullet becomes "compute period-over-period changes and trailing windows (90 days, 3 months), which look back before date_range by themselves, and running totals;". Other bullets and paragraphs unchanged.
- `name`: "Stage name. In a list of query objects every entry but the last carries a name, and later stages use it as their source_model. A STAGE IS NOT NEEDED for top N per group (filter on `rank(...)` in the same stage), for averaging or bucketing per-entity totals (a nested aggregate, or a computed dimension with `partition_by`), or for shares and totals (`partition_by`). A filter on a transform's value (`rank(...) <= 3`, `cumsum(...) > 100`) applies after it, over all rows. Stages ARE needed only to drop rows by a dimension or time condition that a cross-row value must still see, or to join or reuse a whole result: a row filter or date_range applies before a running total, lag / lead, rank, ntile, consecutive_periods or `partition_by` total (change, change_pct, time_shift and window= still read past date_range or a filter on their own time dimension, but not past other filters). Compute the value in an inner stage and filter in the outer one. An outer stage sees the inner result's columns with dotted paths flattened to __: an inner dimension customers.regions.name is customers__regions__name outside; a measure keeps its name. More: inspect(reference=["memory:help.queries"], entity_type="memory")."
- `dimensions` appends: "Group by naturally readable labels: a name (customers.regions.name), never by an id alone unless ids are asked for; when a name may repeat (people, products), by the id and the name."
- `measures`: `sum(amount) / sum(amount, partition_by=customers.regions.name)`; `avg(sum(amount, partition_by=[customers.regions.name, customers.id]), partition_by=[customers.regions.name])`. `filters`: `rank(sum(amount), partition_by=customers.regions.name, direction='desc') <= 1`. `count_distinct(customer_id…)` stays (a count, not a grouping).
- `date_range`: ends at "cumsum starts at the range start." (drops the inner/outer-stage recipe).

**D3 — Single owner (option A).** Removed copies: `help.queries` `## One query first` section and its "Typical uses: …" line (its flattening example becomes `customers.regions.name` → `customers__regions__name`); `help.workflow` "Answer with one query whenever …" paragraph; `help.transforms` Limits "A row-level condition applies before transforms, … filter in an outer stage." → "Whether a filter needs a stage around a transform: the query object's `name` field."; `help.time` lead-in "For a running total since the first row, compute it in a named stage without `date_range`, then filter the time in an outer stage:" → "A running total since the first row, in two stages:" (the JSON example stays). The `help.intro` overview line stays (a capability overview, not the rule). Alternative — keep help copies, fix only the wrong ones — rejected: the rule would live in 4–5 paraphrases the drift guard cannot see.

**D4 — Readable labels.** The issue's "group by names, not ids, unless ids are asked for" merges entities sharing a name (Codex finding); the adopted wording requires a readable label and keeps the id beside a name that may repeat.

**D5 — Drift guard.** Marker constants in `tests/_mcp_idiom_fixtures.py`: `STAGE_NOT_NEEDED = "A STAGE IS NOT NEEDED"`, `STAGE_NEEDED = "a cross-row value must still see"` (owner `name`), `READABLE_LABEL = "never by an id alone"` (owner `dimensions`), `ONE_QUERY = "ONE QUESTION = ONE QUERY"` (owners: instructions and `query` description), `FAN_OUT = "NEVER fans out or double counts"` (owner: instructions; replaces `cannot fan out`). The sole-owner guard takes owner sets. Retired phrases, checked over every surface text (not only help topics): "filter after a window or rank", "filter after a rank or window", "re-query a whole result", "accumulate in an inner stage", "Answer with one query whenever", "## One query first", "Typical uses". The no-`_id` guard rejects any dotted or `__`-flattened path ending in `_id` and any `partition_by=` key ending in `_id` across the `query` description, every schema description, the instructions and every help body (Codex's "too syntactic" finding rejected: the issue asks for exactly this, and it polices only our own examples).

## Risks / Trade-offs

- [Schema growth ≈ 950 chars, to ≈ 27,500 of 28,000] → if over budget, stop and ask rather than raising it.
- [`name` becomes the second-longest field (~1,200 chars)] → accepted: it is the field an agent reads when it reaches for a stage.
- [Capitals read as shouting] → deliberate, per the issue; limited to the one rule and the stage-rule pair.
- [Changed existing idiom tests] → the rank-filter, share, nested and stage tests switch to `customers.regions.name` (keys North / South, same values), approved with the plan.
