## Context

See proposal.md - Why. The matrix lives in `examples/comparisons/matrix.yaml`, is exported to `matrix.json` by `export_matrix.py` (staleness guarded by `tests/test_comparison_matrix.py`), and each query-semantics verdict is backed by probes in `probes.yaml` that four runners execute against a shared `truth_sql` on `dataset.sql`. `events` there has no key to any other table, and only SLayer models it today. SLayer's cross-catalog Trino joins are already verified live (`TestTrinoFederation` in `tests/integration/test_integration_trino.py`) and documented (`docs/database-support.md`, "Cross-catalog joins").

## Goals / Non-Goals

**Goals:**
- Every new or changed verdict is backed by a probe, an existing test, or a cited source, and the cell says which.
- Adding `events` to the rival models changes no existing probe's status on any engine or Cube planner.

**Non-Goals:**
- Live Trino probes for Malloy, Cube or MetricFlow (the suite is DuckDB-only).
- Changing `export_matrix.py` or the website (the "54 capabilities" post title is the user's edit in motley-website).

## Decisions

### D1. README picture: committed HTML source rendered to a PNG
`examples/readme/sql_vs_slayer.html` is a two-pane, syntax-highlighted mock of the current screenshot: left `query.sql` with the current SQL unchanged, right `query.json` with

```json
{
  "source_model": "orders",
  "measures": ["sum(order_total)", "change_pct(sum(order_total))"],
  "dimensions": ["stores.name", "month(ordered_at)"]
}
```

(verified on the Jaffle Shop demo at HEAD: runs, no warnings). A header comment holds the headless-Chrome render command. The output is `docs/images/sql-vs-slayer.png`; `README.md` line 47 points at `https://raw.githubusercontent.com/MotleyAI/slayer/main/docs/images/sql-vs-slayer.png`, the hero image's scheme, because the README is also the PyPI description. The source sits outside `docs/` so zensical doesn't publish it as an orphan page. Alternative rejected: re-uploading to GitHub user-attachments (manual, unreproducible).

### D2. C13 "Embeddable UI components"
- intent: "Ready-made UI components (charts, tables, query builders) that render the layer's results in your own app."
- SLayer (`no`): "Headless: APIs only, no UI components."
- Malloy (`partial`): text names its renderer (`@malloydata/render`) and the Publisher embed / React SDK; keep the unfinished-token-check note.
- Cube (`partial`): "Headless React bindings (`QueryRenderer`, `QueryBuilder`) over REST; no charts or tables"; keep the Cloud iframe caveat.
- MetricFlow (`no`): unchanged.
- comment: "Malloy renders results with its own components; Cube Core ships headless React bindings; hosted embedding is a Cloud feature."

### D3. C16 "Cross-database queries": all four `yes` via Trino
The row grades whether one semantic query can join tables living in different databases; a federating Trino counts (user's ruling). Intent unchanged ("Join models living in different databases.").
- SLayer: how "One Trino datasource; `sql_table: postgresql.public.customers` joins `mysql.shop.orders`"; caveat "Verified live against Postgres and MySQL catalogs. Models on two SLayer datasources still can't meet in one query."
- Malloy: how "`trino.table('catalog.schema.table')` on one Trino connection"; caveat "Documented. Sources on two Malloy connections can't be joined; a DuckDB connection can also `ATTACH` other databases."
- Cube: text "Trino driver; cubes whose `sql_table` names different catalogs join as usual"; caveat "From source, not verified here; catalog-qualified `sql_table` isn't documented. Cubes on two Cube data sources need a `rollup_join` (Preview) in Cube Store."
- MetricFlow: text "dbt-trino: models in different catalogs via `database:`, joined through entities"; caveat "From source, not verified here. One adapter per dbt project, so no federation beyond Trino."
- comment: "All four reach several databases through Trino catalogs; natively, only Cube (`rollup_join`, Preview) and Malloy (a DuckDB connection with `ATTACH`) come close."

Sources: Malloy docs `documentation/language/connections` (three-segment Trino paths) and `setup/database_support`; Cube `docs-mintlify/admin/connect-to-data/data-sources/trino.mdx` and `recipes/data-modeling/cross-data-source-queries.mdx`; MetricFlow `dbt-metricflow/pyproject.toml` (`dbt-trino` extra), `metricflow/sql/render/trino.py`, `metricflow_semantics/sql/sql_table.py`.

### D4. Q26 "Metrics without a join path"
Placed after Q25; group Query semantics; origin SLayer; intent "Events per customer in one query, though no join path links events to customers."

Expected cells (final verdicts and text follow D6):
- SLayer `yes`: how "`count(events.id) / count(*)` from `customers`"; caveat "Computed over events' own rows and repeated on every row (per bucket of a spine time dimension) with an `unrelated_dataset` warning; grouping by an events column is a typed error (verified)."
- Malloy `partial` (user's ruling: writing a `join_cross` is a workaround, not the capability): how "`extend: { join_cross: events }` · `events.count()`"; caveat "Symmetric aggregates keep totals exact (verified), but you write the join, a time bucket needs a hand-written `on`, and nothing stops a row-multiplying group-by."
- Cube `no`: text "`Can't find join path` (verified)."; caveat records the model-declared `1 = 1` join outcome "(not counted)".
- MetricFlow `partial`: text "Metrics from unrelated models combine when grouped by `metric_time` or by nothing (verified)."; caveat "Grouping by a customer is refused; a cross-model ratio is a model-declared derived metric (not counted)."
- comment: "Only SLayer combines unrelated models without a written join and keeps the result's rows the population's; Malloy's `join_cross` is a capable workaround."

### D5. Q26 probes
Models: a standalone `events` source in `malloy/model.malloy` (no joins), an `events` cube in `cube/model/` (no joins), and an `events` semantic model with a simple count metric in the MetricFlow dbt project (`semantic.yml`; any cross-model derived metric goes in `model_declared.yml`). Calendars and `join_cross` stay inside probe queries, never in `model.malloy`.

| Probe | SLayer | Malloy | Cube Core | MetricFlow |
|---|---|---|---|---|
| `Q26-total-per-customer`: total event count on every customer row | match + warning `unrelated_dataset` | match, query-time `join_cross` | error matching `Can't find join path` | error matching `does not match any of the available group-by-items` |
| `Q26-ratio`: `count(events)/count(customers)` = 5/7, one row, `source_model: customers` | match + warning | match | two measures, no dimensions, Tesseract: outcome recorded | model-declared cross-model derived metric (title marks it not counted) |
| `Q26-per-month`: orders revenue and event count per month, 2025-01..2025-03 | match (time spine) + warning | match, probe-local calendar `on` | error matching `Can't find join path` | match (`metric_time`) |
| `Q26-per-region-month`: customer region x month, 2025-01..2025-03, event count per month repeated across regions | match + warning | match or error, recorded | error matching `Can't find join path` | error matching the group-by message |
| `Q26-group-by-unrelated-refused`: grouping by an events column alongside a customer column | typed error | - | - | - |
| `Q26-cube-fake-join`: probe-local `schema` file with a `customers_x` cube that `extends: customers` and joins `events` on `1 = 1` | - | - | outcome recorded, title marks it a model-declared workaround (not counted) | - |

Per-month domain: the window 2025-01..2025-03 has orders in every month, so SLayer's dense spine and MetricFlow's union of fact months agree; every engine reports the event count as `coalesce(..., 0)` (events fall only in 2025-03, so January and February cover the empty bucket). Error expectations match the specific join-path / group-by message, never a bare error.

### D6. Grading rule (fixed at plan time)
Verdicts and cell text follow actual probe outcomes under the suite's scoping: query-time expressions count; model-declared probes are noted "(not counted)". An engine that only combines unrelated grand totals or `metric_time` buckets is `partial`. A contradicted expectation moves the cell to what the probe shows; a genuine engine bug becomes a `known_bug` probe plus an entry under "Known issues" in `examples/comparisons/README.md`.

### D7. Regression isolation
The MetricFlow runner loads the whole dbt project (incl. `model_declared.yml`) for every probe, and the Cube runner layers every `cube/model/` file into every schema, so the `events` additions are not isolated. Instead, all four runners (Cube on both planners) are run on the merge-base (a temporary `git worktree` at `git merge-base HEAD origin/main`) and on the branch, and every pre-existing probe's status must be identical.

## Risks / Trade-offs

- [The raw README image URL 404s until merge] → same as the hero image; accepted.
- [Rival C16 claims for Cube and MetricFlow rest on source reading] → caveats say "from source, not verified here".
- [Adding `events` shifts Cube root selection or MetricFlow resolution] → D7 before/after status diff per engine and planner.
- [Website post title still says 54] → flagged to the user; outside this repo.
