## Context

See proposal.md (Why). The demo is jafgen CSVs loaded into DuckDB by `slayer/demo/jaffle_shop.py`: the
loader skips tables without a `raw_<name>.csv` and inserts only the CSV's columns; a database whose
table/column/type projection differs from `JAFFLE_SCHEMA_SQL` is rebuilt into a sibling file and swapped
in. jafgen is unseeded (`uuid4`, `random`, `numpy`) and every build or reuse shifts dates so the newest
order is today, so values and month buckets differ between builds and days.

Structural cause of the bug: markdown snippets are never executed. The fix is one executable check over
every in-scope query, not a pass over today's instances.

arc42: system §3.9 (dotted-canonical) — the multistage fix spells the outer dimension `stores__name`,
the inner stage's emitted flat column name, not the banned `__` split-alias input form; §3.12 (Pydantic,
no dataclasses) for test helpers; §3.14 (idempotent, additive ingestion) — `regions` and `status` arrive
through ingestion of declared FKs/columns and the enrichment stays additive. Both specs attach under the
existing `demo` capability claimed by `python.surfaces`; no arc42 or `.c4` edit.

## Goals / Non-Goals

**Goals:** the extended demo answers every in-scope example; the doc-example check fails loudly on any
drift, including query forms it cannot parse; the latest-deps unit suite runs daily.

**Non-Goals:**
- `docs/concepts/*`, `docs/dbt/*`, agent help text and the MCP `query` tool description (hypothetical
  schema by design); the SQL-API pages `docs/interfaces/flight-sql.md` / `pg-facade.md`.
- Queries outside code blocks (inline code spans, image alt text) — the README alt-text query is already
  executed by `tests/test_demo_jaffle_shop_healing.py`; the dbt page's inline queries run on the dbt
  example's own dataset.
- Machine-checking run-by-name `--refine` / `refine=` fragments and sample response blocks (hand-aligned).
- Seeding jafgen (ids come from `uuid4`; full determinism is not achievable without patching it).
- The motley-website quickstart (DEV-2184).

## Decisions

1. **Add `orders.status` rather than rewrite the status examples.** (user) Values `completed` /
   `refunded` / `cancelled` ≈ 90/7/3, derived from `md5(id)` (stable, unlike DuckDB's `hash`); amounts
   unchanged, so "refunded/cancelled orders keep `order_total`" is a real learning for the memory
   examples. Alternative (rewrite to `stores.name` / `month(ordered_at)`) loses the single-table first
   query and needs an invented quirk for the memory example.
2. **Add a `regions` table plus `stores.region_id`, not a `stores.region` column.** (user) Only a table
   gives the demo a two-hop path from its fact table (`orders → stores → regions`), which the landing
   page's multi-hop example needs; it also gives region > store for share-of-parent. Static map:
   Philadelphia, Brooklyn → Northeast; Chicago → Midwest; San Francisco, Los Angeles → West;
   New Orleans → South. `city` maps to `stores.name`.
3. **Loader: seeded tables and derived columns.** (codex) `regions` rows are inserted explicitly after
   `create_schema`; a derived-columns map (table → {column: SQL expression}) adds computed columns to the
   loader's SELECT — `stores.region_id` from the name map, `orders.status` from `md5(id)`. An unmapped
   store name raises `SlayerError` naming it; the sibling-file build leaves the existing DB in place.
4. **Opt-out marker: a visible `!!! note "Illustrative schema"` admonition.** (user) One marker serves
   reader and test; a hidden comment would not "say it doesn't". It applies when it is the last non-blank
   block element before the fence, to that fence only. (codex) A block that mentions `source_model` without
   being a query (the landing-page diagram) is exempted from the guard by a hidden
   `<!-- doc-check: not-a-query -->` comment directly above its fence instead; it never exempts queries. (user)
5. **Structural extraction of every query literal.** (user) JSON blocks parsed whole; Python blocks
   walked with `ast` (`SlayerQuery(...)` calls, `literal_eval`-able dict/list literals); other blocks scan
   balanced `{…}`/`[…]` spans, string-aware so `{floor}` / `{? … ?}` inside strings don't break it
   (codex), each tried as JSON. Covers `slayer query '…'`, `curl -d '…'`, `query(query={…})` and
   `JSON.stringify({…})` once the JS example uses quoted keys. This extends the issue's list of five forms;
   run-by-name and `@file` forms are skipped.
6. **Coverage guard.** (codex) A code block mentioning `source_model` that yields no query and carries no
   marker fails — an unparseable form surfaces instead of escaping silently.
7. **Each query must return ≥1 row, on 2-year data.** (user) "No results" fails users as surely as an
   error; 2 years is the CLI default, so the test sees the user's data (stores 3–4 open in year 2).
   Fixed date ranges in examples become relative.
8. **Data-robust examples.** (default) Builds and days differ (see Context), so a filter whose
   non-emptiness depends on a random trend (e.g. "regions whose latest MoM change is negative") is
   rewritten to a condition that holds on every build — a flaky example would make the daily job noisy.
9. **One demo per session, safe under xdist.** (codex) The DuckDB is built once (real jafgen, 2 years)
   under an inter-process file lock in `tmp_path_factory.getbasetemp().parent`; each worker copies it into
   its own storage dir and runs `ensure_demo_datasource` (reuse path: no jafgen, ingestion local to the
   worker) — the pattern of `tests/integration/_demo_build.py`. Sharing one storage dir would race model
   writes and DuckDB's single-writer lock.
10. **`docs/index.md` is in scope.** (user) The landing page is the most-copied query.
11. **Reference check.** (default + codex) Every `jaffle_shop.<model>[.<path>]` reference must resolve as
    `slayer inspect` resolves it; rewritten memory/inspect examples spell entity references
    `jaffle_shop.`-qualified so they are covered. Bare prose references stay unchecked (not reliably
    identifiable).
12. **Daily run in its own workflow.** (user) Move `unit-test-latest-deps` to
    `.github/workflows/latest-deps.yml` (same job id — codex), keeping `push` (main) / `pull_request` and
    adding `schedule: cron '0 6 * * *'` + `workflow_dispatch`. Alternative (`schedule:` in `ci.yml` plus
    `if:` guards on three jobs) runs or guards every job daily, including the billed BigQuery job. No
    required checks or rulesets depend on the workflow name.
13. **Rewrites keep each example's point.** (default) Mapping: `revenue`/`amount` → `order_total`;
    `created_at` → `ordered_at`; `order_id` → `id`; `region` → `stores.regions.name`;
    `customers.regions.name` → `stores.regions.name`; `count_distinct(upper(email))` →
    `count_distinct(concat(customer_id, store_id))`; `percentile(subtotal * quantity, …)` →
    `percentile(tax_paid / subtotal, p=0.5)`; landing page `last(customers.score, changed_at)` →
    `last(stores.name, stores.opened_at)` (newest store in the region); `is_returned` learning → the
    `orders.status` refund learning. Sample responses and demo model lists follow the demo.
14. **Notebooks.** (default + codex) Re-execute the notebooks whose cells change and those whose
    committed outputs show the demo schema (model listings, `inspect`, column inventories), after deleting
    the git-ignored local demo DB and `slayer_models` (run twice, so the second run takes the reuse path).
    Other notebooks' outputs remain real results of their unchanged cells.

Rejected Codex findings:
- Extract inline code and image alt text — rejected (codex): see Non-Goals; would need per-cell opt-outs
  on a page backed by another dataset.
- Compare constraints in the schema-freshness check — rejected (codex): SLayer's builds always create
  them; new columns already trigger the rebuild.
- Validate sample response blocks — rejected (codex): pairing a response with its query is a prose
  heuristic and values are random per build; hand-aligned instead.
- Machine-check refine fragments — rejected (codex): needs fixture saved queries the pages never define;
  three fragments, hand-fixed.

## Risks / Trade-offs

- [Schema change rebuilds every existing local demo DB once] → existing stale-schema path, ~20 s.
- [jafgen adds a store missing from the region map] → setup fails naming the store; the old DB survives.
- [A doc example becomes empty on some build or day] → decision 8; a failure names `page:line`.
- [Session demo build adds ~30 s to the unit suite] → built once across all xdist workers.
- [Scheduled-run failures notify only whoever last edited the cron line] → accepted; runs are listed
  under Actions → the workflow, filter `event:schedule`.
