## 1. Tests first (pr-tests)

- [x] 1.1 Demo loader tests (unit; real loader on the `jafgen_dir` CSVs, no jafgen run): `regions` seeded with
      the four regions; every store has a non-null `region_id` from the static map; every order's `status` is in
      {completed, refunded, cancelled}; the same order id always gets the same status; an unmapped store name
      raises `SlayerError` naming it and leaves an existing demo DB unchanged; `verify()` reports a zero
      `stores->regions` orphan count. Verify: they fail on main.
- [x] 1.2 Demo spec scenarios on a 2-year build: all three statuses occur; `stores.regions.name` grouped
      `count(*)` sums to the total order count. Verify: they fail on main.
- [x] 1.3 Session demo fixture for the doc check: build the 2-year DuckDB once under an inter-process file lock in
      `tmp_path_factory.getbasetemp().parent`, then per worker copy it into its own storage dir and run
      `ensure_demo_datasource` (reuse path). `filelock` is only transitive today — add it as an explicit dev
      dependency if used. Verify: `pytest -n logical` on the doc tests runs jafgen once.
- [x] 1.4 `tests/test_doc_examples.py` extraction mechanism tests on synthetic markdown: JSON block, JSON stage
      list, multi-line `slayer query '…'`, multi-line `curl -d '…'` with escaped quotes, `SlayerQuery(...)`,
      Python dict / list literals, MCP `query(query={…})`, `JSON.stringify({…})` with quoted keys, several
      queries in one block, `{floor}` / `{? … ?}` inside strings; run-by-name and `@file` yield nothing.
      Verify: each form is extracted with the right line.
- [x] 1.5 Marker and guard mechanism tests: an unmarked hypothetical query fails; the same block after an
      `!!! note "Illustrative schema"` admonition is exempt and the following block is still checked; a marker
      followed by prose doesn't apply; an empty-result query fails; a block mentioning `source_model` with no
      extractable query fails unless marked; a dangling `jaffle_shop.` reference fails. Verify: all pass.
- [x] 1.6 The page-level doc check: parametrized per `page:line` over README.md, docs/index.md,
      docs/getting-started/*.md, docs/examples/*.md, docs/examples/*/*.md, docs/reference/*.md — execute each
      query (≥1 row), apply the guard, resolve every `jaffle_shop.<model>[.<path>]` reference as `slayer
      inspect` does. Verify: on main it hits every A/B item of DEV-2183 (also `quantity` in aggregations.md),
      plus the guard on the getting-started/rest-api.md JS example and the docs/index.md "Under the hood" diagram.
- [x] 1.7 Codex review of the tests against the plan; fold findings.

## 2. Demo extension

- [x] 2.1 `JAFFLE_SCHEMA_SQL`: `regions(id, name)`, `stores.region_id NOT NULL REFERENCES regions(id)`,
      `orders.status NOT NULL`; `LOAD_ORDER`/`TABLE_NAMES` include `regions`. Verify: 1.1 schema assertions.
- [x] 2.2 Loader: explicit `regions` seed insert after `create_schema`; a derived-columns map adding
      `stores.region_id` (static name map, unmapped → `SlayerError`) and `orders.status` (`md5(id)`, ≈90/7/3)
      to the loader SELECT; `verify()` gains `stores->regions`. Verify: 1.1, 1.2 pass.
- [x] 2.3 Enrichment: `regions` description + labels; `stores.region_id`, `orders.status` labels/descriptions
      (status documents that refunded/cancelled orders keep `order_total`); `total_revenue` description says
      gross of refunded/cancelled. Verify: existing enrichment tests + a fresh demo `inspect`.

## 3. Docs

- [x] 3.1 A items: `getting-started/cli.md` and `reference/cli.md` status queries run as written; `is_returned`
      memory examples (`getting-started/cli.md`, `reference/cli.md`, `reference/python-client.md`) → refund
      learning on `jaffle_shop.orders.status`; `slayer inspect` → `jaffle_shop.orders.stores.regions.name`,
      `jaffle_shop.orders.id`; outer stage `stores__name` in `multistage_queries.md` ×2 and `dynamic.md`;
      `aggregations.md` expression example and share-of-parent (dims `stores.regions.name`, `stores.name`,
      `partition_by=stores.regions.name`) with its prose. Verify: 1.6 passes for those pages.
- [x] 3.2 B items per design decision 13: `getting-started/python.md`, `getting-started/rest-api.md` (JS
      example with quoted keys), `getting-started/mcp.md`, `reference/rest-api.md`, `reference/mcp.md`,
      `reference/python-client.md`, `docs/index.md` (query + bullet explanations; the "Under the hood" diagram's
      `SlayerQuery (source_model, …)` becomes `SlayerQuery (model, …)` so the guard doesn't flag it),
      `examples/04_time/time.md`
      (data-robust `last(...)` filter, decision 8); absolute date ranges → relative; sample responses and demo
      model lists (`regions` added) follow the demo; refine fragments in demo vocabulary. Verify: 1.6 passes for
      every page.
- [x] 3.3 Delete `docs/interfaces/{cli,rest-api,mcp,python-client}.md` after grepping for inbound links; nav in
      `zensical.toml` unchanged. Verify: `grep -rn "interfaces/\(cli\|rest-api\|mcp\|python-client\)" docs README.md zensical.toml` is empty.
- [x] 3.4 Re-execute the notebooks whose cells change or whose committed outputs show the demo schema, after
      deleting the git-ignored `docs/examples/jaffle_data/demo` DB and `slayer_models` (run twice). Verify: the
      saved outputs mention `regions` / `status` where they list the schema.

## 4. CI

- [x] 4.1 Move the `unit-test-latest-deps` job (same id and steps) from `.github/workflows/ci.yml` to new
      `.github/workflows/latest-deps.yml` with `push` (main), `pull_request` (`'**'`), `schedule: cron
      '0 6 * * *'` and `workflow_dispatch`. Verify: YAML parses; the PR shows the job.

## 5. Gates

- [x] 5.1 Full unit suite (`poetry run pytest -m "not integration" -n logical`), integration suite with the CI
      invocation (notebooks), `ruff check slayer/ tests/`, `basedpyright` (no baseline growth),
      `la-arch-check`, `openspec validate --strict`. Verify: all green.
