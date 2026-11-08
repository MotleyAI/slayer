## Why

Query examples in the docs are never executed, so they drift from the bundled Jaffle Shop demo: a 1.5.0
user ran the CLI quickstart and got `Cannot resolve reference 'status'`, and a systematic run found the
same failure across the getting-started, reference and example pages. Nothing catches the drift, and an
upstream dependency release that breaks the demo is only noticed on the next push.

## What Changes

- The demo gains an `orders.status` column (`completed` / `refunded` / `cancelled`) and a store region
  hierarchy (`regions` table, `stores.region_id`), giving the docs a fact-table categorical and a
  two-hop path `orders → stores → regions`.
- Every failing doc example is fixed: run as written on the extended demo, or rewritten to a demo query
  making the same point; fixed date ranges become relative ones.
- The four orphaned `docs/interfaces/{cli,rest-api,mcp,python-client}.md` pages are deleted.
- A unit test executes every query literal in the code blocks of README, `docs/index.md`,
  `docs/getting-started/`, `docs/examples/` and `docs/reference/` against an in-process demo; each must
  return rows unless its block is marked with an `!!! note "Illustrative schema"` admonition. Every
  `jaffle_shop.<model>[.<path>]` reference on those pages must resolve.
- The `unit-test-latest-deps` CI job moves to its own workflow and also runs daily.

## Capabilities

### New Capabilities
- `demo/doc-examples`: every documented query in the in-scope pages runs on the demo, or is visibly marked
  as illustrative.

### Modified Capabilities
- `demo/jaffle-shop`: the demo carries an order status and a store region hierarchy.

## Impact

- `slayer/demo/jaffle_shop.py` (schema, loader, enrichment, `verify`); existing local demo databases
  rebuild once through the stale-schema path.
- `README.md`, `docs/index.md`, `docs/getting-started/*`, `docs/examples/**/*.md`, `docs/reference/*`;
  deleted `docs/interfaces/{cli,rest-api,mcp,python-client}.md`; affected example notebooks re-executed.
- New `tests/test_doc_examples.py` plus demo loader tests.
- `.github/workflows/ci.yml` and new `.github/workflows/latest-deps.yml`.
