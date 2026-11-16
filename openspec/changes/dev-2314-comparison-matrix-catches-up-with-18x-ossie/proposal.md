## Why

The feature matrix in `examples/comparisons/` was last edited on `1.x` between 1.3.0 and 1.4.0. Since then 1.7.0
added list columns and 1.8.0 added Ossie export, so several SLayer cells understate what 1.8.x ships. Once this
lands, the website should sync `matrix.json` from `1.x` until 2.0 ships, so its probe links must point at `1.x`.

## What Changes

- **C23** (Open interchange spec): SLayer becomes **yes** (`slayer export-osi`), backed by five new Ossie round-trip
  probes; the comment notes that SLayer and MetricFlow write Ossie.
- **C4** (Database coverage): SLayer reads "9 engines tested live, 4 more by SQL generation." (Trino joined Tier 1).
- **Q22** (Nested source data): SLayer becomes **partial**, byte for byte from `main`; the Q27 probes are its evidence.
- **Q27** (List columns): new row with all four engine cells, its probes and fixtures (`customers.tags`, Cube and
  MetricFlow one-row-per-tag models, list canonicalisation in the Malloy and MetricFlow runners), byte for byte from
  `main`, plus `main`'s Q27 unit test.
- The SLayer probe runner gains an Ossie round-trip mode (`osi_roundtrip`): export with the real CLI, re-import into a
  fresh store, then compare the models, a query, or the export report.
- `export_matrix.py` derives the probe-link ref from the package version: below 2 links to `1.x`, otherwise `main`.
- `matrix.json` regenerated (55 → 56 rows); C9 (partial) and the Q6 caveat (broadcast) stay as they are on `1.x`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. The change touches only the comparison example suite and its test; SLayer's behaviour does not change
(`skip_specs: true`).

## Impact

- `examples/comparisons/`: `matrix.yaml`, `matrix.json`, `probes.yaml`, `dataset.sql`, `README.md`,
  `export_matrix.py`, `slayer/run_slayer.py`, `slayer/models/customers.yaml`, `cube/model/customers.yml`,
  `cube/model_declared/customer_tags.yml`, `metricflow/dbt_project/models/{semantic.yml,model_declared.yml,stg_customer_tags.sql}`,
  `metricflow/run_metricflow.py`, `malloy/run_malloy.mjs`.
- `tests/test_comparison_matrix.py`.
- motley-website (outside this repo): the post's title and count ("54 capabilities") need an edit there; the slug stays.
