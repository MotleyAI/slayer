## Why

The 1.x backport of OpenTelemetry (#605, DEV-2358) found defects in code `main` shares with #602, and `1.x` also
carries the DEV-2314 comparison-matrix update that `main` lacks. Merging `1.x` into `main` brings both over and keeps
`main` containing every `1.x` commit, instead of hand-porting the same fixes and resolving them again at the next
forward merge.

## What Changes

- Merge `1.x` (with #605) into `main`, resolving every conflict by the DEV-2358 "Forward-merge notes (1.x → main)",
  with the overrides in design.md.
- OTel fixes arriving through the merge:
  - `otel.start()` after `otel.uninstrument()` closes the previous providers and detaches their log handlers;
  - the exporter-log redaction is lifted once a timed-out provider shutdown ends;
  - the exporters and `slayer otel test` use the same per-signal endpoint, even when its variable holds only spaces;
  - a REST handler that raises before responding records `http.response.status_code` 500;
  - `connection_url()` names the engine the client actually uses, so telemetry reports the cached engine's host.
- An EXPLAIN query's span tree is specified: no `slayer.query.run`, and its `slayer.db.explain` span is a child of
  `slayer.query`. The `slayer.db.explain` span carries `db.response.returned_rows`.
- DEV-2314 on `main`: the C23 Ossie round-trip probes and the runner's `osi_roundtrip` mode, C4's engine count, the
  probe-runner expectation checks, and a regenerated `matrix.json`.
- `openspec/forward-merge-ledger.yaml` lists the 1.x OTel requirements whose wording `main` keeps differently.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `otel/collection`: "A query is one span tree" gains the EXPLAIN clause and scenario.

## Impact

- `slayer/otel/{recorder,exporters,control,middleware}.py`, `slayer/database/client.py`.
- `tests/test_otel_{guards,controls,scope,collection}.py`, `tests/test_dev2074_save_ambiguity.py`.
- `examples/comparisons/` (`run_slayer.py`, `probes.yaml`, `matrix.yaml`, `matrix.json`, `export_matrix.py`,
  `README.md`, `slayer/models/{customers,monthly_rev,avg_customer_rev}.yaml`), `tests/test_comparison_matrix.py`.
- `openspec/forward-merge-ledger.yaml`, `poetry.lock` (re-locked on the merge result).
- The archived `1.x` change folders for DEV-2314 and DEV-2358 join `main`'s archive.
