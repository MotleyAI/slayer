## Why

The first runs of `ossie-compliance.yml` failed without any SLayer regression: upstream apache/ossie turned each
wide-table case into one `compliance/wide_table/<id>.ost` script, upstream `main` gained documents and spec prose the
gate misjudges, and the gate's BigQuery job was cancelled by another job queued in the shared GCP project's
concurrency group.

## What Changes

- The gate reads each wide-table case from upstream's `.ost` script: the case's data and model from its `connect` and
  `modelFile`, its query from `executeLower`, its expected rows or error code from the script's expected output. The
  reader fails closed on any script content it does not recognise. Upstream's `skip(...)` (a gap of upstream's own
  runner) does not exempt a case.
- `tests/fixtures/ossie_corpus` is re-vendored from apache/ossie `main`; the old `cases/<id>/` directories go.
- The corpus compile check expects a metric with no SQL-readable dialect entry (e.g. DAX/MDX only) to be unevaluable.
- The Layer 1 completeness check classifies the new prose of § Approximate Aggregations and drops prose the spec no
  longer holds.
- The gate's BigQuery job stays in `bigquery-shared-gcp-project` and queues (`queue: max`); a test holds every job of
  that group to `queue: max` and `cancel-in-progress: false`.
- The compliance page names the case scripts and the new vendored revision.

## Capabilities

### New Capabilities

### Modified Capabilities
- `compatibility/ossie-compliance`: the gate's wide-table suite is upstream's case scripts, run on the data and model
  each script loads; a scheduled run's BigQuery job queues behind the shared project's other BigQuery runs and is never
  cancelled by them.
- `models/semantic-model-documents`: the wide-table query oracle is the case script (its `executeLower` query and
  expected table or error code), not `query.json` / `expected.txt`.

## Impact

- Tests: `tests/_ossie_gate_suite.py`, `tests/_dev2217_fixtures.py`, `tests/_ossie_gate_fixtures.py`,
  `tests/_ossie_gate_spec.py`, `tests/test_ossie_gate.py`, `tests/test_ossie_gate_compile.py`,
  `tests/integration/test_integration_ossie_gate.py`, `tests/test_dev2243_wide_table_oracle.py`,
  `tests/test_ossie_gate_corpus_root.py`, `tests/test_ossie_schema_drift.py`, `tests/test_dev2243_corpus.py`; new
  reader and workflow-concurrency tests.
- Corpus: `tests/fixtures/ossie_corpus/**` (re-vendored).
- CI: `.github/workflows/ossie-compliance.yml`.
- Docs: `docs/interfaces/ossie-compliance.md`.
- No `slayer/` production code changes.
