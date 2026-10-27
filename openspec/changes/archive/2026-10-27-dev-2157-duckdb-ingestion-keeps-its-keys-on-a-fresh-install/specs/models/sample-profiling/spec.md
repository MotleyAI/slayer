## MODIFIED Requirements

### Requirement: One profiling path for every sample read and refresh

Every sample-value read path — `inspect_model` (and `inspect` of a model at `compact=False`), `inspect` of a column at `compact=False`, and the search column-hit refresh — and every forced refresh (`slayer datasources refresh-samples`, the `edit_model` refresh) SHALL profile a model's columns through one model-level profiling path, so the caching, failure bounding, failure cache and logging rules below hold identically everywhere. Hidden, identifier and opaque (`UNKNOWN`) columns SHALL never be profiled. Numeric/temporal columns SHALL be profiled by a single batched min/max query per call; categorical (text/boolean) columns by one top-values query per column. A forced refresh SHALL keep profiling only `sql_table`-backed models (sql-mode and query-backed models skipped).

#### Scenario: inspect_model profiles numeric columns with one query

- **WHEN** `inspect_model` renders a healthy model with several uncached numeric/temporal columns
- **THEN** exactly one min/max query is executed for them, and the rendered and persisted ranges are identical to what the search and `inspect`-column paths persist for the same columns

#### Scenario: Healthy-model parity across read paths

- **WHEN** the same uncached column on a healthy model is profiled via `inspect_model`, via `inspect` of the column at `compact=False`, and via a search column hit
- **THEN** each path persists the same `sampled`, `sampled_values` and `distinct_count`

#### Scenario: Forced refresh skips non-table-backed models

- **WHEN** a forced refresh runs over an sql-mode or query-backed model
- **THEN** no profiling query is executed for it and no error is reported
