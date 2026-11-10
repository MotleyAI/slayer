## REMOVED Requirements

### Requirement: One profiling path for every sample read and refresh
**Reason**: Restated in "One profiling path for every sample read and forced refresh": the MCP `inspect_model` tool its scenarios name is removed in SLayer 2.0, and `inspect` of a model at `compact=False` is that read path.
**Migration**: None.

## ADDED Requirements

### Requirement: One profiling path for every sample read and forced refresh

Every sample-value read path — `inspect` of a model at `compact=False`, `inspect` of a column at `compact=False`, and the search column-hit refresh — and every forced refresh (`slayer datasources refresh-samples`, the `edit_model` refresh) SHALL profile a model's columns through one model-level profiling path, so the caching, failure bounding, failure cache and logging rules below hold identically everywhere. Hidden, identifier and opaque (`UNKNOWN`) columns SHALL never be profiled. Numeric/temporal columns SHALL be profiled by a single batched min/max query per call; categorical (text/boolean) columns by one top-values query per column. A forced refresh SHALL keep profiling only `sql_table`-backed models (sql-mode and query-backed models skipped).

#### Scenario: Inspecting a model profiles numeric columns with one query

- **WHEN** `inspect` of a model at `compact=False` renders a healthy model with several uncached numeric/temporal columns
- **THEN** exactly one min/max query is executed for them, and the rendered and persisted ranges are identical to what the search and `inspect`-column paths persist for the same columns

#### Scenario: Healthy-model parity across read paths

- **WHEN** the same uncached column on a healthy model is profiled via `inspect` of the model at `compact=False`, via `inspect` of the column at `compact=False`, and via a search column hit
- **THEN** each path persists the same `sampled`, `sampled_values` and `distinct_count`

#### Scenario: Forced refresh skips non-table-backed models

- **WHEN** a forced refresh runs over an sql-mode or query-backed model
- **THEN** no profiling query is executed for it and no error is reported

## MODIFIED Requirements

### Requirement: List columns are profiled over their elements

A list column SHALL be profiled over its elements, through the same profiling path, caching, failure bounding and logging as any other column. Its `sampled_values` and `distinct_count` SHALL describe the distinct non-NULL elements by frequency for `TEXT` / `BOOLEAN` elements, and its range SHALL be the elements' min and max for numeric and temporal elements. A whole list SHALL never be sampled, and a list with `UNKNOWN` elements SHALL never be profiled.

#### Scenario: Text elements
- **WHEN** `inspect` of a model at `compact=False` profiles `contractors.skills` over rows holding `['go', 'sql', 'go']`, `['sql', NULL]`, `[]`, NULL and `['go']`
- **THEN** `sampled_values` is `go` then `sql`, and `distinct_count` is 2

#### Scenario: Numeric elements
- **WHEN** a column `scores ARRAY<INT>` holding `[3, 9]` and `[1]` is profiled
- **THEN** its sampled range is 1 to 9

#### Scenario: Opaque elements
- **WHEN** a list column with `element_type: UNKNOWN` is read by `inspect` of its model at `compact=False`
- **THEN** no profiling query is executed for it
