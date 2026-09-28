# models/sample-profiling Specification

## Purpose
Defines how SLayer profiles column sample values (`Column.sampled`, `sampled_values`, `distinct_count`) lazily on read and on forced refresh: what counts as cached, how a failing model or column bounds and caches its failures, how failures are logged, and how profiling is scoped under a row-level-security policy.

## Requirements

### Requirement: One profiling path for every sample read and refresh

Every sample-value read path — `inspect_model` (and `inspect` of a model at `compact=False`), `inspect` of a column at `compact=False`, and the search column-hit refresh — and every forced refresh (`slayer search refresh-samples`, the `edit_model` refresh) SHALL profile a model's columns through one model-level profiling path, so the caching, failure bounding, failure cache and logging rules below hold identically everywhere. Hidden, identifier and opaque (`UNKNOWN`) columns SHALL never be profiled. Numeric/temporal columns SHALL be profiled by a single batched min/max query per call; categorical (text/boolean) columns by one top-values query per column. A forced refresh SHALL keep profiling only `sql_table`-backed models (sql-mode and query-backed models skipped).

#### Scenario: inspect_model profiles numeric columns with one query

- **WHEN** `inspect_model` renders a healthy model with several uncached numeric/temporal columns
- **THEN** exactly one min/max query is executed for them, and the rendered and persisted ranges are identical to what the search and `inspect`-column paths persist for the same columns

#### Scenario: Healthy-model parity across read paths

- **WHEN** the same uncached column on a healthy model is profiled via `inspect_model`, via `inspect` of the column at `compact=False`, and via a search column hit
- **THEN** each path persists the same `sampled`, `sampled_values` and `distinct_count`

#### Scenario: Forced refresh skips non-table-backed models

- **WHEN** a forced refresh runs over an sql-mode or query-backed model
- **THEN** no profiling query is executed for it and no error is reported

### Requirement: A successful profile always caches the column

A profiling query that succeeds SHALL leave the column cached, so later reads execute no profiling query for it: a categorical column with no non-NULL values SHALL store an empty `sampled_values` list, and a numeric/temporal column whose min and max are both NULL SHALL store `sampled` = `all NULL`.

#### Scenario: All-NULL numeric column is not re-profiled

- **WHEN** a numeric column whose values are all NULL is profiled successfully and the model is read again
- **THEN** its `sampled` is `all NULL` and the second read executes no profiling query

#### Scenario: Empty categorical column is not re-profiled

- **WHEN** a text column with no non-NULL values is profiled successfully and the model is read again
- **THEN** its `sampled_values` is `[]` and the second read executes no profiling query

### Requirement: Profiling failures are classified and bounded per call

When a profiling query fails, profiling SHALL run one probe query (a row count of the model through the same engine, policy and datasource) the first time a failure occurs in a call. If the probe fails, the failure SHALL be model-level: profiling of the model stops for the rest of the call. If the probe succeeds, the failure SHALL be column-level: profiling continues with the remaining columns and no further probe is run in that call; however, three consecutive column-level failures with no successful profiling query in between SHALL be treated as a model-level failure. When the batched numeric/temporal query fails on a model whose probe succeeded, each of those columns SHALL be re-profiled with its own query, so only the failing columns are recorded as failed. The failure type SHALL NOT be inferred from the exception class.

#### Scenario: Model whose every query fails

- **WHEN** a model whose every query fails (e.g. its table is missing) is read with N > 2 uncached columns
- **THEN** exactly two queries are executed (the first profiling query and the probe), no sample is persisted, and the remaining columns are not queried

#### Scenario: One bad column on a healthy model

- **WHEN** one categorical column's profiling query fails on a model whose other queries succeed
- **THEN** the probe runs once, every other uncached column is profiled and persisted, and only the bad column is recorded as failed

#### Scenario: Probe succeeds but every column query fails

- **WHEN** a model's row-count probe succeeds but every profiling query on its columns fails
- **THEN** profiling stops after three consecutive column failures, no further column is queried, and the model is recorded as failed at model level

#### Scenario: Failing numeric batch on a healthy model

- **WHEN** the batched min/max query fails because one numeric column's expression errors, on a model whose probe succeeds
- **THEN** each numeric column is re-profiled individually, the good ones are persisted, and only the bad one is recorded as failed

#### Scenario: Search hits on a failing model

- **WHEN** a search returns column hits on three uncached columns of a model whose every query fails
- **THEN** at most two queries are executed for that model during the hit refresh

### Requirement: Profiling failures are cached per engine

A recorded failure SHALL be cached in memory, scoped to the engine instance, for 1 hour; while cached, reads through that engine SHALL execute no profiling query for the failed model (model-level) or the failed column (column-level). The cache SHALL be keyed by the model's and column's definitions excluding their sample fields, so a changed definition is profiled again immediately. Failures SHALL NOT be persisted to storage. A forced refresh SHALL ignore both the sample cache and the failure cache, and SHALL still stop on a model-level failure and record it.

#### Scenario: Second read within the TTL

- **WHEN** a model whose every query fails is read a second time through the same engine within 1 hour
- **THEN** no query is executed for its profiling

#### Scenario: Retry after the TTL

- **WHEN** the same model is read through the same engine after more than 1 hour
- **THEN** profiling is attempted again

#### Scenario: Retry after an edit

- **WHEN** a failed column's or model's definition changes (e.g. its `sql` or `sql_table`) and the model is read again within the TTL
- **THEN** profiling is attempted again

#### Scenario: Engines do not share failures

- **WHEN** a model fails to profile through one engine and is then read through another engine (e.g. with a different row-level-security policy)
- **THEN** the second engine attempts profiling

#### Scenario: Forced refresh of a failing model

- **WHEN** a forced refresh runs over a model whose every query fails and whose columns already hold samples
- **THEN** it executes two queries, reports one error for the model, and leaves the existing samples unchanged

### Requirement: Profiling failures are logged

Every recorded profiling failure SHALL log exactly one WARNING naming the datasource, model and — for a column-level failure — column, plus the first line of the error; a model-level warning SHALL also state how many columns were skipped. Reads served from the failure cache SHALL NOT log at WARNING or above. A failure to persist a profiled sample SHALL log one WARNING per column and be reported in the refresh errors; the freshly profiled value SHALL still be returned for that read and SHALL NOT be recorded as a profiling failure.

#### Scenario: Model-level failure logs once

- **WHEN** a model whose every query fails is read twice within the TTL
- **THEN** exactly one WARNING is logged in total, naming the model and the number of skipped columns

#### Scenario: Persist failure keeps the fresh value

- **WHEN** a column profiles successfully but persisting its sample raises
- **THEN** one WARNING is logged for that column, the read renders the fresh value, and the next read profiles the column again

### Requirement: Profiling under a row-level-security policy stays in the engine

When the engine carries a session policy, profiling SHALL neither read nor write the persisted sample fields: every column SHALL be treated as uncached on first read, profiled samples SHALL be cached in memory per engine under the same keys and TTL as failures, and every read path SHALL render only these engine-scoped samples (or none), never the persisted ones. Search column hits SHALL have their text re-rendered from the engine-scoped column.

#### Scenario: Policy engine ignores persisted samples

- **WHEN** a column already holds persisted samples and is read through an engine with a session policy
- **THEN** the rendered samples come from a tenant-scoped profiling query, not from storage, and storage is not written

#### Scenario: Policy engine reuses its own samples

- **WHEN** the same column is read a second time through that engine within 1 hour
- **THEN** no profiling query is executed and the tenant-scoped samples are rendered

#### Scenario: Search hit text under a policy

- **WHEN** a search through an engine with a session policy returns a hit on a column with persisted samples
- **THEN** the hit's text carries the engine-scoped samples (or none), never the persisted ones
