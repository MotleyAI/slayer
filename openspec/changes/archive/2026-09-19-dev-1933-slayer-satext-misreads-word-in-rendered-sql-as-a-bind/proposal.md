## Why

The SQL client runs fully rendered SQL through SQLAlchemy's `text()`, whose
bind-parameter regex reads any `:word` — even inside a string literal, such as a
regex non-capturing group `(?:…)` — as a bind parameter with no value, and the
query fails (`A value is required for bind parameter 'too'`). It hit production
on 2026-09-17 through an ad-hoc `ModelExtension` regex column; the defect dates
to the first commit and only needs a `:`-followed-by-word shape to surface.

## What Changes

- Every statement the SQL client executes leaves through **one verbatim door**:
  driver-level execution with no parameter set. `text()` is no longer used for
  rendered SQL — nor for anything else in the client.
- All **four** rendered-SQL paths are covered — async and sync query execution
  *and* the async and sync column-type probes (the issue names only the two
  execute sites; the probes run the same rendered SQL).
- `%` behaviour is unchanged: the driver receives a single `%` and, given no
  parameters, does not interpolate — the same server-side SQL as today's
  double-then-undouble path.
- New `sql` node principle (arc42 §3.13, wording approved) pinned by a test.
- One sentence in the models docs: column SQL reaches the driver verbatim.

## Capabilities

### New Capabilities

- `sql/execution`: how rendered SQL reaches the database — verbatim, with no
  bind-parameter or format-directive reinterpretation on any execution path.

### Modified Capabilities

<!-- None: query semantics and rendering are untouched; only the wire boundary
     between the client and the driver is specified. -->

## Impact

- **Code**: `slayer/sql/client.py` only — a helper pair replaces every
  `sa.text(...)` execution (rendered SQL, timeout SETs, read-only SETs).
- **Normative harness**: `architecture/sql.arc42.md` §3 gains item 13 (approved
  wording); `architecture/index.yaml` `sql` node gains `specs: [sql]` at archive
  time (the group only exists on disk once archived).
- **Tests**: new `tests/test_dev1933_verbatim_execution.py` (guard + every door +
  end-to-end) and `tests/integration/test_dev1933_postgres_verbatim.py`; one
  focused case in each Tier-1 dialect suite; the Snowflake statement-timeout
  mocks retarget from `execute` to `exec_driver_sql` (consented).
- **Docs**: `docs/concepts/models.md`, Columns section.
- **Behaviour**: SQL containing `:word` (inside literals or regexes) now
  executes; nothing else observable changes.
