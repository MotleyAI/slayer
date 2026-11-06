## Why

The cancellation fix shipped as 1.6.1 on `release-1.6.x` (PR #549), but `main` (tagged `v1.7.0`) still
has the old `_run_sync_in_thread`, so a cancelled blocking client call there still stalls the event
loop. This is the forward-merge step of the DEV-2206 release plan.

## What Changes

- Merge `origin/main` into this branch (a merge commit), resolving conflicts toward `main`'s structure,
  so a PR from this branch into `main` carries the fix, its tests and its spec requirement.
- Reword the one doc sentence the fix added so it matches `main`'s driver model (async drivers run
  metadata reads and `ping` on the async engine, not in a worker thread).
- No version bump: `main` stays at `1.7.0`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. The requirement "Cancelling a client's blocking database call returns at once" already reached
`openspec/specs/sql/execution/spec.md` via the archived 1.6.1 change and arrives on `main` by the merge
itself, so this change sets `skip_specs: true`.

## Impact

- `slayer/sql/client.py`, `tests/test_sql_client.py`, `openspec/specs/sql/execution/spec.md`,
  `pyproject.toml` (merge conflicts), `docs/configuration/sql-clients.md` (one sentence).
- No API change.
