## Why

On load, a stored query's legacy time literals (`'2024/01/01'`, zone offsets) are repaired only where the compared
column resolves through a hand-rolled raw-dict walk that follows forward-declared joins one adjacent hop at a time. A
reference the binder resolves — a short-form route over several hops, a hop over a join declared on the peer (a reverse
edge), or a column a query extension declares — keeps its legacy literal, and the stored query fails with
`TimeLiteralError` when it runs. The repair also reads Mode-B filter text with sqlglot, against system axiom 8 (one
reader of Mode B).

## What Changes

- The repair resolves each compared column exactly as the binder does, through the shared static resolver
  (`engine.extensions.query_scopes` / `QueryScope.resolve`): extensions applied, own qualifier dropped, incident hop in
  either declared direction, else a unique short-form route. The raw-dict resolver (`_stored_column_type`,
  `_query_host`, the raw `_extended` helper) is deleted.
- The shared static resolver honours per-segment quoting, as the binder does; existing callers are unchanged.
- The resolver reads typed in-memory views of the datasource's stored models, built by the same dict-upgrade steps a
  model load runs (no live refinement, no write-back, no filter repair); a view that fails is skipped, as the engine
  skips an unloadable peer.
- A memory's query reads the datasource storage identity resolution picks (priority applied).
- `core.syntax` gains a span reader of string literals compared directly with a column reference; storage no longer
  parses Mode-B text with sqlglot.
- An ambiguous, unresolvable or non-temporal comparison stays verbatim, as today.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/stored-upgrade`: "Stored legacy filter literals are repaired on load" resolves the compared column as the
  binder does — joins in either direction, a unique short-form route, query extensions, quoting.

## Impact

- `slayer/storage/base.py` (repair, typed views, shared dict-upgrade method), `slayer/storage/legacy_time_literals.py`
  (span splice only), `slayer/core/syntax.py` (literal-comparison span reader), `slayer/engine/extensions.py`
  (quoting through `resolve_path` / `QueryScope.resolve`).
- Tests: `tests/test_stored_upgrade_filter_literals.py`, `tests/_stored_upgrade_fixtures.py`, a `core.syntax` reader
  test.
- No API, MCP or REST change; no new dependency; no architecture-model change expected.
