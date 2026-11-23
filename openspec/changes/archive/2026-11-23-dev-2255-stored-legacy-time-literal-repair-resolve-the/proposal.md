## Why

On load, a stored query's legacy time literals (`'2024/01/01'`, zone offsets) are repaired, and its date-only comparisons
keep their whole-day meaning, only where storage's own raw-dict walker can type the compared column. That walker has no
short-form routing and ignores quoting, so a routed DATE or TIMESTAMP column keeps its legacy literal (the query then
fails with `TimeLiteralError`) or silently compares with midnight. The repair also reads Mode-B text with sqlglot, against
system axiom 8 (one reader of Mode B).

## What Changes

- One typed step rewrites a stored query's literals before extension inlining and qualification: each compared reference
  is resolved by the binder's resolver (`resolve_path`), the pre-2.0 reading qualification keeps, over the typed datasets
  qualification reads; an extension-declared column is typed by the inliner's own lookup.
- The step covers both the legacy-literal repair and the whole-day upgrade of stored queries, memories included; a
  dataset's own formulas and field filters keep their current typing.
- A memory's query is typed in the datasource its qualification reads (datasource priority applied).
- The shared resolver honours per-segment quoting, and qualification keeps a quoted leaf quoted.
- `core.syntax` gains a reader of string literals compared with a reference; storage no longer parses Mode-B text with
  sqlglot.
- A comparison the step cannot type is left as written; qualification then keeps or refuses the document, as today.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/stored-upgrade`: the legacy-literal repair and the whole-day upgrade type the compared column as the binder
  resolves it — a short-form route, joins in either direction, extension columns, quoting; a memory's query is typed in
  the datasource its qualification reads; qualification keeps a quoted leaf quoted.

## Impact

- `slayer/storage/base.py` (the typed step in one guarded conversion scope with inlining and qualification),
  `slayer/storage/date_only_migration.py` (query and memory upgrades become version stamps),
  `slayer/storage/legacy_time_literals.py` (no sqlglot), `slayer/storage/qualified_references.py` (shared population
  choice, quoting), `slayer/storage/extension_fields.py` (exposed column lookup), `slayer/core/syntax.py` (literal
  comparison reader), `slayer/core/date_only.py` (per-segment quoting), `slayer/engine/extensions.py` and the resolvers
  below it (quoting).
- Tests: `tests/test_stored_upgrade_filter_literals.py`, `tests/_stored_upgrade_fixtures.py`,
  `tests/test_syntax_literal_comparisons.py`, `tests/test_resolve_path_quoting.py`, whole-day upgrade tests.
- No API, MCP or REST change; no new dependency; no architecture-model change expected.
