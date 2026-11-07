## Why

The SQL-compatible grammar, row-level expressions in every column position, the portable function library and
list columns all shipped without their docs. Several pages and agent-facing texts (help topics, MCP field
descriptions) are now wrong. Many rules are documented nowhere. Old-parser code and comments remain.

## What Changes

- Fix every wrong statement in `docs/`, `slayer/memories/help_content/`, the MCP tool and field descriptions,
  and the `slayer/core/query.py` / `slayer/core/models.py` Field descriptions; document the missing rules where
  agents and users look for them.
- The MCP `query` schema gains one clause each on `measures` (`FILTER (WHERE …)`), `variables` (literal-only
  binding), `filters` (missing operators and list functions) and `source_model` (per-query expression form);
  its size budget is re-set to the new measured size plus ~10%.
- Delete the four `docs/interfaces/{cli,mcp,python-client,rest-api}.md` duplicates after folding their unique
  content into `docs/reference/*`. Add notebook 08 to the nav. Fill the missing tools, routes and commands in
  the reference pages.
- New guard tests:
  - every docs page is in the nav;
  - no issue numbers in `docs/`;
  - every Mode-B example in docs and help topics parses.
- Remove the dead `escape="python"` variable-substitution regime, together with `substitute_variables`'
  `escape` parameter (internal signature change). Also remove `_column_summary`, the "unsupported AST node
  Set" error wording, and stale old-parser comments.
- Regenerate `examples/comparisons/matrix.json` with Q22 marked `partial`. Rewrite and re-execute notebooks 02
  and 08.

## Capabilities

### New Capabilities

### Modified Capabilities
- `mcp/tool-surface`: the owning-field guidance list gains the new `measures` / `variables` / `filters` /
  `source_model` guidance; the documented-idioms list gains the new examples (and any existing untested
  example), each required to execute as documented.

## Impact

- **Code:** `slayer/core/query.py`, `slayer/core/models.py`, `slayer/core/syntax.py`, `slayer/ir/variables.py`,
  `slayer/mcp/server.py`, `slayer/sql/window_detect.py`.
- **Docs:** `docs/**`, except the pages owned by the sibling follow-ups. Also `slayer/memories/help_content/*.md`,
  `zensical.toml`, and `examples/comparisons/matrix.{yaml,json}`.
- **Tests:**
  - `tests/test_mcp_surface_budget.py` (budget constant, markers);
  - `tests/test_models.py` and `tests/test_query_optional_blocks.py` (escape regime);
  - `tests/test_variables_planner.py`, `tests/test_help_seed.py`, `tests/test_docs_dev_1560.py`;
  - the two brace-error `match=` strings;
  - new guard tests and idiom execution tests.
- No public API, persistence or SQL-emission change.
