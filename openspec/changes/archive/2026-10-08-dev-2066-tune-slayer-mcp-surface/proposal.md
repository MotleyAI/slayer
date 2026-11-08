## Why

Claude Code truncates every MCP tool description and the server `instructions` to 2048 chars, while passing input schemas in full. The `query` description is 11,079 chars, so in the slayer-evals baseline (DEV-2055 / DEV-2065) agents never saw `partition_by`, `window=`, nesting, transforms or cross-model semantics, and fell back to pulling rows and assembling answers themselves. Tool parameters carry no schema descriptions, and the advertised help call (`inspect(memory:help.intro)`) returns only a 77-char preview.

## What Changes

- Every MCP tool description is dedented and fits 2048 chars (checked at server build); the server `instructions` too. The `query` description becomes a map: the single-query capability list, pointers to the query fields, and one batch call for the full help reference.
- Every top-level tool parameter gets a schema description; docstring `Args:` blocks go.
- The query-language reference moves onto the query schema fields where the agent writes each value (capability + minimal runnable syntax + error-preventing rules), each concept in exactly one field; `QueryRefinement` and `ModelMeasure.formula` point to the owning field instead of copying text.
- Five help topics (`help.aggregations`, `help.transforms`, `help.time`, `help.joins`, `help.queries`) hold the long tail the fields do not; `help.workflow` gains the query method / verify / filter-literal discipline; `help.intro` is re-pointed. Topics are self-contained (skill-shaped) for a later MCP-skills surface.
- `inspect` returns the full body of any `help.*` memory regardless of `compact`, on every surface (MCP, REST, CLI, `SlayerClient`).
- Optional `always_load_query` server option (flag `--always-load-query` on `slayer mcp` / `slayer serve`, env `SLAYER_MCP_ALWAYS_LOAD_QUERY`, default off) marks `query` with Claude Code's `anthropic/alwaysLoad` meta; the `mcp` dependency floor rises to the first release supporting tool `meta`.
- `MeasureNameCollidesWithColumnError` suggests a free measure name.

## Capabilities

### New Capabilities
- `mcp/tool-surface`: what the MCP server advertises to agents — description and instructions budgets, parameter descriptions, the query-language placement across schema fields and help topics, help-topic reachability and full-body inspection, and the always-load option.

### Modified Capabilities
- `queries/type-errors`: the measure-name/column collision error names a free alternative in its `suggestion:` line.

## Impact

- Code: `slayer/mcp/server.py` (all tool signatures and descriptions, instructions, `create_mcp_server`), `slayer/core/query.py` and `slayer/core/models.py` (field / class descriptions), `slayer/memories/help_seed.py` + `help_content/`, `slayer/inspect/service.py`, `slayer/engine/compile/projection.py` + `slayer/engine/elaborate_env.py` + `slayer/core/errors.py`, `slayer/cli.py`, `slayer/api/server.py` (`create_app`).
- Field descriptions on the core query models are domain-wide: they also appear in REST OpenAPI wherever those models are used.
- Dependency: `mcp` floor in `pyproject.toml` (+ lockfile, import remedy text, pin test).
- Docs: `docs/reference/mcp.md`, `docs/interfaces/mcp.md`, `docs/reference/cli.md`, `docs/interfaces/cli.md`, `docs/reference/rest-api.md`, `docs/concepts/memories.md`.
