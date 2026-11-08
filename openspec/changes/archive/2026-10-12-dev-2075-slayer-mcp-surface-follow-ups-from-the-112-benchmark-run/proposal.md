## Why

The slayer-evals run on motley-slayer 1.1.2 (DEV-2066's surface) left a cluster of failures: rules agents need sit only in help topics (gap filling, projecting only the asked columns), no example shows several child models or orphan child rows in one query, the `source_model` example over-roots at the listing model, and a union validation error buries the real cause between two irrelevant branch errors.

## What Changes

- `SlayerQuery.time_dimensions` owns the gap-filling rule (`time_spine.timestamp` with a lower `date_range` bound returns every bucket); it leaves `help.time` and `help.intro`.
- `SlayerQuery.measures` gains a several-child-models example (each aggregate over its own rows), the placement of child rows without a parent (the NULL row of a parent dimension when the population has it, and an ungrouped total), and a cross-model ratio example.
- `SlayerQuery.order` owns "return only the columns the question asks for; never add the sort value to measures"; the projection sentence leaves `help.workflow`.
- `SlayerQuery.source_model` makes rooting at the listed model conditional on the question asking for every such row; otherwise root where the facts are.
- The server `instructions` state why one query cannot fan out or double count.
- The MCP `query` argument is a discriminated union: an invalid query object or stage list reports only that form's validation errors.

## Capabilities

### New Capabilities

### Modified Capabilities
- `mcp/tool-surface`: the instructions carry the fan-out reason; the owning-field table gains the gap-filling, several-child, orphan-row, ratio, projection and conditional-rooting guidance, none of it repeated in help topics or instructions; the documented-idiom list gains the matching executable idioms.
- `mcp/query-tool`: an invalid `query` argument reports only the errors of the form its JSON type selects.

## Impact

- Code: `slayer/core/query.py` (field descriptions), `slayer/mcp/server.py` (`SERVER_INSTRUCTIONS`, `query` parameter type), `slayer/memories/help_content/00_intro.md`, `02_workflow.md`, `05_time.md`.
- Field descriptions are domain-wide: they also reach REST OpenAPI wherever `SlayerQuery` is used.
- The `query` parameter's JSON schema changes from `anyOf` to `oneOf`.
- Docs: `docs/reference/mcp.md` (one sentence on the validation error).
