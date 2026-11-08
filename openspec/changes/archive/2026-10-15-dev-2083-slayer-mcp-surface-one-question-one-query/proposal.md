## Why

The slayer-evals run on `98ca207` (Opus 5.5, 53 tasks × `slayer` / `slayer+python`) still shows 25 % / 36 % of trials running more than one query, 7–9 trials per profile adding stages where one stage does the job — partly because the `name` description says "use stages to … filter after a window or rank" while `filters` says rank filters work in the same stage — and examples grouped by `customers.region_id` copied into answers that list region ids instead of names.

## What Changes

- The server `instructions` lead with the rule in capitals: ONE QUESTION = ONE QUERY — one `query` call, no splitting, no recombining results by hand or in Python, no stages for what one stage can do — followed by what one query computes and why it never fans out.
- The `query` tool description opens with the same two sentences; its running-total capability no longer claims to look back before `date_range`.
- The `name` (stages) description makes stages the documented exception: what needs no stage, that a filter on a transform's value applies after it, and that a stage is needed only to drop rows by a dimension or time condition a cross-row value must still see (or to join / reuse a whole result).
- The older copies of both rules leave `date_range`, `help.queries`, `help.workflow`, `help.transforms` and `help.time`.
- Examples group and partition by `customers.regions.name`, not ids; `dimensions` requires a readable label per row and the id alongside a name that may repeat.

## Capabilities

### New Capabilities

### Modified Capabilities
- `mcp/tool-surface`: the instructions and the `query` description carry the one-query rule; the owning-field table gains the stage rule (`name`) and the readable-label rule (`dimensions`), with retired copies guarded; documented idioms gain the cross-row-value-under-a-filter behaviour and use readable dimensions.

## Impact

- Code: `slayer/mcp/server.py` (`SERVER_INSTRUCTIONS`, `query` docstring), `slayer/core/query.py` (`name`, `dimensions`, `measures`, `filters`, `date_range` descriptions), `slayer/memories/help_content/02_workflow.md`, `04_transforms.md`, `05_time.md`, `07_queries.md`.
- Field descriptions are domain-wide: they also reach REST OpenAPI wherever `SlayerQuery` is used.
- No engine behaviour, API shape or import changes; no `docs/` change.
