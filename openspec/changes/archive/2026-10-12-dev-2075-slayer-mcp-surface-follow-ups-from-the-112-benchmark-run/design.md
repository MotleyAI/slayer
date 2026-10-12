## Context

See proposal.md (Why) and the delta specs. DEV-2066's constraints hold: tool descriptions and `instructions` ≤ 2048 chars (Claude Code's cut), input schemas passed whole, the `query` schema within `QUERY_SCHEMA_BUDGET` (28,000; 25,541 before this change), each concept in one place, no benchmark task text.

Plan-time probes (in-process DuckDB):
- Rooted at `customers` by `regions.name`, parentless orders (NULL or dangling `customer_id`) count in the NULL-region row only when a customer has no region; otherwise they appear in no row. Ungrouped, `sum(orders.amount)` includes them. This is the specified engine behaviour (`queries/semantics` › Aggregates are virtual models; arc42 semantics Axiom 6), so the issue's unconditional "already counted in the NULL-group row" is documented conditionally.
- `time_spine.timestamp` by month with `date_range` `"2025"` and no `source_model` returns 12 rows; an empty month has `count` 0 and `sum` NULL.
- FastMCP validates the `query` argument against `str | SlayerQuery | list[SlayerQuery]` by trying every branch, so a bad dict reports three errors (valid string / the real cause / valid list). FastMCP parses JSON strings into dicts / lists first.

Architecture: `slayer.mcp` and `slayer.memories` are buckets without a node arc42 file; `slayer.core.query` is `core`. Principles applied: system P9 (examples use dotted paths), P12 (Pydantic); core P4 (the reserved-name error text is unchanged). No arc42 or model edits; no import changes.

## Goals / Non-Goals

**Goals:** every rule an agent acts on sits on the field where it acts, once, with a drift guard that also covers help topics and instructions for the moved rules; the union error names only the real cause.

**Non-Goals:** changing engine semantics; REST `QueryRequest` (it does not use the union); FastMCP's error framing ("Error executing tool query: … For further information visit …"); running the evals (driven from slayer-evals).

## Decisions

**D1 — Owners.** Gap filling → `time_dimensions` (where the agent picks the bucketed column; `date_range` already carries the look-back rules). Several child models, parentless child rows and the cross-model ratio → `measures`, beside the joined-aggregate bullet. Projection → `order` (the failure is adding the sort value; `measures` is already the longest field). Conditional rooting → `source_model`. Fan-out reason → `instructions`. Removed copies: `help.time` `## Gaps`; `help.intro` judgment call 5 and "gaps" in its `help.time` line; `help.workflow` "Project exactly the columns the question names."; the help.time preview's "gaps"

**D2 — Exact wording.**
- `time_dimensions` appends: "Buckets without rows are absent. To return every bucket, empty ones included, bucket time_spine.timestamp with a lower date_range bound and measure models by dotted path: {"dimension": "time_spine.timestamp", "granularity": "month", "date_range": "2025"} with sum(orders.amount) returns all twelve months. It also lines several models up on one time axis."
- `measures` first paragraph: "Formulas combine aggregations with arithmetic, also across models: rooted at customers, sum(orders.amount) / count(*) divides the orders' total by the number of customers." (Not the benchmark's `count(orders.id) / count(*)`, and labelled exactly: ungrouped, the orders' total includes parentless orders.)
- `measures` joined-aggregate bullet appends: "Rooted at customers, sum(orders.amount) and sum(returns.amount) in one query are each computed over their own rows: several child models never fan out or double count. A child row without a parent (an order with no customer) has NULL parent columns: it counts in the NULL row of a parent dimension when that row exists, and in an ungrouped total; never add it again."
- `order` appends: "Return only the columns the question asks for: never add the sort value to measures just to sort by it."
- `source_model` becomes: "The population: the rows the result lists. When the question asks for every X (each region, zero included), root at X so rows without facts appear: rooted at regions, <existing count + named conditional example> list every region, including those without orders (a count over no rows is 0, any other aggregate NULL). Always name a conditional aggregate. Otherwise root where the facts are, or omit to infer the smallest model determining every dimension and row-level condition column (reported in the response); rows without facts then drop out. A saved model or stage name, an inline extension {…} adding per-query columns, or a full inline model."
- `instructions`, after "Do not assemble …": "Aggregates over several joined models in one query are each computed over their own rows, so one query cannot fan out or double count; split only when a stage must re-query a whole result."

**D3 — Markers.** Schema-owned markers join `MARKER_OWNERS`: `time_spine.timestamp` (`time_dimensions`), `sum(returns.amount)` and `an order with no customer` and `sum(orders.amount) / count(*)` (`measures`), `only the columns the question asks for` (`order`), `where the facts are` (`source_model`). A second, sole-owner guard scans the `query` description, every schema description, the `instructions` and every help topic body for these markers plus `cannot fan out` (owner: `instructions`). Existing markers keep the schema-only scope: help topics legitimately hold their long-tail copies (DEV-2066 D2).

**D4 — Discriminated `query` argument.** The parameter type becomes a callable `Discriminator` over `Tag("name")` str / `Tag("object")` `SlayerQuery` / `Tag("stages")` `list[SlayerQuery]`, choosing by Python type (str → name, list → stages, else object), keeping its `Field(description=...)`. Pydantic then validates only the chosen branch: one error at `query.object.…` or `query.stages.<i>.…`. Alternative — post-filtering the `ValidationError` — rejected: it patches the symptom, needs to know which branch "should" have matched, and leaves the schema non-exclusive. The schema becomes `oneOf` (already accepted by `tests/test_dev1858_mcp_query_tidy.py`).

**D5 — Test data.** The existing idiom dataset and its pinned values stay untouched. A second seeded DuckDB dataset in `tests/_mcp_idiom_fixtures.py` adds a `returns` model joined to customers, a region-less customer, a customerless order and a dangling-customer order; the "no NULL population row" case runs on that dataset without the region-less customer (a seed flag). The spine idiom runs on the existing dataset (May 2025 has no orders).

## Risks / Trade-offs

- [Schema growth] → ~1k chars; if the measured `query` schema exceeds `QUERY_SCHEMA_BUDGET`, stop and ask rather than raising it.
- [Examples overfit the benchmark] → the ratio example deliberately differs from the benchmark's formula; the several-child example is the issue's own prescription.
- [Removing the gap rule from `help.intro` weakens the overview] → the field description is always visible; the overview keeps pointing to the fields.
- [`oneOf` in the schema] → JSON Schema standard, already present elsewhere in the `query` schema.
