## Why

One query cannot combine two models or stages that no join path connects: rooted at `customers`, `count(events.id)` fails with "no route to target", so "events per customer" needs two queries and a division done by the agent. SLayer already defines how unrelated datasets meet — a detached dataset meets the time spine as a product — so the algebra can admit the general case without new semantics, provided the product never multiplies result rows.

## What Changes

- A query-frame reference whose first hop names a dataset no join path reaches from the population (a model or query-local stage in the same datasource) resolves through a product hop `P × D` instead of failing; route search never takes it.
- Aggregates across `×` are homed on `D` and resolve per `to_many_handling` like any unattributable aggregate (broadcast by default; spine dimensions attribute through `D`'s own axis); they are computed over `D`'s own rows and attached cardinality-neutrally.
- Every other row-level read across `×` (dimension, time dimension, row-level computed-dimension operand or order key, raw rows) fails with a new typed checker error naming the remedy — so the result's rows are always the population's.
- Filters reading `D` keep the existing association semantics (Axiom 14).
- New warning kind `unrelated_dataset`, emitted whenever an aggregate crosses `×`.
- `recommend_root_model` resolves an attachment item on an unrelated dataset through the product hop and flags it `unrelated: true` (new `ItemPath` field).
- The `ValidationError` for a stage join with empty `join_pairs` names the aggregate remedy.
- Semantics Axiom 3 gains sub-clause 3.1 (Unrelated datasets).

## Capabilities

### New Capabilities
- `queries/unrelated-datasets`: resolution, aggregation, row-level refusal, filters, warnings, and the no-row-explosion guarantees for datasets with no join path from the population.

### Modified Capabilities
- `queries/population`: root recommendation resolves attachments on unrelated datasets through the product hop and reports them as such.
- `mcp/tool-surface`: the `measures` guidance names the unrelated-dataset behaviour, and the documented idioms gain the unrelated ratio.

## Impact

- Code: `slayer/core/join_walker.py` (product hop resolution), binding / population / join-safety in `slayer/engine/`, the checker (new typed error), `slayer/core/warnings.py`, `slayer/core/models.py` (join_pairs message), root recommendation, MCP `measures` description.
- APIs: new warning kind on every surface; new `ItemPath.unrelated` field on Python / REST / CLI / MCP `recommend_root_model`; queries that failed with "no route to target" now return values with warnings.
- Docs: `docs/concepts/queries.md`, `recommend_root_model` reference docs; `architecture/semantics.arc42.md` Axiom 3.1.
- Downstream: slayer-evals' events-per-customer guard task becomes answerable in one query.
