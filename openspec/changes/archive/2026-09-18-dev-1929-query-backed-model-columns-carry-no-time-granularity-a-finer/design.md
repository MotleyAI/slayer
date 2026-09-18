# Design

## Context

See proposal.md — Why. After DEV-1471 the refinement rule is `check_time_dimension_column(name, column_type, upstream_granularity, requested_granularity)` in `slayer/engine/elaborate_env.py`, fed by `_time_dimension_column_facts` in `slayer/engine/binding.py`: the `StageSchema` arm reads `StageColumn.type` / `.granularity`; the `ModelScope` arm walks joins to the terminal model's `Column` and returns its `type` and `None`. Every path from a `StageColumn` to a `Column` drops the granularity: `_expand_query_backed_model` (`slayer/engine/query_engine.py`, the cache builder behind both `_validate_and_populate_cache` and every `_resolve_model`) and `synthetic_model_from_stage_schema` (`slayer/ir/source_bundle.py`, the sibling stand-in used by `_stage_scope_and_bundle` for `ModelExtension`-over-sibling and threaded as a referenced model for stage joins). A `TimeTruncKey` slot's type is read off the underlying column, which the checker already requires to be temporal, so a bucketed stage column is always temporal.

Constraints: semantics Axiom 9 (the rule inspects the column's type facts, never where they came from) and 11.3; engine P1 (typed pipeline), P3 (the fact rides the bundle's model), P5 (schemas compose), P9 (the raise stays in the checker); core P4 (typed error, stable text pinned by the raise ledger); system P11 (additive persisted field, no version bump), P12, P14 (ingestion never writes it).

## Goals / Non-Goals

**Goals:**
- The refinement rule is total across model and stage datasets with no scope-kind branch: the granularity is a type fact on every column carrier the binder can reach.
- One message for the three origins (stage, query-backed cache, hand-set).

**Non-Goals:**
- Merging `Column` and `StageColumn`, or extracting their shared fact core (DEV-1939).
- Importing dbt `time_granularity` / Cube granularities onto the field (DEV-1938).
- Re-rendering pre-existing query-backed caches; skipping the redundant same-granularity re-truncation in SQL.

## Decisions

1. **Honoured everywhere, optional, cautionary docstring.** A hand-set `granularity` on a table-backed column is honoured exactly like the engine-stamped one; the field description tells authors to set it only when sure. Alternative (engine-managed only, ignored on table-backed models) rejected: it needs a model-kind branch in the binder plus a save-time reject, for a worse end state; the declaration is unverified but a wrong value yields a spurious rejection, never a wrong answer.
2. **Construction-time validator.** `Column` rejects a set `granularity` when `type` is not DATE / TIMESTAMP, like `filter` and `allowed_aggregations` already fail at construction. It cannot fire in the planner (see Context: a bucketed stage column is always temporal).
3. **One generalised message; ledger row edited (approved).** `TimeDimension 'x' cannot re-bucket to 'day': its column is already bucketed at 'month', which does not nest into 'day'. Request the same or a nesting-coarser granularity, or bucket the raw column instead.` Alternative (two messages keyed by scope kind) rejected: the checker would consult construction, which Axiom 9 forbids. The DEV-1471 tests assert substrings only, so only `tests/_dev1871_raise_ledger.py` changes.
4. **Names kept.** `upstream_granularity` on the checker and `BoundTimeDimension` stays (the bucketing did happen upstream of the time dimension, wherever it lives); docstrings widen. A rename would churn the DEV-1471 call sites for no behavioural gain.
5. **Both StageColumn→Column seams stamp it** (Codex finding, folded): `_expand_query_backed_model` and `synthetic_model_from_stage_schema` each pass `granularity=sc.granularity`. Two hand-copies are accepted here and recorded as the motivation for DEV-1939.
6. **No schema-version bump; stale snapshots accepted.** Pre-existing query-backed models keep a granularity-less persisted snapshot until their next save; query-time behaviour is correct immediately because the virtual model is rebuilt on every resolve. The docs already state the cache is written only on save.
7. **MCP surfaces, minimal** (Codex finding, folded): one clause naming `granularity` in the `create_model` and `edit_model` column-field lists; `render_model_inspection` (`slayer/inspect/model_render.py`) emits `granularity` in its JSON column payload when set (a conditional key, absent when unset) — that JSON is what `inspect_model` returns. Set / preserve / clear / reject fall out of the existing dump-merge-revalidate upsert. (`_model_to_summary` in `slayer/mcp/server.py` is unused dead code with no callers, NOT the inspect surface; the earlier plan named it in error. The markdown inspect table keeps fixed columns, so granularity surfaces only in the JSON shape, matching the scenario's "no such entry when unset".)
8. **Spec placement.** New capability `models/column-granularity`; `queries/time-dimensions` keeps the stage requirement under its name (text widened for the message) and gains two ADDED requirements, avoiding a RENAMED+MODIFIED pair on one requirement.

## Risks / Trade-offs

- [A wrong hand-set granularity rejects valid queries] → the error names the recorded bucket and the docstring warns; the remedy is to remove the declaration.
- [Ledger message bytes] → finalise the text before editing the row; the parity test pins it.
- [Existing persisted YAML has no `granularity`] → Pydantic default `None`; loading is unchanged.

## Migration Plan

No data or schema migration; the field is additive with default `None`. Rollback = revert.
