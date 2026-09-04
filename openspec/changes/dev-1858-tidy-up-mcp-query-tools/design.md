# Design — tidy up MCP query tools

## Context

`engine.execute()` already accepts `SlayerQuery | dict | list | str` (`slayer/engine/query_engine.py:741`); both MCP tools are wrappers that re-encode subsets of that union. See proposal.md — Why.

## Goals / Non-Goals

- Goal: the MCP `query` tool becomes a thin, semantics-free wrapper over `engine.execute` — no MCP-private dispatch rules.
- Non-goals: any change to REST (`POST /query` keeps both body shapes), CLI, Python client, engine, or core; any help-content rewrite (already json-shaped).

## Decisions

- **Bare string = run-by-name only** (engine semantics, no MCP fallback). Previously a non-query-backed name silently wrapped into `SlayerQuery(source_model=name)`; now it raises the engine's "not query-backed" error, which itself names the fix. Alternative (keeping the fallback) rejected: it reintroduces exactly the private unwrapping this change retires.
- **`query_nested` hard-deleted** — no stub/alias. Agents re-read the tool list every session; a stub is dead schema weight.
- **`strict` / `distinct_dimension_values` live only inside the query json** — they are `SlayerQuery` fields; keeping them as tool args would preserve a second spelling. The "strict unsupported with run-by-name" guard is deleted as unrepresentable (a bare string carries no strict field).
- **One uniform output path**: the old run-by-name branch skipped the attributes block; unified handling appends it whenever present.
- **Docstring content is preserved, restructured** — the measure-formula catalog, three `source_model` forms, filter syntax, multi-stage list rules (absorbed from `query_nested`), and full variables precedence (runtime > named-stage > outer-query > model.query_variables). `slayer/mcp/server.py` docstrings are the agent-facing product surface and must not be trimmed.
- **`DECISIONS.md` untouched** (Codex suggested updating its stale `query_nested` mention): it is a retired, dated historical record.

## Risks / Trade-offs

- [Agents lose the one-shot `query(source_model="orders")` browse gesture] → the error message tells them the exact replacement; docs show the dict form.
- [Union `str | dict | list[dict]` schema quality in FastMCP] → same anyOf mechanism the current `source_model: str | ModelExtension | SlayerModel` already uses; pinned by a schema regression test.

## Migration Plan

Single PR; no data or storage migration. Callers of the removed forms are only this repo's tests and docs (Storyline verified unaffected).
