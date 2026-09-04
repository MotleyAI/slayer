# Design

## Context

See proposal.md — Why. Relevant current state: `_format_output` (slayer/mcp/server.py) dispatches markdown/json/csv rendering, and warnings already render in all three formats (`_format_warnings`, `_csv_warning_comments`, json `{"data","warnings"}` shape). The `query` tool takes a run-by-name shortcut for a bare stored-query-model name with no overrides; passing `limit` disables that shortcut (pre-existing behavior). `SlayerQuery.limit` is emitted as the outermost SQL LIMIT.

## Goals / Non-Goals

- Goal: cap lives entirely in the MCP layer; the engine, REST API, Flight, and PG facade are untouched.
- Non-goals: no config/env knob for the default cap; no exact-total row counting; no ceiling on explicit limits.

## Decisions

1. **`limit` is the knob; no new tool argument** (over a separate `max_rows` arg). One knob matches Storyline's agent-facing contract and avoids two interacting parameters on an already 16-parameter tool. Explicit `limit` is fully trusted — the maintainer explicitly rejected a hard ceiling (Storyline's 10,000): a caller overriding the default knows what they're doing.
2. **Push-down + universal slice** (over post-hoc slice alone). When no explicit limit, the structured path sets the query limit to cap+1 (21) so the database never ships the full runaway result; the post-execution slice is the universal guarantee for paths push-down can't reach (run-by-name, DAG output, explain plans). Consequence: notices say "more rows exist" uniformly; exact totals are unknowable in the pushed-down case.
3. **Notice rides the warnings union** (over dedicated per-format footers). A `ResponseTruncationWarning` (kind `"truncated"`, fields `returned_rows`, `hint`) joins `AnySlayerWarning` in slayer/core/warnings.py; the MCP layer appends it before formatting. Zero new rendering branches; the three formats stay in sync by construction. The engine never emits this kind — it is additive schema only from the REST API's perspective.
4. **Cap decision uses the caller's arguments, not the executed result.** `len(result.data) > limit` must never be the trigger — with an explicit limit the MCP layer does no slicing at all (even for explain plans whose row count is unrelated to the SQL limit). Only the no-limit paths slice, at 20.
5. **query_nested: root stage only.** The root (last) entry of `queries` controls the cap; other stages' limits are irrelevant. Push-down copies the root dict rather than mutating the caller's input.

## Risks / Trade-offs

- [Pushed-down `LIMIT 21` visible in show_sql/dry_run output] → honest: it is the SQL that runs; docstrings note the default cap.
- [Run-by-name notice says "pass a higher 'limit'", but passing `limit` switches to structured execution with different variable precedence] → pre-existing sharp edge, kept out of the one-line notice deliberately; uniform hint text wins.
- [Truncating an explain plan (no-limit case) can mangle plan readability] → accepted for uniformity; a giant EXPLAIN ANALYZE floods context the same way rows do, and `limit` lifts the cap.

## Migration Plan

Additive behavior change in one release; no storage or schema migration. Rollback = revert.
