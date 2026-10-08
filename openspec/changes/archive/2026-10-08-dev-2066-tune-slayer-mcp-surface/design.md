## Context

See proposal.md (Why) and specs/mcp/tool-surface for the requirements. Constraints that shape the approach:

- Claude Code cuts each MCP tool description and the server `instructions` at 2048 chars (`CLAUDE_CODE_MAX_MCP_DESCRIPTION_LENGTH`, practically never set), but passes the input schema whole, `$defs` / `$ref` included. Codex renders schemas differently; its surface is DEV-2068 and out of scope.
- FastMCP does not dedent docstrings; `@mcp.tool(description=...)` replaces the docstring; `param: Annotated[T, Field(description=...)] = default` emits the description beside the property's `$ref` / `anyOf`.
- Pydantic class docstrings become `$defs` descriptions, so they are agent-visible text too.
- `inspect(entity_type="memory")` with the default `compact=True` returns only the one-line preview; `descriptions_max_chars` truncates both preview and body.
- `help_seed` deletes every `RETIRED_HELP_IDS` id before upserting the current topics.
- Field descriptions on `SlayerQuery`, `TimeDimension`, `ModelMeasure` also reach REST OpenAPI wherever those models are used; REST `QueryRequest` redeclares fields without descriptions.

Architecture: `slayer.mcp`, `slayer.inspect`, `slayer.cli`, `slayer.api` are in the `surfaces` bucket (no node arc42 file); `slayer.memories` is the `memories` bucket (`surfaces → memories` arrow exists). Applicable principles: system §3 P9 (dotted-canonical — `partition_by` keys documented as dotted paths), P10 (one Mode-B language, documented once), P12 (Pydantic); core P4 (typed errors with the stable `suggestion:` line). No arc42 or model edits.

## Goals / Non-Goals

**Goals:** everything an agent needs to discover a capability is visible by default (≤ 2048 description + full schema); the long tail is one `inspect` call away and that call returns content; each concept is written once.

**Non-Goals:** `search` ranking or indexing; serving help as MCP skills (DEV-2067 — topics here are shaped so it can serve the same files); Codex surface (DEV-2068); the garbled auto-name of `sum(iif(...))` (DEV-2070 — examples here always name conditional aggregates); REST `QueryRequest` descriptions; running the evals (driven from DEV-2065).

## Decisions

**D1 — Docstring stays the single source; a budgeted helper registers it.** `_agent_description(fn)` = `inspect.cleandoc(fn.__doc__)`, raising at build if over `MAX_TOOL_DESCRIPTION_CHARS = 2048`; every tool registers via `@mcp.tool(description=_agent_description(fn))` (the same budget check guards `instructions`). Alternative — a separate description table — rejected: two copies drift.

**D2 — Fields carry capability + minimal syntax; help topics carry the long tail.** A field description holds what the field does, each capability it enables as one line with a runnable example, and every rule whose violation errors or silently gives a wrong answer. Full catalogues, edge semantics (empty input: counts 0, others NULL; dialect limits; the nesting parameter-grain rule; transform nesting limits), the time-point grammar, routing / ambiguity detail and stage naming live in help topics cited by exact call. Alternative — the full reference on fields — rejected: a ~30KB schema on every request buries the behaviour-changing line, and discipline text has no natural field.

**D3 — Owners.** `SlayerQuery.measures` owns the expression capabilities (aggregation, `partition_by`, `window`, nesting, transforms, cross-model semantics) because that is where agents write them; `ModelMeasure.formula` and every `QueryRefinement` field point to the owning `SlayerQuery` field (the `_refinement_field` copy of descriptions goes). Class docstrings that would repeat field guidance are trimmed to a one-line identity. Owner table: specs/mcp/tool-surface, "Query-language guidance lives on the owning schema field, once".

**D4 — Five help topics, reusing retired ids.** `help.aggregations`, `help.transforms`, `help.time`, `help.joins`, `help.queries` (removed from `RETIRED_HELP_IDS`; the others stay retired). Each is self-contained with a `_DESCRIPTIONS` preview written as "when to read this" — the shape an MCP skill needs. `help.workflow` gains the Method / Verify / filter-literal discipline from today's `query` docstring; `help.intro` re-points to fields then topics. The `query` description and `instructions` carry one batch call listing all five. Alternative — one combined topic — rejected: batch `inspect` makes several topics one call, and per-field pointers stay precise.

**D5 — `help.` memories ignore `compact`.** In `InspectService`'s memory path, an id starting with the `help_seed` prefix constant renders full; `descriptions_max_chars` still truncates (an explicit caller choice). One place covers MCP, REST, CLI and `SlayerClient`. Alternative — a per-kind `compact` default for all memories — rejected by the user: help is slated to move to MCP skills (DEV-2067).

**D6 — Collision suggestion from a pure helper.** At the raise site in `projection.py`, a pure function computes the first free public name over `source_columns ∪ every declared / public / canonical name in the query`: first the formula's canonical alias (from `canonical_aggregate_alias`, the one naming authority), else `<declared>_<n>` from n = 2. It does not touch `_taken_names` or `_unique_hidden_name` (stateful, hidden-alias only). `MeasureNameCollidesWithColumnError` takes an optional `suggestion`, so direct construction without one keeps today's text.

**D7 — Always-load option, wired like `ingest_on_startup`.** `create_mcp_server(always_load_query=False)` and `create_app(always_load_query=False)` (which builds the embedded MCP server), `--always-load-query` on `slayer mcp` and `slayer serve`, env `SLAYER_MCP_ALWAYS_LOAD_QUERY`. When on, `query` registers with `meta={"anthropic/alwaysLoad": True}`; `searchHint` omitted (irrelevant for a never-deferred tool). The `mcp` floor rises to the first release whose `FastMCP.tool` accepts `meta`, established by installing candidates in a scratch venv and checking `_meta` reaches `tools/list`; the floor is updated in `pyproject.toml`, the lockfile, the `_MCP_REMEDY` text and `tests/test_mcp_dependency_pin.py`.

**D8 — Guards are tests over the advertised surface.** Built once via `create_mcp_server(storage=…, _seed_help=False)` + `list_tools()`: description budget / dedent / no `Args:`; instructions budget; parameter coverage; a `query` schema size budget (measured after the rewrite + ~10%, commented: the schema ships with every request); the ownership drift guard over the `query` description plus every description in its schema (field- and class-level) using distinctive markers; help-pointer resolution.

## Risks / Trade-offs

- [The bigger `query` schema costs tokens on every request] → size-budget test; D2 keeps the long tail in help.
- [Agents still skip help] → every capability is visible on its field without help; help holds only the long tail.
- [Domain-wide field text changes REST OpenAPI] → intended (one language, one description); REST `QueryRequest` untouched.
- [A documented idiom drifts from engine behaviour] → execution tests pin each documented idiom on an in-process DuckDB fixture.
- [The `help.` prefix exception is ad hoc] → accepted as temporary until DEV-2067 serves help as skills.
- [Raising the `mcp` floor narrows installable versions] → floor set to the verified minimum only.
