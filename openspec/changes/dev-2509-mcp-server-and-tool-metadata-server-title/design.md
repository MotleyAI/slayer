## Context

`create_mcp_server` (`slayer/mcp/server.py`) builds the server as
`MCPServer("SLayer", instructions=..., version=...)` through the `CountedMCPServer` subclass, and registers all 28
tools through a local `tool(**kwargs)` wrapper that forwards to `mcp.tool(...)`. The installed SDK (mcp 2.3.0)
accepts `title`, `description`, `website_url` and `icons` on `MCPServer.__init__`, and `title`, `annotations` and
`icons` on `tool()`. It already advertises a wrapped `{"result": string}` output schema for every `-> str` tool
(`structured_output=None`), and `_with_deprecations` rewrites `structured_content["result"]`. The module imports
the SDK lazily (`_import_mcp_sdk`) so an out-of-range mcp gets an actionable `ImportError`.

Architecture: the code sits in the `surfaces` virtual bucket, which owns the `mcp` spec group; no model, view or
`index.yaml` change. Applicable principles: `system.arc42.md` §3.6 (Pydantic v2, never dataclasses); the
tool-surface spec's description budget (2048 chars) and every-parameter-described requirements. No arc42
contradiction.

## Goals / Non-Goals

**Goals:**
- Server title, description, website URL and themed icons in `initialize`.
- A title and all four annotation hints on every tool, from what each implementation does.
- A registration shape that makes a tool without them impossible.
- Corrected descriptions where they misstate behaviour.

**Non-Goals:**
- Server Cards, `ai-catalog.json`, the HTML setup page and directory listings (hosted Motley endpoint, not
  self-hosted SLayer).
- Any `outputSchema` beyond the SDK's wrapped result string.
- Any runtime behaviour change.

## Decisions

1. **Icon = the SLayer octopus as two themed PNG data URIs** (user). No SLayer vector mark exists: the repo, its
   history and the Motley Figma brand file were checked (the SLayer web page uses only the Motley wordmark; the
   brand guide's marks are the Motley M). Options weighed: the octopus PNGs; the Motley raspberry-M SVG favicon
   (vector, but Motley's identity, not SLayer's); a user-supplied octopus SVG (none available). Files:
   `slayer/mcp/icons/slayer-dark.png` = byte copy of `docs/SLayer.png` (white on transparent, theme `dark`);
   `slayer/mcp/icons/slayer-light.png` = the same alpha channel with RGB `#091d31` (theme `light`), generated once
   with a throwaway Pillow run and committed (no new dependency). Both 180×160, `sizes: ["180x160"]`, embedded as
   `data:image/png;base64,` URIs; read from package data next to `server.py`.
2. **Cached sample writes do not break read-only** (user). `inspect` and `search` re-profile stale field samples and
   persist them (`ensure_samples_fresh` → `storage.update_field_sampled`); no authored definition changes, so both
   are read-only. Weighed: `readOnlyHint: false` (literal about the storage write, but makes clients prompt on the
   main discovery tools and is inconsistent with `query`).
3. **`ingest_datasource` is destructive** (user). It never deletes and only reports drift deletes, but a re-ingest
   can re-type existing fields (list types) and saves even when that breaks stored dependents, reporting them as
   `broken_dependents`. Weighed: `false` per its "additive" docstring. Its docstring is corrected to name the
   re-type.
4. **Open-world = can connect to a datasource's database; embedding-provider calls excluded** (user). Weighed: also
   counting embedding calls, which would make nearly every tool open-world and track deployment config the static
   hint cannot follow.
5. **`datasets_summary`, `list_datasources`, `get_datasource_priority`, `recommend_population` are closed-world**
   (default). The issue's notes listed them as database readers; the code opens no connection, and the issue says
   to set hints from the code.
6. **No `outputSchema` change** (default). The SDK's auto-generated wrapped schema is kept and pinned by a test.
   Rejected (codex): pass `structured_output=False` to drop it — the issue says not to lose it, and
   `_with_deprecations` rewrites `structured_content["result"]`.
7. **Literal MCP definitions for destructive and idempotent** (default): a same-name replace is destructive (every
   `create_*` replaces a same-named entity); a refusal or "not found" on a repeat call counts as idempotent.
   `save_memory` is the one non-idempotent tool (no `id` allocates a new memory each call).
8. **Criteria are over stored definitions; derived state excluded** (codex). Stored definitions = datasources,
   datasets minus cached sampled values, semantic models, ontologies, ontology mappings, memories, datasource
   priority. Cached samples, embeddings and search indexes are derived and excluded from the read-only and
   idempotent criteria.
9. **Required `title` and `hints` on the registration wrapper** (default): the local `tool()` wrapper takes them as
   required keyword arguments with no defaults; the hints come from one helper that always sets all four. A
   structural guard instead of annotating instances. `ToolAnnotations` and `Icon` come through `_import_mcp_sdk`,
   keeping the lazy-import remedy.
10. **Light icon first** (default): a client ignoring `theme` most likely renders on a light background.
11. **Closed-world test seeds query-backed datasets** (codex): `datasets_summary` runs the query planner on them
    (`relationships_as_read`), a path table-only fixtures would miss.
12. **Read-only test snapshots stored definitions only** (codex): cached samples, embeddings and search-index rows
    may change.
13. **Identity test parses the icon data URIs strictly** (codex): scheme, MIME, valid base64 and decoded PNG
    dimensions for both icons, since the SDK's `Icon` does not validate `src`.
14. **Server title and description wording** (user, approved wording): title `SLayer semantic layer`; description
    "Query databases by describing the data you want (metrics, dimensions, filters); SLayer writes and runs the
    SQL, joins included. Also connects datasources, builds and edits datasets, and keeps agent memories."
15. **Tool titles** (user, approved wording): as listed in the tool-surface spec table.
16. **Description corrections** (user): `create_dataset` example field key `"sql"` → `"expression"`;
    `ingest_datasource` names the re-type (never deletes; may re-type an existing field to the type the database
    now reports, listing the stored definitions that breaks); `delete_datasource` names its cascade (its ontology
    mappings deleted, memories' links stripped, its datasets kept); `delete_dataset` names its refusal (a stored
    model-scoped metric or ontology mapping depends on it); `edit_datasource` says connection settings change via
    `create_datasource` under the same name, which replaces it.

## Risks / Trade-offs

- [Raster icons] → 180×160 meets the "≥128px" criterion; a vector octopus can replace them later without a spec
  change beyond the icon entries.
- [Clients ignoring `theme`] → light-theme icon listed first.
- [Static hints vs per-call effects] (e.g. `save_memory` with an `id` is idempotent, without is not) → hints take
  the worst case over all arguments.
- [Hint drift as tools change] → the pinned per-tool table test and the closed-world / read-only behaviour tests
  fail when an implementation stops matching its row.
