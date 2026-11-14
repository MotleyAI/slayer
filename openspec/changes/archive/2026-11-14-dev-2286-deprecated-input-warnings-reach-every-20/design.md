## Context

Fresh query input is converted in two places today: `SlayerQuery`'s wrap validator converts a fresh `strict` and a legacy root extension (accumulating a payload list), while the REST `QueryRequest`'s wrap validator calls `prepare_query_input`, which converts only the root extension (one optional payload) and migrates around a fresh `strict` so `SlayerQuery` can convert it later. `QueryRequest` mirrors `SlayerQuery`'s fields and the endpoint rebuilds the query from `model_dump(exclude_none=True)`, so a `strict: null` riding `extra="allow"` is stripped before any converter sees it. The MCP `query` tool's dry-run branch returns `f"SQL:\n{result.sql}"` and drops `result.warnings` of every kind.

Principles that apply: `python.core` §3.6 (*Warnings are structured payloads, surfaced twice* — the dry-run fix brings the MCP dry-run response in line); system §3.6 (Pydantic only) and the import law (no new cross-node import: `slayer.api` already imports `prepare_query_input` from core). No arc42 or `.c4` edit. The specs touched are `compatibility` (cross-cutting in `index.yaml`, touches `python.surfaces` and `python.core`) and `mcp` (owned by the `surfaces` bucket); no new capability.

## Goals / Non-Goals

**Goals:**
- One converter of fresh query input, used by `SlayerQuery` and the REST request, so a presence-sensitive deprecated field cannot be lost between parse and conversion.
- The MCP dry-run reply carries every warning of the run, shaped per `format`.
- The removal-tripwire spec matches the MCP model-save message shape.

**Non-Goals:**
- Changing MCP model-save replies to structured entries, or `create_model`'s prose reply to JSON (ruling: decision 1).
- `show_sql` under `format="json"` (prefixes `SQL:` text to the JSON payload) — untouched.
- The CLI: deprecations already reach stderr as `Deprecated:` lines.

## Decisions

1. **MCP model saves report a deprecated input as its message; the spec is amended.** Options: (A) structured entries on `create_model` / `edit_model` — `create_model` would switch from prose to a JSON reply, `edit_model`'s refresh-hook strings would need a structured kind, and MCP memory saves would still report messages; (B) the message shape, as every MCP save tool already reports and `docs/upgrading/2.0.md` documents. Taken: B — one shape across MCP save tools, the message names old form, replacement and removal version, and core §3.6 governs query responses (`SlayerResponse.warnings`), not save replies. (user)
2. **One fresh-input converter.** `prepare_query_input` converts a fresh `strict` (when `_has_fresh_strict`), then a legacy root extension, then migrates the schema, and returns the data with a list of every payload; `SlayerQuery`'s wrap validator and `QueryRequest`'s wrap validator both call it. The request then holds every payload of its body, the dumped body never holds `strict`, the `SlayerQuery` built from it carries none, and the existing `request_deprecations` path reports each once. Rejected: dumping with `exclude_unset` (an explicit `null` on a non-nullable `SlayerQuery` field would become a validation error) and special-casing `strict` in the endpoint (a second converter that must agree by hand). (default)
3. **The REST run-by-name gate refuses a request carrying deprecated inputs.** A false or null `strict` converts to no field at all, so neither `model_fields_set` nor `model_extra` would reveal it; gating on the request's deprecations keeps the 400 for `{"name", "strict": …}`. (default; confirmed by codex)
4. **The MCP dry run carries every warning kind**, not only `deprecated_input` — the same dropped list, and `models/aggregation-names` already requires `library_name_shadow` there. (default)
5. **The dry-run reply follows `format`**: json → one object `{"sql", "warnings"?}` (`warnings` only when non-empty, like the executed json payload); csv → `# warning:` lines before `SQL:\n<sql>`; markdown → `SQL:\n<sql>` followed by the `Warnings:` block. Reuses the existing warning renderers. (default; csv shape from codex)
6. **Tests cover a query carrying two deprecated forms** (`strict` and an extension object in `source_model`), in process and over REST, asserting two distinct reports once each — the payload-list change is where a warning could silently drop. (codex)
7. **Regression tests pin the fresh-versus-stored boundary** across the converter move: `strict: true` with no version, the current version and a malformed version converts with one payload; a v3 stored query upgrades to `to_many_handling: "error"` silently; a legacy root extension validated inside a stored-document migration converts silently. (codex)

## Risks / Trade-offs

- [The MCP json dry-run reply changes from `SQL:` text to a JSON object] → it was never parseable as JSON under `format="json"`; markdown, the default, keeps starting with `SQL:`, so existing callers of the default format are unaffected.
- [Moving `strict` conversion into the REST request changes where the `FutureWarning` is attributed] → it is still emitted once per conversion, at request parse; the response report is unchanged.
