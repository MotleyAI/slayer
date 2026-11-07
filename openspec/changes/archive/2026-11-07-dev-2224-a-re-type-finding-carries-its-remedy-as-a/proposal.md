## Why

A re-type finding knows whether its fix is a re-ingest or a type change, but keeps that only as English `remedy` / `summary` text naming the datasource and the `slayer ingest` CLI, which an embedding application cannot show its users. To word its own message the application must re-derive SLayer's remedy rules, and those two rule copies agree only by hand — Storyline's platform health is the first consumer that needs a typed remedy.

## What Changes

- `RetypeFinding` gains a typed `remedy_kind: "reingest" | "set_type"`, the single source of the remedy decision.
- `remedy` and `summary` are derived from the finding's fields (byte-identical text to today), so they can never disagree with `remedy_kind`.
- `RetypeFinding.of(...)` takes `remedy_kind` instead of `reingest: bool`.
- A serialised finding without `remedy_kind` still loads: its kind is inferred from the canonical legacy `remedy` text; unrecognised legacy text is rejected.
- The spec states precisely that re-ingest is the remedy only for a live list on a table-backed model or an `INT` column SQLite's storage probe widened; every other conflict, including any on an `sql`-mode model, is a type change.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/schema-drift`: the re-type finding carries a typed remedy kind agreeing with its remedy text, and the re-ingest rule is narrowed in wording to table-backed models.

## Impact

- `slayer/engine/schema_drift.py` (`RetypeFinding`, its three construction sites).
- REST `/validate-models`, MCP `validate_models`, `SchemaDriftError.findings`: the serialized finding gains `remedy_kind`; all other fields and texts unchanged.
- `docs/concepts/schema-drift.md`.
