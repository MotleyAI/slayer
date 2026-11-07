## Context

`RetypeFinding` (`slayer/engine/schema_drift.py`) is built by `RetypeFinding.of(..., reingest: bool)` at three sites — `retype_findings` (table-backed; `reingest` iff the live column is `ARRAY`), `sql_model_retype_findings` (always `False`), and `_sqlite_probe_int_drift_for_model` (always `True`). `of` turns the bool into stored `remedy` / `summary` text and drops it. The CLI prints `summary`; `SchemaDriftError` appends each finding's `summary` to its message; REST and MCP return `model_dump(mode="json")`.

Structural cause (see proposal.md - Why): the remedy decision and its texts are separate values that must agree by hand — between SLayer and its consumers, and, once a typed field is added beside stored texts, inside `RetypeFinding` itself. Grounding: `architecture/system.arc42.md` §3.12 (Pydantic v2) and §3.13 (a closed kind set; dispatch on a kind fails closed). No arc42 or `.c4` change.

## Goals / Non-Goals

**Goals:**
- One stored representation of the remedy decision; every text derived from it.
- Old serialized findings keep loading.

**Non-Goals:**
- The 1.8.x version bump and release (a separate bump-version PR).
- Any change to which conflicts are findings or to the remedy texts.

## Decisions

1. **`remedy_kind: RemedyKind`, `RemedyKind = Literal["reingest", "set_type"]`** is a stored field of `RetypeFinding`, the single source of the decision. Name and values from the issue's example. (default)
2. **`remedy` and `summary` are `@computed_field` properties** derived from the finding's fields, byte-identical to today's texts; they serialize after the stored fields, so the JSON gains `remedy_kind` just before `remedy`. Alternative weighed: keep them stored beside `remedy_kind` — rejected, a directly built finding could carry a contradicting text. (default)
3. **`RetypeFinding.of(..., remedy_kind=...)` replaces `reingest: bool`**, so the decision has one spelling at every construction site. (default)
4. **Legacy payloads**: a `model_validator(mode="before")` fills a missing `remedy_kind` from the payload's `remedy` text, matching only the two canonical forms (`re-ingest datasource '<ds>' (\`slayer ingest\`) to re-type it` → `reingest`; `set the column's type to <TYPE>` → `set_type`). Any incoming `remedy` / `summary` is ignored and recomputed. Alternative weighed: an optional `remedy_kind: ... | None` — rejected, old payloads would carry no kind. (default)
5. **Unrecognized legacy remedy text is refused** (validation error) rather than defaulting to `set_type`, so a malformed or foreign payload never carries a wrong typed kind (§3.13 fails closed). (codex)
6. **The spec names the table-backed scope of the re-ingest rule** and adds an `sql`-mode live-list scenario: the stored requirement read "re-ingesting for a live list", which an `sql`-mode list (always `set_type`) contradicts. (codex)

## Risks / Trade-offs

- [A future remedy text change breaks legacy inference] → inference matches only the two historical texts, which are fixed constants of the past format; new payloads always carry `remedy_kind`.
- [`computed_field` is new to this codebase; basedpyright may flag `@computed_field` over `@property`] → follow pydantic's documented pattern; fix at the root, never grow the baseline.
