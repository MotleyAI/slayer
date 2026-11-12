## Context

See proposal.md for the five routes. Today a query payload has two readers: Mode B (`slayer.core.syntax`) and dialect SQL (sqlglot), reached through:
- `ModelExtension.columns` (`slayer/core/query.py`), applied by `slayer/engine/extensions.py` and substituted by `slayer/ir/variables.py`;
- `SourceSpec`'s inline `SlayerModel` branch, read by `bundle_builder`, `stage_ordering`, `model_reads`, `schema_drift`, `memories/resolver`, `inspect/model_render`, `usage_reporting/features`, the OSI exporter and the extension walker;
- `substitute_variables`, which splices `{var}` values as text into a model's Mode-A surfaces;
- `param_binding._bind_text`, which parses a query-side string argument as dialect SQL.

Stored queries live in memories (never written back; their repairs rerun on every load), saved queries and query-backed models' `source_queries` (written back when the outer `SlayerModel` version is stale). `SlayerQuery` is at v13; an unloadable document is skipped through `stored_document_boundary` with an `UnloadableDocumentWarning`.

Principles that bind the design:
- `semantics.arc42.md` §4 P2 (two expression layers, gaining 2.3) and P3 (logical names);
- `system.arc42.md` P7 (fail closed), P8 (one reader of Mode B), P10 (one typing), P11 (fail at save time);
- `python.sql.arc42.md` P4 (one Mode-A door);
- `python.engine.arc42.md` P8 (models persist verbatim);
- `python.storage.arc42.md` P3 (versioned persistence; an upgrade path is never a removal);
- `python.core.arc42.md` P5 (typed errors), P6 (structured warnings).

## Goals / Non-Goals

**Goals:**
- One reader for everything a query payload carries: Mode B. Mode A is authored only in stored model definitions, and a query-supplied value reaches it only as a literal node.
- Stored documents written with the removed forms convert exactly where a resolver can attribute every reference, and are otherwise unloadable, never silently reinterpreted.

**Non-Goals:**
- Restricting Mode A in stored model definitions (an editor's `Column.sql`, `sql`, `sql_table`, model filters, custom aggregations), including scalar subqueries there.
- A restricted-caller switch: the removal applies to every caller.
- Memory write-back.
- Composite aggregation parameters carrying attached aggregates (DEV-2016).
- SLayer SQL (DEV-1768).

## Decisions

1. **Inline models are removed in this change**, not after DEV-1768: the only capability they hold beyond extensions, expressions and stages is the raw-SQL / table reach itself. (user)
2. **DEV-2044's Mode-A half is folded in**: literal-only `{var}` in Mode-A text. DEV-2044 is closed as absorbed after merge, with the user's yes. (user)
3. **A query-side string aggregation argument naming a placeholder is read as Mode B**, bound exactly as its unquoted spelling, as `aggregations/formula-templates` already states. Definition defaults and `Aggregation.formula` stay Mode A; settings (`window='90d'`) are unchanged. Stored queries are not migrated: a dialect-only argument fails at save and at query time with the typed parameter error. (user)
4. **An unconvertible stored document becomes unloadable** through the existing boundary, untouched on disk; no memory-only stripping, no quarantined form. (user)
5. **Every convertible extension column converts, with one structured warning per converted column** naming the document and field. The stored-upgrade "same rows" rule carries an explicit exception for dialect-vs-portable meaning shifts (SQLite `LIKE` case, regex flavour, `||` with NULL, TEXT↔number coercion). (user)
6. **Approved wording**, `semantics.arc42.md` §4 P2, new sub-clause after 2.2: "**2.3** A query carries no Mode A: Mode A is written only in a stored model's definition, and a value a query supplies enters it only as a literal. [enforced: test:tests/test_dev2021_queries_carry_no_mode_a.py]" (user, approved wording)
7. **Query-time Mode A is removed for every caller; `ModelExtension.columns` is deleted outright; stored queries migrate by inlining.** (user, DEV-2021 re-scoping comment of 2026-10-21)
8. **Fresh input carrying a removed form is refused with a typed error**, never deprecated and converted: conversion would keep the route open. A versioned non-store payload is fresh input for this purpose; conversion runs only in the storage load pipeline. (default)
9. **Conversion is model-aware and runs in the storage load pipeline**, beside the existing model-aware repairs, before validation. It binds the stored query with the static query scope builder (DEV-2102), with the extension columns present. Every reference resolving to an extension column is replaced by its expression, qualified by the canonical path it resolved through. The registry's v13 → v14 step only stamps. (user; folded from codex)
   - *Alternative*: a pure dict→dict text rewrite, which cannot attribute references through edge names, aliases or an inferred population, and would strand convertible documents.
10. **Result keys are preserved.** A dimension becomes a computed dimension named after the old column, its join path flattened with `__` when it is reached through a join (`customers__tier_label`), so every stage output keeps its column names; the top-level result key then changes from `<root>.<path>.<column>` to `<root>.<path>__<column>`, and the conversion warning names it. A time dimension becomes an expression time dimension named the same way. A measure reading it gets an explicit `name` equal to its old auto-generated name, its join path flattened the same way. (user, on DEV-2268's flat-name rule) Filters, order, partition and ranking keys, `main_time_dimension`, extension measures and other extension columns take the expression in place. A field that cannot hold an expression makes the document unconvertible. (default; `main_time_dimension` folded from codex)
11. **Column shapes:**
    - `filter` becomes `iif(<filter>, <sql>, NULL)`.
    - A sql-less column converts only when its name resolves to a visible column of the extended model; otherwise it is unconvertible.
    - A declared type other than `TEXT` / `UNKNOWN` is kept as `CAST(<expr> AS <type>)`, reproducing today's rendering for the numeric types. An `ARRAY` column is unconvertible.
    - `label` / `format` carry over where the target position supports them; other metadata is dropped. (user for the type rule; codex for the sql-less and type findings)
12. **Unconvertible shapes:**
    - an inline `source_model`;
    - column text that does not parse as Mode B;
    - an extension join whose `join_pairs` names an extension column on either side;
    - a dependency cycle among extension columns;
    - an extension column shadowing a base column;
    - a sql-less column not resolving to a visible column;
    - an `ARRAY` column;
    - a reference the resolver cannot attribute, for instance because a model the query reads is missing from the store.

    Extension columns expand in topological order. (default; cycles, shadowing and both sides of `join_pairs` folded from codex)
13. **Memories convert on every load without write-back**, following their existing repair pattern; the conversion warning repeats on each such load. `SlayerModel` and `Memory` versions are bumped with `SlayerQuery` so nested queries stamp. (user)
14. **Mode-A variables bind structurally.**
    - `{? … ?}` optional blocks resolve first, at template level.
    - Each remaining `{var}` is replaced by a sentinel, the text is parsed in the model's dialect, and each sentinel node is replaced by a literal node: string-literal content, a bare number or boolean (a numeric string reads as its number), or a whole `IN` list.
    - A sentinel in any other position is refused with the "variables supply literal values" error: an identifier, quoted identifier, function or type name, comment, a partial `IN` list, or a string value in a bare slot.
    - The position check needs the dialect, so it runs at model save (system P11) and at query time; importers go through save validation, and inspection is unchanged. Rules are identical to Mode B's. (default; sentinel parse folded from codex)
15. **`Column.sql` parses as exactly one expression**, so a statement (`SELECT …`, `WITH …`, DDL / DML, a command) or a multi-statement text is refused at construction. A parenthesised scalar subquery inside an expression stays legal. (default, from the 2026-10-22 issue comment; widened from codex)
16. **Profiling** samples non-array columns with `min(col)` / `max(col)` and keeps the array element expansion. (default; arrays folded from codex)
17. **Usage reporting**: the `inline_source` key is kept and fires on extensions; inline-model measure counting is removed. (default)
18. **Every spec requirement that names a removed form is modified or removed in this change** (codex). A doc-grep test was proposed and rejected: it would test wording. The narrowing of `source_model` to `str | None` makes basedpyright flag every leftover inline-model branch; a separate static test was rejected (codex) as redundant.
19. **Codex findings**: 13 raised. Folded: the version slot (v13 → v14), version bumps for nested write-back, model-aware conversion, the sql-less visibility rule, the spec sweep, the consumer checklist, `main_time_dimension` and `join_pairs` coverage, cycles and shadowing, the sentinel parse, the save-time check location, the widened statement check, the type rule, array profiling. Rejected: the doc-grep test and the static `source_model` test (see 18); banning scalar subqueries in stored `Column.sql` (out of scope, see Non-Goals). (codex)

## Risks / Trade-offs

- [Callers relying on extension columns or inline models break: Storyline's query tool since storyline#765, scripts] → Intended (decision 7). Typed errors name the replacement, an expression in the using position or a stored model.
- [A converted stored column may read differently under Mode B's portable meaning] → Accepted (decision 5); one warning per converted column.
- [Stored documents become unloadable when unconvertible] → Accepted (decision 4); they stay on disk for hand repair, and the warning names the construct.
- [Memories reconvert and re-warn on every load] → Accepted (decision 13).
- [Editor-authored Mode-A text with a placeholder in an identifier position stops working] → The Cube importer emits only literal positions. Such a model loads verbatim, fails with the literal-values error when queried, and is refused on re-save.

## Migration Plan

`SlayerQuery` v13 → v14, with `SlayerModel` and `Memory` bumped so nested queries stamp. The model-aware conversion runs in the storage load pipeline. Models are written back as today; memories reconvert on each load. Rollback: a v14 document read by older code fails on the unknown version (forward-versioned documents are best-effort, as for every bump).
